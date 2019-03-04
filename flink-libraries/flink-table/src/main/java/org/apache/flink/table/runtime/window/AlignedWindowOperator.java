/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.window.TimeWindow;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.functions.ExecutionContext;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;
import org.apache.flink.table.runtime.window.aligned.AlignedWindowAggregator;
import org.apache.flink.table.runtime.window.aligned.AlignedWindowTrigger;
import org.apache.flink.table.runtime.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.WindowAssigner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An operator that implements the logic for windowing based on a {@link AlignedWindowAggregator} and
 * {@link WindowAssigner} and {@link AlignedWindowTrigger}.
 *
 * <p>This window operator is an optimized operator for aligned windows. For aligned windows we can
 * make sure what's the next triggered window, what's the next triggered watermark. So that we can
 * avoid to rely on registering a lot of timer triggers to trigger window and get the
 * higher performance.
 *
 * <p>The {@link AlignedWindowAggregator} is used to access & maintain window states and fire window
 * results. It can be a buffered or memory-managed-buffered implementation to reduce state access.
 */
public class AlignedWindowOperator
	extends AbstractStreamOperator<BaseRow>
	implements OneInputStreamOperator<BaseRow, BaseRow> {

	private static final long serialVersionUID = 1L;

	private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
	private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
	private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final AlignedWindowAggregator<BaseRow, TimeWindow, BaseRow> windowRunner;

	private final WindowAssigner<TimeWindow> windowAssigner;

	private final AlignedWindowTrigger windowTrigger;

	private final int rowtimeIndex;

	// --------------------------------------------------------------------------------

	private transient long nextTriggerTime;

	private transient TimeWindow nextTriggerWindow;

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given
	 * timestamp.
	 */
	private transient TimestampedCollector<BaseRow> collector;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	private transient Counter numLateRecordsDropped;

	private transient Meter lateRecordsDroppedRate;

	private transient Gauge<Long> watermarkLatency;

	private transient long currentWatermark;

	public AlignedWindowOperator(
		AlignedWindowAggregator<BaseRow, TimeWindow, BaseRow> windowRunner,
		WindowAssigner<TimeWindow> windowAssigner,
		AlignedWindowTrigger windowTrigger,
		int rowtimeIndex) {
		this.windowRunner = windowRunner;
		this.windowTrigger = windowTrigger;

		// rowtime index should >= 0 when in event time mode
		if (!(windowAssigner instanceof SlidingWindowAssigner) && !(windowAssigner instanceof TumblingWindowAssigner)) {
			throw new IllegalArgumentException("Currently aligned window only support sliding and tumbling windows.");
		}
		checkArgument(!windowAssigner.isEventTime() || rowtimeIndex >= 0);
		this.windowAssigner = windowAssigner;
		this.rowtimeIndex = rowtimeIndex;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.collector = new TimestampedCollector<>(output);
		this.collector.eraseTimestamp();

		this.nextTriggerTime = Long.MIN_VALUE;
		this.nextTriggerWindow = null;

		TypeSerializer<TimeWindow> windowSerializer = new TimeWindow.Serializer();
		ExecutionContext ctx = new ExecutionContextImpl(this, getRuntimeContext(), windowSerializer);
		windowRunner.open(ctx);

		// metrics
		this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
		this.lateRecordsDroppedRate = metrics.meter(
			LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
			new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
		this.watermarkLatency = metrics.gauge(WATERMARK_LATENCY_METRIC_NAME, () -> {
			long watermark = this.currentWatermark;
			if (watermark < 0) {
				return 0L;
			} else {
				return System.currentTimeMillis() - watermark;
			}
		});
	}

	@Override
	public void close() throws Exception {
		super.close();
		collector = null;
		functionsClosed = true;
		windowRunner.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		collector = null;
		if (!functionsClosed) {
			functionsClosed = true;
			windowRunner.close();
		}
	}

	@Override
	public void processElement(StreamRecord<BaseRow> record) throws Exception {
		// prepare inputRow and timestamp
		BaseRow inputRow = record.getValue();
		// TODO: support processing time in the future
		long timestamp = inputRow.getLong(rowtimeIndex);

		Collection<TimeWindow> windows = windowAssigner.assignWindows(inputRow, timestamp);
		boolean isElementDropped = true;
		for (TimeWindow window : windows) {
			if (!isWindowLate(window, currentWatermark)) {
				isElementDropped = false;
				windowRunner.addElement(currentKey(), window, inputRow);
			}
			// TODO: support late arrival trigger
		}

		if (isElementDropped) {
			numLateRecordsDropped.inc();
			lateRecordsDroppedRate.markEvent();
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		this.currentWatermark = mark.getTimestamp();
		advanceWatermark(currentWatermark);
		super.processWatermark(mark);
	}

	private void advanceWatermark(long watermark) throws Exception {
		if (nextTriggerTime == Long.MIN_VALUE) {
			TimeWindow lowestWindow = windowRunner.lowestWindow();
			if (lowestWindow == null) {
				// if there is no window registered, no trigger happens
				return;
			} else {
				// initial first next trigger time
				this.nextTriggerTime = lowestWindow.maxTimestamp();
				this.nextTriggerWindow = lowestWindow;
			}
			// the initial trigger time should not be too larger than current watermark to
			// avoid skipping some un-arrived windows
			if (watermark < nextTriggerTime) {
				// the next trigger time should be the closest to the watermark
				this.nextTriggerTime = windowTrigger.nextTriggerTime(watermark);
				this.nextTriggerWindow = windowTrigger.nextTriggerWindow(watermark);
			}
		}
		// trigger windows
		if (watermark >= nextTriggerTime) {
			for (TimeWindow window : windowRunner.ascendingWindows(nextTriggerWindow)) {
				if (needTriggerWindow(window, watermark)) {
					windowRunner.fireWindow(window, collector);
				} else {
					// the next trigger time should be the closest to the watermark
					this.nextTriggerTime = windowTrigger.nextTriggerTime(watermark);
					this.nextTriggerWindow = windowTrigger.nextTriggerWindow(watermark);
					break;
				}
			}
		}
		// expire windows
		List<TimeWindow> windowsToExpire = new ArrayList<>();
		for (TimeWindow window : windowRunner.ascendingWindows()) {
			if (isWindowLate(window, watermark)) {
				windowsToExpire.add(window);
			} else {
				break;
			}
		}
		expireWindows(windowsToExpire);
	}

	/**
	 * Whether a window is late (can be expired).
	 *
	 * <p>NOTE: Currently, the implementation of this method is totally the same as
	 * {@link #needTriggerWindow(TimeWindow, long)}. But will be different in the
	 * future when take allowlateness into account.
	 */
	private boolean isWindowLate(TimeWindow window, long watermark) {
		long cleanupTime = window.maxTimestamp();
		return cleanupTime <= watermark;
	}

	/**
	 * Whether a window can be triggered, i.e. watermark excesses window's max timestamp.
	 */
	private boolean needTriggerWindow(TimeWindow window, long watermark) {
		long cleanupTime = window.maxTimestamp();
		return cleanupTime <= watermark;
	}

	private void expireWindows(Collection<TimeWindow> windowsToExpire) throws Exception {
		for (TimeWindow window : windowsToExpire) {
			windowRunner.expireWindow(window);
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		windowRunner.snapshot();
	}

	@Override
	public void endInput() throws Exception {
		// nothing to do
	}

	@SuppressWarnings("unchecked")
	private BaseRow currentKey() {
		return (BaseRow) getCurrentKey();
	}

	// ------------------------------------------------------------------------------
	// Visible For Testing
	// ------------------------------------------------------------------------------

	protected Counter getNumLateRecordsDropped() {
		return numLateRecordsDropped;
	}

	protected Gauge<Long> getWatermarkLatency() {
		return watermarkLatency;
	}

	@Override
	public boolean requireState() {
		return true;
	}

}
