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

package org.apache.flink.table.runtime.window.aligned;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.table.api.window.TimeWindow;
import org.apache.flink.table.codegen.GeneratedSubKeyedAggsHandleFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.runtime.functions.ExecutionContext;
import org.apache.flink.table.runtime.functions.SubKeyedAggsHandleFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * It's an buffered implementation of {@link AlignedWindowAggregator}. It keeps an HashMap on buffer,
 * when an element arrives, it will accumulated into the HashMap first, and flushed to state when
 * checkpoints. This will reduce a lot of state accessing.
 */
public final class BufferedAlignedWindowAggregator
	implements AlignedWindowAggregator<BaseRow, TimeWindow, BaseRow> {

	private static final long serialVersionUID = 1L;

	private final TypeInformation<BaseRow> accTypeInfo;
	private final TypeInformation<BaseRow> aggResultTypeInfo;
	private final GeneratedSubKeyedAggsHandleFunction<TimeWindow> generatedWindowAggregator;

	private final long minibatchSize;
	private final boolean sendRetraction;

	//----------------------------------------------------------------------------------

	private transient ExecutionContext ctx;

	private transient SubKeyedAggsHandleFunction<TimeWindow> windowAggregator;

	// aligned window buffer
	private transient TreeMap<TimeWindow, Map<BaseRow, BufferEntry>> buffer;

	// stores window acc, schema: <KEY, WINDOW, ACC>
	private transient SubKeyedValueState<BaseRow, TimeWindow, BaseRow> windowAccState;

	// stores buffer status, schema: <KEY, WINDOW, STATUS>
	private transient KeyedMapState<BaseRow, TimeWindow, Integer> bufferStatusState;

	// store previous agg result which do not contains keys, schema: <KEY, WINDOW, AGG_OUT>
	private transient SubKeyedValueState<BaseRow, TimeWindow, BaseRow> previousState;

	private transient int numOfElements;

	private transient JoinedRow outRow;

	public BufferedAlignedWindowAggregator(
		TypeInformation<BaseRow> accTypeInfo,
		TypeInformation<BaseRow> aggResultTypeInfo,
		GeneratedSubKeyedAggsHandleFunction<TimeWindow> generatedWindowAggregator,
		long minibatchSize,
		boolean sendRetraction) {
		this.accTypeInfo = accTypeInfo;
		this.aggResultTypeInfo = aggResultTypeInfo;
		this.generatedWindowAggregator = generatedWindowAggregator;
		this.minibatchSize = minibatchSize;
		this.sendRetraction = sendRetraction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		this.ctx = ctx;
		this.buffer = new TreeMap<>();
		this.numOfElements = 0;
		this.outRow = new JoinedRow();
		BaseRowUtil.setAccumulate(outRow);

		// the type cast is needed here, otherwise compile will complain
		this.windowAggregator = (SubKeyedAggsHandleFunction<TimeWindow>) generatedWindowAggregator.newInstance(
			ctx.getRuntimeContext().getUserCodeClassLoader());
		windowAggregator.open(ctx);

		ValueStateDescriptor<BaseRow> windowStateDescriptor = new ValueStateDescriptor<>(
			"window-acc",
			accTypeInfo);
		this.windowAccState = ctx.getSubKeyedValueState(windowStateDescriptor);

		// SubKeyedValueState do not support iterate on all keys, so we have to use KeyedMapState
		MapStateDescriptor<TimeWindow, Integer> bufferStatusStateDescriptor = new MapStateDescriptor<>(
			"window-buffer",
			new TimeWindow.Serializer(),
			IntSerializer.INSTANCE);
		this.bufferStatusState = ctx.getKeyedMapState(bufferStatusStateDescriptor);
		// restore buffer status state
		restore();

		if (sendRetraction) {
			ValueStateDescriptor<BaseRow> previousStateDescriptor = new ValueStateDescriptor<>(
				"previous-agg-result",
				aggResultTypeInfo);
			this.previousState = ctx.getSubKeyedValueState(previousStateDescriptor);
		}

		// counter metric to get the size of bundle
		ctx.getRuntimeContext()
			.getMetricGroup()
			.gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
		ctx.getRuntimeContext().getMetricGroup().gauge("bundleRatio", (Gauge<Double>) () -> {
			int numOfKeys = buffer.size();
			if (numOfKeys == 0) {
				return 0.0;
			} else {
				return 1.0 * numOfElements / numOfKeys;
			}
		});
	}

	@Override
	public void addElement(BaseRow key, TimeWindow window, BaseRow input) throws Exception {
		numOfElements++;
		// aggregate current input row to buffer
		Map<BaseRow, BufferEntry> keyAccs = buffer.computeIfAbsent(window, k -> new HashMap<>());
		BufferEntry bufferEntry = keyAccs.get(key);
		BaseRow accumulator;
		if (bufferEntry == null) {
			accumulator = windowAggregator.createAccumulators();
			bufferEntry = BufferEntry.of(accumulator);
			keyAccs.put(key, bufferEntry);
		} else {
			accumulator = bufferEntry.getAccumulator();
		}
		bufferEntry.markNewElementReceived();

		// do accumulate/retract
		// null namespace means use heap data views
		windowAggregator.setAccumulators(null, accumulator);
		if (BaseRowUtil.isAccumulateMsg(input)) {
			windowAggregator.accumulate(input);
		} else {
			windowAggregator.retract(input);
		}
		accumulator = windowAggregator.getAccumulators();

		// put back to buffer entry
		bufferEntry.setAccumulator(accumulator);

		if (minibatchSize > 0 && numOfElements > minibatchSize) {
			// flush buffer to state to reduce heap load
			snapshot();
		}
	}

	@Override
	public void fireWindow(TimeWindow window, Collector<BaseRow> out) throws Exception {
		Map<BaseRow, BufferEntry> fired = buffer.get(window);
		if (fired == null) {
			return;
		}
		for (Map.Entry<BaseRow, BufferEntry> entry : fired.entrySet()) {
			BaseRow key = entry.getKey();
			BufferEntry bufferEntry = entry.getValue();
			checkNotNull(bufferEntry, "buffer is empty under key: %s window: %s", key, window);
			// emit window result
			doFire(key, window, bufferEntry, out);
		}
	}

	private void doFire(
		BaseRow key,
		TimeWindow window,
		BufferEntry bufferEntry,
		Collector<BaseRow> out) throws Exception {
		// set key to current context for window aggregator
		ctx.setCurrentKey(key);
		// there are incremental data since last fire, need output new result
		if (bufferEntry.deltaSinceLastFire()) {
			if (bufferEntry.bufferIsFullData()) {
				// buffer acc is the full data, no need to touch state
				BaseRow acc = bufferEntry.getAccumulator();
				// null namespace means use heap data views
				windowAggregator.setAccumulators(null, acc);
			} else {
				// buffer acc is the partial data, need to load acc from state
				BaseRow bufferAcc = bufferEntry.getAccumulator();
				BaseRow stateAcc = windowAccState.get(key, window);
				windowAggregator.setAccumulators(window, stateAcc);
				windowAggregator.merge(window, bufferAcc);
				// set an empty acc to buffer entry, and mark buffer flushed
				bufferEntry.setAccumulator(windowAggregator.createAccumulators());
				bufferEntry.markBufferFlushed();
			}
			BaseRow aggResult = windowAggregator.getValue(window);

			if (sendRetraction) {
				// has emitted result for the key + window, output retraction first
				if (bufferEntry.isFired()) {
					BaseRow previousAggResult = previousState.get(key, window);
					outRow.replace(key, previousAggResult);
					BaseRowUtil.setRetract(outRow);
					// send retraction
					out.collect(outRow);
				}
				// send accumulation
				outRow.replace(key, aggResult);
				BaseRowUtil.setAccumulate(outRow);
				out.collect(outRow);
				// update previousState
				previousState.put(key, window, aggResult);
			} else {
				// no early/late fires, directly send accumulation
				outRow.replace(key, aggResult);
				// no need to set header
				out.collect(outRow);
			}
		}
		bufferEntry.markFired();
	}

	@Override
	public void expireWindow(TimeWindow window) throws Exception {
		Map<BaseRow, BufferEntry> removed = buffer.remove(window);
		if (removed == null) {
			return;
		}
		// remove all the acc state under the window
		for (Map.Entry<BaseRow, BufferEntry> entry : removed.entrySet()) {
			BaseRow key = entry.getKey();
			windowAccState.remove(key, window);
			bufferStatusState.remove(key, window);
			if (sendRetraction) {
				previousState.remove(key, window);
			}
			// cleanup state dataviews
			ctx.setCurrentKey(key);
			windowAggregator.cleanup(window);
		}
	}

	@Override
	public TimeWindow lowestWindow() {
		if (buffer.isEmpty()) {
			return null;
		} else {
			return buffer.firstKey();
		}
	}

	@Override
	public Iterable<TimeWindow> ascendingWindows() {
		return buffer.navigableKeySet();
	}

	@Override
	public Iterable<TimeWindow> ascendingWindows(TimeWindow fromWindow) throws Exception {
		return buffer.navigableKeySet().tailSet(fromWindow);
	}

	@Override
	public void snapshot() throws Exception {
		this.numOfElements = 0;
		// iterate on all buffer entries and snapshot
		for (Map.Entry<TimeWindow, Map<BaseRow, BufferEntry>> entry : buffer.entrySet()) {
			TimeWindow window = entry.getKey();
			for (Map.Entry<BaseRow, BufferEntry> keyAccEntry : entry.getValue().entrySet()) {
				BaseRow key = keyAccEntry.getKey();
				BufferEntry bufferEntry = keyAccEntry.getValue();
				BaseRow bufferAcc = bufferEntry.getAccumulator();
				// set key to current context for window aggregator
				ctx.setCurrentKey(key);
				// merge buffer acc to state acc
				BaseRow stateAcc = windowAccState.get(key, window);
				if (stateAcc == null) {
					stateAcc = windowAggregator.createAccumulators();
				}
				windowAggregator.setAccumulators(window, stateAcc);
				windowAggregator.merge(window, bufferAcc);
				// put the updated acc into state
				windowAccState.put(key, window, windowAggregator.getAccumulators());
				// mark the buffer has been flushed to state which means is not full data now
				bufferEntry.setAccumulator(windowAggregator.createAccumulators());
				bufferEntry.markBufferFlushed();

				// persist the buffer status to state
				int status = bufferEntry.getStatus();
				bufferStatusState.add(key, window, status);
			}
		}
	}

	public void restore() throws Exception {
		// use get all instead of iterator keys to avoid too much seeks
		Map<BaseRow, Map<TimeWindow, Integer>> bufferStatus = bufferStatusState.getAll();
		for (Map.Entry<BaseRow, Map<TimeWindow, Integer>> entry : bufferStatus.entrySet()) {
			BaseRow key = entry.getKey();
			for (Map.Entry<TimeWindow, Integer> windowStatus : entry.getValue().entrySet()) {
				TimeWindow window = windowStatus.getKey();
				Integer status = windowStatus.getValue();
				// create a new BufferEntry instance
				BufferEntry bufferEntry = BufferEntry
					.of(windowAggregator.createAccumulators());
				bufferEntry.setStatus(status);
				// put into buffer
				Map<BaseRow, BufferEntry> keyAccs = buffer.computeIfAbsent(
					window,
					k -> new HashMap<>());
				keyAccs.put(key, bufferEntry);
			}
		}
	}

	@Override
	public void close() throws Exception {

	}

	/**
	 * The entry of a buffer.
	 */
	protected static final class BufferEntry {
		private BaseRow accumulator;

		// =========================================
		// we keep some flags to reduce state accesses
		// =========================================

		private boolean bufferIsFullData;
		private boolean deltaSinceLastFire;
		private boolean fired;

		protected BufferEntry(
			BaseRow accumulator,
			boolean bufferIsFullData,
			boolean deltaSinceLastFire,
			boolean fired) {
			this.accumulator = accumulator;
			this.bufferIsFullData = bufferIsFullData;
			this.deltaSinceLastFire = deltaSinceLastFire;
			this.fired = fired;
		}

		public static BufferEntry of(BaseRow accumulator) {
			return new BufferEntry(accumulator, true, true, false);
		}

		/**
		 * Marks a new element is received under this buffer entry, which means we have new
		 * updates on this buffer.
		 */
		public void markNewElementReceived() {
			this.deltaSinceLastFire = true;
		}

		/**
		 * Marks this buffer has been flushed to state once, which means we have partial
		 * accumulator in state, so that we have to merge state acc when firing.
		 */
		public void markBufferFlushed() {
			this.bufferIsFullData = false;
		}

		/**
		 * Marks this buffer entry has been fired once, which means if early/late firing
		 * is enabled, we have to send retraction first.
		 */
		public void markFired() {
			this.deltaSinceLastFire = false;
			this.fired = true;
		}

		/**
		 * Gets the accumulator stored in the buffer entry.
		 */
		public BaseRow getAccumulator() {
			return accumulator;
		}

		/**
		 * Sets the accumulator into the buffer entry.
		 */
		public void setAccumulator(BaseRow accumulator) {
			this.accumulator = accumulator;
		}

		/**
		 * Whether this buffer has been fired once.
		 * If true, we should send retraction first if early/late firing is enabled.
		 */
		public boolean isFired() {
			return fired;
		}

		/**
		 * Whether there is some incremental delta received since last fire.
		 * If false, we can skip to fire this entry.
		 */
		public boolean deltaSinceLastFire() {
			return deltaSinceLastFire;
		}

		/**
		 * Whether this buffer contains full data.
		 * If true, we can skip to read state as the state is empty for this entry.
		 */
		public boolean bufferIsFullData() {
			return bufferIsFullData;
		}

		/**
		 * Gets the status flag of this buffer entry. The status flag encapsulates
		 * the boolean flag information.
		 */
		public int getStatus() {
			int status = 0;
			if (bufferIsFullData) {
				status |= 1;
			}
			if (deltaSinceLastFire) {
				status |= 1 << 1;
			}
			if (fired) {
				status |= 1 << 2;
			}
			return status;
		}

		/**
		 * Sets the status flag to buffer entry. The status flag encapsulates
		 * the boolean flag information.
		 */
		public void setStatus(int status) {
			this.bufferIsFullData = (status & 1) != 0;
			this.deltaSinceLastFire = (status & (1 << 1)) != 0;
			this.fired = (status & (1 << 2)) != 0;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			BufferEntry that = (BufferEntry) o;
			return bufferIsFullData == that.bufferIsFullData &&
				deltaSinceLastFire == that.deltaSinceLastFire &&
				fired == that.fired &&
				Objects.equals(accumulator, that.accumulator);
		}

		@Override
		public int hashCode() {
			return Objects.hash(accumulator, bufferIsFullData, deltaSinceLastFire, fired);
		}
	}
}
