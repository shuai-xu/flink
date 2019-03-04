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

package org.apache.flink.table.runtime.bundle;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates watermarks with specified emit latency.
 */
public class MiniBatchedWatermarkAssignerOperator
	extends AbstractStreamOperator<BaseRow>
	implements OneInputStreamOperator<BaseRow, BaseRow> {

	private final int rowtimeIndex;

	private final long offset;

	// timezone offset.
	private final long tzOffset;

	private long watermarkInterval;

	private transient long currentWatermark;

	private transient long expectedWatermark;

	public MiniBatchedWatermarkAssignerOperator(
		int rowtimeIndex,
		long offset,
		long tzOffset,
		long watermarkInterval) {
		this.rowtimeIndex = rowtimeIndex;
		this.offset = offset;
		this.tzOffset = tzOffset;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
		this.watermarkInterval = watermarkInterval;
	}

	@Override
	public void open() throws Exception {
		super.open();

		Preconditions.checkArgument(watermarkInterval > 0,
			"The inferred emit latency should be larger than 0");

		// timezone offset should be considered when calculating watermark start time.
		currentWatermark = 0;
		expectedWatermark = getMiniBatchStart(currentWatermark, tzOffset, watermarkInterval)
			+ watermarkInterval - 1;
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow row = element.getValue();
		if (row.isNullAt(rowtimeIndex)) {
			throw new RuntimeException("RowTime field should not be null," +
				" please convert it to a non-null long value.");
		}
		long wm = row.getLong(rowtimeIndex) - offset;
		currentWatermark = Math.max(currentWatermark, wm);
		// forward element
		output.collect(element);

		if (currentWatermark >= expectedWatermark) {
			output.emitWatermark(new Watermark(currentWatermark));
			long start = getMiniBatchStart(currentWatermark, tzOffset, watermarkInterval);
			long end = start + watermarkInterval - 1;
			expectedWatermark = end > currentWatermark ? end : end + watermarkInterval;
		}
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link MiniBatchedWatermarkAssignerOperator} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void endInput() throws Exception {
		processWatermark(Watermark.MAX_WATERMARK);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	/**
	 * Method to get the minibatch start for a watermark.
	 */
	public static long getMiniBatchStart(long watermark, long tzOffset, long interval) {
		return watermark - (watermark - tzOffset + interval) % interval;
	}
}
