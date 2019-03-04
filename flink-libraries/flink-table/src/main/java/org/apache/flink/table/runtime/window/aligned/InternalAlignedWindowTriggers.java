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

import org.apache.flink.table.api.window.TimeWindow;

import java.time.Duration;

/**
 * Internal {@link AlignedWindowTrigger TimeWindowTriggers}.
 */
public class InternalAlignedWindowTriggers {

	/**
	 * Creates a tumbling {@link AlignedWindowTrigger} with the given window size.
	 */
	public static AlignedWindowTrigger tumbling(Duration size, Duration offset) {
		return new TumblingTimeWindowTrigger(size.toMillis(), offset.toMillis());
	}

	/**
	 * Creates a sliding {@link AlignedWindowTrigger} with the given window size and slide.
	 */
	public static AlignedWindowTrigger sliding(Duration size, Duration slide, Duration offset) {
		return new SlidingTimeWindowTrigger(size.toMillis(), slide.toMillis(), offset.toMillis());
	}

	private static final class TumblingTimeWindowTrigger implements AlignedWindowTrigger {

		private static final long serialVersionUID = 1L;

		/**
		 * Size of this window.
		 */
		private final long size;

		private final long offset;

		private TumblingTimeWindowTrigger(long size, long offset) {
			this.size = size;
			this.offset = offset;
		}

		@Override
		public long nextTriggerTime(long clockTime) {
			long start = TimeWindow.getWindowStartWithOffset(clockTime, offset, size);
			long max = start + size - 1;
			return max > clockTime ? max : max + size;
		}

		@Override
		public TimeWindow nextTriggerWindow(long clockTime) {
			long maxTimestamp = nextTriggerTime(clockTime);
			long windowEnd = maxTimestamp + 1;
			return new TimeWindow(windowEnd - size, windowEnd);
		}
	}

	private static final class SlidingTimeWindowTrigger implements AlignedWindowTrigger {

		private static final long serialVersionUID = 1L;

		private final long size;

		private final long slide;

		private final long offset;

		private SlidingTimeWindowTrigger(long size, long slide, long offset) {
			if (size % slide != 0) {
				throw new IllegalArgumentException(
					"SlidingAlignedEventTimeTrigger parameters must satisfy size % slide = 0");
			}
			this.size = size;
			this.slide = slide;
			this.offset = offset;
		}

		@Override
		public long nextTriggerTime(long clockTime) {
			// pane size is slide, so the logic is similar to tumbling
			long start = TimeWindow.getWindowStartWithOffset(clockTime, offset, slide);
			long max = start + slide - 1;
			return max > clockTime ? max : max + slide;
		}

		@Override
		public TimeWindow nextTriggerWindow(long clockTime) {
			long maxTimestamp = nextTriggerTime(clockTime);
			long windowEnd = maxTimestamp + 1;
			return new TimeWindow(windowEnd - size, windowEnd);
		}
	}
}
