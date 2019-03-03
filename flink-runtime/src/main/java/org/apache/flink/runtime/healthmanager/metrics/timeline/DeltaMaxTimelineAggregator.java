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

package org.apache.flink.runtime.healthmanager.metrics.timeline;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Calculate max value of the metric in given interval.
 */
public class DeltaMaxTimelineAggregator extends TimelineAggregator {

	private long nextIntervalKey = -1;
	private double max = Double.MIN_VALUE;
	private long lastTimestamp = -1;
	private double lastValue = 0;

	public DeltaMaxTimelineAggregator(long interval) {
		super(interval);
	}

	@Override
	public void addValue(Tuple2<Long, Double> value) {
		if (lastTimestamp >= value.f0) {
			// duplicated data, ignored.
			return;
		}
		double delta = value.f1 - lastValue;
		lastValue = value.f1;
		lastTimestamp = value.f0;

		if (nextIntervalKey == value.f0 / interval && max < delta) {
			max = delta;
		} else if (nextIntervalKey < value.f0 / interval) {
			if (max != Double.MIN_VALUE) {
				currentTimestamp = nextIntervalKey * interval;
				currentValue = max;
			}
			nextIntervalKey = value.f0 / interval;
			max = delta;
		}
	}
}
