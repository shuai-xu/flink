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
 * Calculate value range of the metric in given interval.
 */
public class LatestTimelineAggregator extends TimelineAggregator {

	private long nextIntervalKey = -1;
	private long lastTimestamp = -1;
	private double lastValue = Double.NEGATIVE_INFINITY;

	public LatestTimelineAggregator(long interval) {
		super(interval);
	}

	@Override
	public void addValue(Tuple2<Long, Double> value) {
		if (nextIntervalKey == value.f0 / interval) {
			if (value.f0 > lastTimestamp) {
				lastValue = value.f1;
				lastTimestamp = value.f0;
			}
		} else if (nextIntervalKey < value.f0 / interval) {
			if (lastTimestamp > 0) {
				currentTimestamp = nextIntervalKey * interval;
				currentValue = lastValue;
			}
			nextIntervalKey = value.f0 / interval;
			lastValue = value.f1;
			lastTimestamp = value.f0;
		}
	}
}
