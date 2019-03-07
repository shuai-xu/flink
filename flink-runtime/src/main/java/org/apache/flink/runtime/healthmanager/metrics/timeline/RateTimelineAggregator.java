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
 * Calculate rate of the metric in given interval.
 */
public class RateTimelineAggregator extends TimelineAggregator {

	private long nextIntervalKey = -1;
	private long maxTimestamp = -1;
	private long minTimestamp = -1;
	private double max = Double.NEGATIVE_INFINITY;
	private double min = Double.NEGATIVE_INFINITY;

	public RateTimelineAggregator(long interval) {
		super(interval);
	}

	@Override
	public void addValue(Tuple2<Long, Double> value) {
		if (nextIntervalKey == value.f0 / interval) {
			if (value.f0 > maxTimestamp) {
				max = value.f1;
				maxTimestamp = value.f0;
			}
			if (value.f0 < minTimestamp) {
				min = value.f1;
				minTimestamp = value.f0;
			}
		} else if (nextIntervalKey < value.f0 / interval) {
			if (max != Double.NEGATIVE_INFINITY && min != Double.NEGATIVE_INFINITY && maxTimestamp != minTimestamp) {
				currentTimestamp = nextIntervalKey * interval;
				currentValue = (max - min) * 1000.0D / (maxTimestamp - minTimestamp);
			}
			nextIntervalKey = value.f0 / interval;
			max = value.f1;
			maxTimestamp = value.f0;
			min = value.f1;
			minTimestamp = value.f0;
		}
	}
}
