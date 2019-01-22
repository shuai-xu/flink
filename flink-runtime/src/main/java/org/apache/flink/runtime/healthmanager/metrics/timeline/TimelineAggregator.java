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

import org.apache.commons.lang3.NotImplementedException;

/**
 * An TimelineAggregator will be aggregate value of a metric in a specified interval with different
 * aggregation function.
 */
public abstract class TimelineAggregator {

	protected long currentTimestamp = -1;
	protected double currentValue = Double.MIN_VALUE;

	protected long interval;

	public TimelineAggregator(long interval) {
		this.interval = interval;
	}

	/**
	 * Add a value to the timeline.
	 * @param value timestamp and value of a metric.
	 */
	public abstract void addValue(Tuple2<Long, Double> value);

	/**
	 * Get the value of the timeline after agg.
	 * @return timestamp and timeline.
	 */
	public Tuple2<Long, Double> getValue() {
		if (currentTimestamp == -1) {
			return null;
		}
		return Tuple2.of(currentTimestamp, currentValue);
	}

	/**
	 * Create a timeline Aggregator.
	 * @param timelineAggType agg type of the aggregator
	 * @param interval        timeline intervale of the aggregator
	 * @return timeline aggregator with given agg type and interval.
	 */
	public static TimelineAggregator createTimelineAggregator(TimelineAggType timelineAggType, long interval) {
		switch (timelineAggType) {
			case AVG:
				return new AvgTimelineAggregator(interval);
			case MAX:
				return new MaxTimelineAggregator(interval);
			case MIN:
				return new MinTimelineAggregator(interval);
			case RATE:
				return new RateTimelineAggregator(interval);
			case RANGE:
				return new RangeTimelineAggregator(interval);
			default:
				throw new NotImplementedException("Agg type:" + timelineAggType);
		}
	}
}
