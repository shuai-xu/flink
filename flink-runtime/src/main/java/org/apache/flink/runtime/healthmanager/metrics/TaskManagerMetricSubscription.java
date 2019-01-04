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

package org.apache.flink.runtime.healthmanager.metrics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggregator;

/**
 * Subscribe a metric of a TM.
 */
public class TaskManagerMetricSubscription extends MetricSubscription<Tuple2<Long, Double>> {

	private String tmId;

	private TimelineAggregator timelineAggregator;

	public TaskManagerMetricSubscription(String tmId, String metricName,
			TimelineAggType timelineAggType,
			long interval) {
		super(metricName, timelineAggType, interval);
		this.tmId = tmId;
		this.timelineAggregator = TimelineAggregator.createTimelineAggregator(getTimelineAggType(), getInterval());
	}

	@Override
	public Tuple2<Long, Double> getValue() {
		return this.timelineAggregator.getValue();
	}

	public String getTmId() {
		return tmId;
	}

	public void addValue(Tuple2<Long, Double> value) {
		this.timelineAggregator.addValue(value);
	}
}
