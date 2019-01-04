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

import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;

/**
 * Metric subscription will aggregate all metric fetched and aggregate according different requirement.
 * @param <T> the return type of the description.
 */
public abstract class MetricSubscription<T> {

	private String metricName;

	private TimelineAggType timelineAggType;

	private long interval;

	public MetricSubscription(String metricName, TimelineAggType timelineAggType, long interval) {
		this.metricName = metricName;
		this.timelineAggType = timelineAggType;
		this.interval = interval;
	}

	public abstract T getValue();

	public String getMetricName() {
		return metricName;
	}

	public TimelineAggType getTimelineAggType() {
		return timelineAggType;
	}

	public long getInterval() {
		return interval;
	}
}
