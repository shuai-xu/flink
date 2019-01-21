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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggregator;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metric subscription for a task metric.
 */
public class TaskMetricSubscription extends MetricSubscription<Tuple2<Long, Double>> {

	private JobID jobID;
	private JobVertexID jobVertexID;
	private MetricAggFunction subtaskAggFunction;
	private MetricAggType subtaskAggType;

	private Map<Integer, TimelineAggregator> subtaskAggregators = new HashMap<>();

	public TaskMetricSubscription(
			JobID jobID, JobVertexID jobVertexID, MetricAggType subtaskAggType,
			String metricName, TimelineAggType timelineAggType, long interval) {
		super(metricName, timelineAggType, interval);
		this.jobID = jobID;
		this.jobVertexID = jobVertexID;
		this.subtaskAggFunction = MetricAggFunction.getMetricAggFunction(subtaskAggType);
		this.subtaskAggType = subtaskAggType;
	}

	@Override
	public Tuple2<Long, Double> getValue() {
		return subtaskAggFunction.getValue(getSubTaskMetricValues());
	}

	private List<Tuple2<Long, Double>> getSubTaskMetricValues() {
		List<Tuple2<Long, Double>> subtaskValues = new ArrayList<>(subtaskAggregators.size());
		for (TimelineAggregator timelineAggregator : subtaskAggregators.values()) {
			subtaskValues.add(timelineAggregator.getValue());
		}
		return subtaskValues;
	}

	public void addValue(Map<Integer, Tuple2<Long, Double>> subtaskMetrics) {
		for (Integer subtaskIndex : subtaskMetrics.keySet()) {
			if (!subtaskAggregators.containsKey(subtaskIndex)) {
				subtaskAggregators.put(
						subtaskIndex,
						TimelineAggregator.createTimelineAggregator(getTimelineAggType(), getInterval()));
			}
			subtaskAggregators.get(subtaskIndex).addValue(subtaskMetrics.get(subtaskIndex));
		}

		// task parallelism changed.
		if (subtaskMetrics.size() != subtaskAggregators.size()) {
			// remove old data.
			subtaskAggregators.clear();
		}
	}

	public JobID getJobID() {
		return jobID;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}
}
