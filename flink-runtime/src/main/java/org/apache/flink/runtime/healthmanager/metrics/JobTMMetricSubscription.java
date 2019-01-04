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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Subscribe metric from all TM running tasks of the given job.
 */
public class JobTMMetricSubscription extends MetricSubscription<Map<String, Tuple2<Long, Double>>> {

	private JobID jobID;

	private Map<String, TimelineAggregator> tmAggregators = new HashMap<>();

	public JobTMMetricSubscription(JobID jobID, String metricName,
			TimelineAggType timelineAggType,
			long interval) {
		super(metricName, timelineAggType, interval);
		this.jobID = jobID;
	}

	@Override
	public Map<String, Tuple2<Long, Double>> getValue() {
		Map<String, Tuple2<Long, Double>> metricValues = new HashMap<>();
		for (Map.Entry<String, TimelineAggregator> entry : tmAggregators.entrySet()) {
			Tuple2<Long, Double> value = entry.getValue().getValue();
			if (value != null) {
				metricValues.put(entry.getKey(), value);
			}
		}
		return metricValues;
	}

	public JobID getJobID() {
		return jobID;
	}

	public void addValue(Map<String, Tuple2<Long, Double>> subtaskMetrics) {

		// add new tm and update agg.
		for (String tmId: subtaskMetrics.keySet()) {
			if (!tmAggregators.containsKey(tmId)) {
				tmAggregators.put(
						tmId,
						TimelineAggregator.createTimelineAggregator(getTimelineAggType(), getInterval()));
			}
			tmAggregators.get(tmId).addValue(subtaskMetrics.get(tmId));
		}

		// remove tm which already released.
		List<String> removedTM = new LinkedList<>();
		for (String tmId : tmAggregators.keySet()) {
			if (!subtaskMetrics.containsKey(tmId)) {
				removedTM.add(tmId);
			}
		}

		for (String tmId : removedTM) {
			tmAggregators.remove(tmId);
		}
	}
}
