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

package org.apache.flink.runtime.healthmanager.plugins.detectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.JobTMMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexNativeMemOveruse;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MemoryOveruseDetector detects TM memory overuse of a job.
 */
public class MemoryOveruseDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(MemoryOveruseDetector.class);

	private static final String TM_MEM_CAPACITY = "Status.ProcessTree.Memory.Allocated";
	private static final String TM_MEM_USAGE_TOTAL = "Status.ProcessTree.Memory.RSS";

	private static final ConfigOption<Long> MEMORY_OVERUSE_CHECK_INTERVAL =
		ConfigOptions.key("healthmonitor.memory-overuse-detector.interval.ms").defaultValue(5 * 60 * 1000L);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;

	private long overuseCheckInterval;

	private JobTMMetricSubscription tmMemCapacitySubscription;
	private JobTMMetricSubscription tmMemUsageTotalSubscription;

	@Override
	public void open(HealthMonitor monitor) {

		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		overuseCheckInterval = monitor.getConfig().getLong(MEMORY_OVERUSE_CHECK_INTERVAL);

		tmMemCapacitySubscription = metricProvider.subscribeAllTMMetric(jobID, TM_MEM_CAPACITY, overuseCheckInterval, TimelineAggType.AVG);
		tmMemUsageTotalSubscription = metricProvider.subscribeAllTMMetric(jobID, TM_MEM_USAGE_TOTAL, overuseCheckInterval, TimelineAggType.AVG);
	}

	@Override
	public void close() {
		if (metricProvider != null && tmMemCapacitySubscription != null) {
			metricProvider.unsubscribe(tmMemCapacitySubscription);
		}

		if (metricProvider != null && tmMemUsageTotalSubscription != null) {
			metricProvider.unsubscribe(tmMemUsageTotalSubscription);
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		long now = System.currentTimeMillis();

		Map<String, Tuple2<Long, Double>> tmCapacities = tmMemCapacitySubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmTotalUsages = tmMemUsageTotalSubscription.getValue();

		if (tmCapacities == null || tmCapacities.isEmpty() || tmTotalUsages == null || tmTotalUsages.isEmpty()) {
			return null;
		}

		Map<JobVertexID, Double> vertexMaxOveruse = new HashMap<>();
		for (String tmId : tmCapacities.keySet()) {
			if (now - tmCapacities.get(tmId).f0 > overuseCheckInterval * 2 ||
				now - tmTotalUsages.get(tmId).f0 > overuseCheckInterval * 2) {
				LOGGER.debug("Skip tm {}, metrics missing.", tmId);
				continue;
			}

			double capacity = tmCapacities.get(tmId).f1;
			double totalUsage = tmTotalUsages.get(tmId).f1;
			if (totalUsage <= capacity) {
				continue;
			}

			double overuse = totalUsage - capacity;
			List<ExecutionVertexID> jobExecutionVertexIds = restServerClient.getTaskManagerTasks(tmId);
			if (jobExecutionVertexIds != null && !jobExecutionVertexIds.isEmpty()) {
				overuse /= jobExecutionVertexIds.size();
				for (ExecutionVertexID jobExecutionVertexId : jobExecutionVertexIds) {
					JobVertexID jvId = jobExecutionVertexId.getJobVertexID();
					if (!vertexMaxOveruse.containsKey(jvId) || vertexMaxOveruse.get(jvId) < overuse) {
						vertexMaxOveruse.put(jvId, overuse);
					}
				}
			}
		}

		if (vertexMaxOveruse != null && !vertexMaxOveruse.isEmpty()) {
			LOGGER.info("Native memory overuse detected for vertices with max overuse {}.", vertexMaxOveruse);
			return new JobVertexNativeMemOveruse(jobID, vertexMaxOveruse);
		}
		return null;
	}
}
