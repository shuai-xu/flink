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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighCpu;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HighCpuDetector detects high cpu usage of a job.
 * Detects {@link JobVertexHighCpu} if the max avg cpu usage of the TM
 * is higher than threshold.
 */
public class HighCpuDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(HighCpuDetector.class);

	public static final ConfigOption<Double> HIGH_CPU_THRESHOLD =
		ConfigOptions.key("healthmonitor.high-cpu-detector.threashold").defaultValue(0.8);
	public static final ConfigOption<Double> HIGH_CPU_SEVERE_THRESHOLD =
		ConfigOptions.key("healthmonitor.high-cpu-detector.severe.threashold").defaultValue(1.2);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;
	private HealthMonitor monitor;

	private long checkInterval;
	private double threshold;
	private double severeThreshold;

	private JobTMMetricSubscription tmCpuAllocatedSubscription;
	private JobTMMetricSubscription tmCpuUsageSubscription;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		checkInterval = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_INTERVAL);
		threshold = monitor.getConfig().getDouble(HIGH_CPU_THRESHOLD);
		severeThreshold = monitor.getConfig().getDouble(HIGH_CPU_SEVERE_THRESHOLD);

		tmCpuAllocatedSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_CPU_CAPACITY, checkInterval, TimelineAggType.AVG);
		tmCpuUsageSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_CPU_USAGE, checkInterval, TimelineAggType.AVG);
	}

	@Override
	public void close() {
		if (metricProvider != null && tmCpuAllocatedSubscription != null) {
			metricProvider.unsubscribe(tmCpuAllocatedSubscription);
		}

		if (metricProvider != null && tmCpuUsageSubscription != null) {
			metricProvider.unsubscribe(tmCpuUsageSubscription);
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		Map<String, Tuple2<Long, Double>> tmCapacities = tmCpuAllocatedSubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmUsages = tmCpuUsageSubscription.getValue();

		if (tmCapacities == null || tmCapacities.isEmpty() || tmUsages == null || tmUsages.isEmpty()) {
			return null;
		}

		boolean severe = false;
		Map<JobVertexID, Double> vertexMaxUtility = new HashMap<>();
		for (String tmId : tmCapacities.keySet()) {
			if (!MetricUtils.validateTmMetric(monitor, checkInterval * 2, tmCapacities.get(tmId), tmUsages.get(tmId))) {
				LOGGER.debug("Skip tm {}, metrics missing.", tmId);
				continue;
			}

			double capacity = tmCapacities.get(tmId).f1;
			double usage = tmUsages.get(tmId).f1;
			LOGGER.debug("TM {}, capacity {}, usage {}.", tmId, capacity, usage);
			if (capacity == 0.0) {
				LOGGER.warn("Skip vertex {}, capacity is 0. SHOULD NOT HAPPEN!", tmId);
				continue;
			}
			double utility = usage / capacity;

			if (utility > threshold) {
				if (utility > severeThreshold) {
					severe = true;
				}
				List<ExecutionVertexID> jobExecutionVertexIds = restServerClient.getTaskManagerTasks(tmId);
				for (ExecutionVertexID jobExecutionVertexId : jobExecutionVertexIds) {
					JobVertexID jvId = jobExecutionVertexId.getJobVertexID();
					if (!vertexMaxUtility.containsKey(jvId) || vertexMaxUtility.get(jvId) < utility) {
						vertexMaxUtility.put(jvId, utility);
					}
				}
				LOGGER.debug("Cpu high detected for tm {}, capacity {}, usage {}, utility {}, tasks of vertices {}.",
					tmId,
					capacity,
					usage,
					utility,
					jobExecutionVertexIds.stream().map(evid -> evid.getJobVertexID()).collect(Collectors.toList()));
			}
		}

		if (vertexMaxUtility != null && !vertexMaxUtility.isEmpty()) {
			LOGGER.info("Cpu high detected for vertices with max utilities {}.", vertexMaxUtility);
			return new JobVertexHighCpu(jobID, vertexMaxUtility, severe);
		}
		return null;
	}
}
