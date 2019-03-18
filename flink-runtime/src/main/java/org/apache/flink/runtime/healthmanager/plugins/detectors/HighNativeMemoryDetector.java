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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighNativeMemory;
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

/**
 * HighNativeMemoryDetector detects TM memory overuse of a job.
 */
public class HighNativeMemoryDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(HighNativeMemoryDetector.class);

	public static final ConfigOption<Double> HIGH_NATIVE_MEM_THRESHOLD =
		ConfigOptions.key("healthmonitor.high-native-mem-detector.threshold").defaultValue(0.8);
	public static final ConfigOption<Double> HIGH_NATIVE_MEM_SEVERE_THRESHOLD =
		ConfigOptions.key("healthmonitor.high-native-mem-detector.severe.threshold").defaultValue(1.2);
	public static final ConfigOption<Double> HIGH_NATIVE_MEM_CRITICAL_THRESHOLD =
		ConfigOptions.key("healthmonitor.high-native-mem-detector.critical.threshold").defaultValue(Double.MAX_VALUE);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;
	private HealthMonitor monitor;

	private long overuseCheckInterval;
	private double threshold;
	private double severeThreshold;
	private double criticalThreshold;

	private JobTMMetricSubscription tmMemCapacitySubscription;
	private JobTMMetricSubscription tmMemUsageTotalSubscription;
	private JobTMMetricSubscription tmMemUsageHeapSubscription;
	private JobTMMetricSubscription tmMemUsageNonHeapSubscription;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		overuseCheckInterval = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_INTERVAL);
		threshold = monitor.getConfig().getDouble(HIGH_NATIVE_MEM_THRESHOLD);
		severeThreshold = monitor.getConfig().getDouble(HIGH_NATIVE_MEM_SEVERE_THRESHOLD);
		criticalThreshold = monitor.getConfig().getDouble(HIGH_NATIVE_MEM_CRITICAL_THRESHOLD);

		tmMemCapacitySubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_CAPACITY,
			overuseCheckInterval, TimelineAggType.MAX);
		tmMemUsageTotalSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_USAGE_TOTAL,
			overuseCheckInterval, TimelineAggType.MAX);
		tmMemUsageHeapSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_HEAP_COMMITTED,
			overuseCheckInterval, TimelineAggType.MAX);
		tmMemUsageNonHeapSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_NON_HEAP_COMMITTED,
			overuseCheckInterval, TimelineAggType.MAX);
	}

	@Override
	public void close() {
		if (metricProvider != null && tmMemCapacitySubscription != null) {
			metricProvider.unsubscribe(tmMemCapacitySubscription);
		}

		if (metricProvider != null && tmMemUsageHeapSubscription != null) {
			metricProvider.unsubscribe(tmMemUsageTotalSubscription);
		}

		if (metricProvider != null && tmMemUsageHeapSubscription != null) {
			metricProvider.unsubscribe(tmMemUsageHeapSubscription);
		}

		if (metricProvider != null && tmMemUsageNonHeapSubscription != null) {
			metricProvider.unsubscribe(tmMemUsageNonHeapSubscription);
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		Map<String, Tuple2<Long, Double>> tmCapacities = tmMemCapacitySubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmTotalUsages = tmMemUsageTotalSubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmHeapUsages = tmMemUsageHeapSubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmNonHeapUsages = tmMemUsageNonHeapSubscription.getValue();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		if (tmCapacities == null || tmCapacities.isEmpty() ||
			tmTotalUsages == null || tmTotalUsages.isEmpty() ||
			tmHeapUsages == null || tmHeapUsages.isEmpty() ||
			tmNonHeapUsages == null || tmNonHeapUsages.isEmpty()) {
			return null;
		}

		boolean severe = false;
		boolean critical = false;
		Map<JobVertexID, Double> vertexMaxUtility = new HashMap<>();
		for (String tmId : tmCapacities.keySet()) {
			if (!MetricUtils.validateTmMetric(monitor, overuseCheckInterval * 2,
				tmCapacities.get(tmId), tmTotalUsages.get(tmId), tmHeapUsages.get(tmId), tmNonHeapUsages.get(tmId))) {
				LOGGER.debug("Skip tm {}, metrics missing.", tmId);
				continue;
			}

			double capacity = tmCapacities.get(tmId).f1;
			double totalUsage = tmTotalUsages.get(tmId).f1;
			double heapUsage = tmHeapUsages.get(tmId).f1;
			double nonHeapUsage = tmNonHeapUsages.get(tmId).f1;

			LOGGER.debug("TM {}, capacity {}, usage total {}, heap {}, non-heap {}.", tmId, capacity, totalUsage, heapUsage, nonHeapUsage);
			if (totalUsage <= capacity * threshold) {
				continue;
			}

			if (totalUsage > capacity * severeThreshold) {
				severe = true;
			}

			if (totalUsage > capacity * criticalThreshold) {
				critical = true;
			}

			List<ExecutionVertexID> jobExecutionVertexIds = restServerClient.getTaskManagerTasks(tmId);
			if (jobExecutionVertexIds == null) {
				continue;
			}

			double nativeUsage = (totalUsage - heapUsage - nonHeapUsage) / 1024 / 1024;
			if (nativeUsage < 0.0) {
				LOGGER.debug("Skip tm {}, abnormal native usage {}.", tmId, nativeUsage);
				continue;
			}

			double nativeCapacity = 0.0;
			for (ExecutionVertexID executionVertexID : jobExecutionVertexIds) {
				JobVertexID jobVertexID = executionVertexID.getJobVertexID();
				nativeCapacity += jobConfig.getVertexConfigs().get(jobVertexID).getResourceSpec().getNativeMemory();
			}

			if (nativeCapacity == 0.0) {
				nativeCapacity = 1.0;
			}

			double utility = nativeUsage / nativeCapacity;
			for (ExecutionVertexID executionVertexID : jobExecutionVertexIds) {
				JobVertexID jobVertexID = executionVertexID.getJobVertexID();
				if (!vertexMaxUtility.containsKey(jobVertexID) || vertexMaxUtility.get(jobVertexID) < utility) {
					vertexMaxUtility.put(jobVertexID, utility);
				}
			}
		}

		if (vertexMaxUtility != null && !vertexMaxUtility.isEmpty()) {
			LOGGER.info("Native memory high detected for vertices with max utility {}.", vertexMaxUtility);
			return new JobVertexHighNativeMemory(jobID, vertexMaxUtility, severe, critical);
		}
		return null;
	}
}
