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
import org.apache.flink.api.common.operators.ResourceSpec;
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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * LowCpuDetector detects low cpu usage of a job.
 * Detects {@link JobVertexLowMemory} if the max avg memory usage of the TM
 * is lower than threshold.
 */
public class LowMemoryDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(LowMemoryDetector.class);

	public static final ConfigOption<Double> LOW_MEM_THRESHOLD =
		ConfigOptions.key("healthmonitor.low-memory-detector.threashold").defaultValue(0.7);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;
	private HealthMonitor monitor;

	private long checkInterval;
	private double threshold;
	private long waitTime;

	private JobTMMetricSubscription tmMemAllocatedSubscription;
	private JobTMMetricSubscription tmMemTotalUsageSubscription;
	private JobTMMetricSubscription tmMemHeapUsageSubscription;
	private JobTMMetricSubscription tmMemNonHeapUsageSubscription;

	private Map<JobVertexID, Long> lowMemSince;
	private Map<JobVertexID, Double> maxHeapUtility;
	private Map<JobVertexID, Double> maxNonHeapUtility;
	private Map<JobVertexID, Double> maxNativeUtility;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		checkInterval = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_INTERVAL);
		threshold = monitor.getConfig().getDouble(LOW_MEM_THRESHOLD);
		waitTime = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_DOWN_WAIT_TIME);

		tmMemAllocatedSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_CAPACITY, checkInterval, TimelineAggType.AVG);
		tmMemTotalUsageSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_USAGE_TOTAL, checkInterval, TimelineAggType.AVG);
		tmMemHeapUsageSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_HEAP_USED, checkInterval, TimelineAggType.AVG);
		tmMemNonHeapUsageSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.TM_MEM_NON_HEAP_USED, checkInterval, TimelineAggType.AVG);

		lowMemSince = new HashMap<>();
		maxHeapUtility = new HashMap<>();
		maxNonHeapUtility = new HashMap<>();
		maxNativeUtility = new HashMap<>();
	}

	@Override
	public void close() {
		if (metricProvider != null && tmMemAllocatedSubscription != null) {
			metricProvider.unsubscribe(tmMemAllocatedSubscription);
		}

		if (metricProvider != null && tmMemTotalUsageSubscription != null) {
			metricProvider.unsubscribe(tmMemTotalUsageSubscription);
		}

		if (metricProvider != null && tmMemHeapUsageSubscription != null) {
			metricProvider.unsubscribe(tmMemHeapUsageSubscription);
		}
		if (metricProvider != null && tmMemNonHeapUsageSubscription != null) {
			metricProvider.unsubscribe(tmMemNonHeapUsageSubscription);
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		long now = System.currentTimeMillis();

		Map<String, Tuple2<Long, Double>> tmCapacities = tmMemAllocatedSubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmTotalUsages = tmMemTotalUsageSubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmHeapUsages = tmMemHeapUsageSubscription.getValue();
		Map<String, Tuple2<Long, Double>> tmNonHeapUsages = tmMemNonHeapUsageSubscription.getValue();

		if (tmCapacities == null || tmCapacities.isEmpty() ||
			tmTotalUsages == null || tmTotalUsages.isEmpty() ||
			tmHeapUsages == null || tmHeapUsages.isEmpty() ||
			tmNonHeapUsages == null || tmNonHeapUsages.isEmpty()) {
			return null;
		}

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		Map<JobVertexID, Double> vertexTaskMaxTotalUtility = new HashMap<>();
		Map<JobVertexID, Double> vertexTaskMaxHeapUtility = new HashMap<>();
		Map<JobVertexID, Double> vertexTaskMaxNonHeapUtility = new HashMap<>();
		Map<JobVertexID, Double> vertexTaskMaxNativeUtility = new HashMap<>();
		for (String tmId : tmCapacities.keySet()) {
			if (!MetricUtils.validateTmMetric(monitor, checkInterval * 2,
				tmCapacities.get(tmId), tmTotalUsages.get(tmId), tmHeapUsages.get(tmId), tmNonHeapUsages.get(tmId))) {
				LOGGER.debug("Skip tm {}, metrics missing.", tmId);
				continue;
			}

			List<JobVertexID> vertexIds = restServerClient.getTaskManagerTasks(tmId)
				.stream().map(executionVertexID -> executionVertexID.getJobVertexID()).collect(Collectors.toList());

			double totalUsage = tmTotalUsages.get(tmId).f1 / 1024 / 1024;
			double heapUsage = tmHeapUsages.get(tmId).f1 / 1024 / 1024;
			double nonHeapUsage = tmNonHeapUsages.get(tmId).f1 / 1024 / 1024;
			double nativeUsage = totalUsage - heapUsage - nonHeapUsage;

			double totalCapacity = tmCapacities.get(tmId).f1 / 1024 / 1024;
			double heapCapacity = 0.0;
			double nonHeapCapacity = 0.0;
			double nativeCapacity = 0.0;

			for (JobVertexID vertexID : vertexIds) {
				ResourceSpec currentResource = jobConfig.getVertexConfigs().get(vertexID).getResourceSpec();
				heapCapacity += currentResource.getHeapMemory();
				nonHeapCapacity += currentResource.getDirectMemory();
				nativeCapacity += currentResource.getNativeMemory();
			}

			double totalUtility = totalUsage / (totalCapacity == 0.0 ? 1.0 : totalCapacity);
			double heapUtility = heapUsage / (heapCapacity == 0.0 ? 1.0 : heapCapacity);
			double nonHeapUtility = nonHeapUsage / (nonHeapCapacity == 0.0 ? 1.0 : nonHeapCapacity);
			double nativeUtility = nativeUsage / (nativeCapacity == 0.0 ? 1.0 : nativeCapacity);

			for (JobVertexID vertexID : vertexIds) {
				if (!vertexTaskMaxTotalUtility.containsKey(vertexID) || vertexTaskMaxTotalUtility.get(vertexID) < totalUtility) {
					vertexTaskMaxTotalUtility.put(vertexID, totalUtility);
				}

				if (!vertexTaskMaxHeapUtility.containsKey(vertexID) || vertexTaskMaxHeapUtility.get(vertexID) < heapUtility) {
					vertexTaskMaxHeapUtility.put(vertexID, heapUtility);
				}

				if (!vertexTaskMaxNonHeapUtility.containsKey(vertexID) || vertexTaskMaxNonHeapUtility.get(vertexID) < nonHeapUtility) {
					vertexTaskMaxNonHeapUtility.put(vertexID, nonHeapUtility);
				}

				if (!vertexTaskMaxNativeUtility.containsKey(vertexID) || vertexTaskMaxNativeUtility.get(vertexID) < nativeUtility) {
					vertexTaskMaxNativeUtility.put(vertexID, nativeUtility);
				}
			}
		}

		for (JobVertexID vertexID : vertexTaskMaxTotalUtility.keySet()) {
			if (vertexTaskMaxTotalUtility.get(vertexID) >= threshold) {
				lowMemSince.put(vertexID, Long.MAX_VALUE);
				maxHeapUtility.remove(vertexID);
				maxNonHeapUtility.remove(vertexID);
				maxNativeUtility.remove(vertexID);
			} else {
				lowMemSince.put(vertexID, Math.min(now, lowMemSince.getOrDefault(vertexID, Long.MAX_VALUE)));
				maxHeapUtility.put(vertexID, Math.max(
					vertexTaskMaxHeapUtility.get(vertexID), maxHeapUtility.getOrDefault(vertexID, 0.0)));
				maxNonHeapUtility.put(vertexID, Math.max(
					vertexTaskMaxNonHeapUtility.get(vertexID), maxNonHeapUtility.getOrDefault(vertexID, 0.0)));
				maxNativeUtility.put(vertexID, Math.max(
					vertexTaskMaxNativeUtility.get(vertexID), maxNativeUtility.getOrDefault(vertexID, 0.0)));
			}
			LOGGER.debug("Vertex {}, total utility {}, lowMemSince {}, maxHeapUtility {}, maxNonHeapUtility {}, maxNativeUtility {}.",
				vertexID,
				vertexTaskMaxTotalUtility.get(vertexID),
				lowMemSince.get(vertexID),
				maxHeapUtility.get(vertexID),
				maxNonHeapUtility.get(vertexID),
				maxNativeUtility.get(vertexID));
		}

		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(jobID);
		for (Map.Entry<JobVertexID, Long> entry : lowMemSince.entrySet()) {
			if (now - entry.getValue() > waitTime) {
				JobVertexID vertexID = entry.getKey();
				jobVertexLowMemory.addVertex(vertexID, maxHeapUtility.get(vertexID), maxNonHeapUtility.get(vertexID), maxNativeUtility.get(vertexID));
			}
		}

		if (jobVertexLowMemory.isEmpty()) {
			return null;
		}

		LOGGER.info("Memory low detected: {}.", jobVertexLowMemory);
		return jobVertexLowMemory;
	}
}
