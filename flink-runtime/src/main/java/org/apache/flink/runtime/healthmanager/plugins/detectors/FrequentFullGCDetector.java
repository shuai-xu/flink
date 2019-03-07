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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * FrequentFullGCDetector detects full GCs count of a job.
 * Detects {@link JobVertexFrequentFullGC} on locating TaskManager of any task of the vertex,
 * full gc occur count is higher than the threshold within the interval.
 */
public class FrequentFullGCDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(FrequentFullGCDetector.class);

	private static final long MINUTE = 60 * 1000L;

	public static final ConfigOption<Integer> FULL_GC_COUNT_THRESHOLD =
		ConfigOptions.key("healthmonitor.full-gc-detector.threshold.perMin").defaultValue(5);
	public static final ConfigOption<Integer> FULL_GC_COUNT_SEVERE_THRESHOLD =
		ConfigOptions.key("healthmonitor.full-gc-detector.severe-threshold.perMin").defaultValue(10);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;
	private HealthMonitor monitor;

	private int gcCountThreshold;
	private int gcCountSevereThreshold;
	private long gcCheckInterval;

	private JobTMMetricSubscription gcMetricSubscription;

	@Override
	public void open(HealthMonitor monitor) {

		this.monitor = monitor;
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		gcCheckInterval = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_INTERVAL);
		gcCountThreshold = (int) (monitor.getConfig().getInteger(FULL_GC_COUNT_THRESHOLD) * (gcCheckInterval / MINUTE));
		gcCountSevereThreshold = (int) (monitor.getConfig().getInteger(FULL_GC_COUNT_SEVERE_THRESHOLD) * (gcCheckInterval / MINUTE));

		gcMetricSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.FULL_GC_COUNT_METRIC, gcCheckInterval, TimelineAggType.RANGE);
	}

	@Override
	public void close() {
		if (metricProvider != null && gcMetricSubscription != null) {
			metricProvider.unsubscribe(gcMetricSubscription);
			gcMetricSubscription = null;
		}
	}

	@Override
	public Symptom detect() {
		LOGGER.debug("Start detecting.");

		Map<String, Tuple2<Long, Double>> gcCount = gcMetricSubscription.getValue();
		if (gcCount == null || gcCount.isEmpty()) {
			return null;
		}

		boolean severe = false;
		Set<JobVertexID> jobVertexIDs = new HashSet<>();
		for (String tmId : gcCount.keySet()) {
			if (!MetricUtils.validateTmMetric(monitor, gcCheckInterval * 2, gcCount.get(tmId))) {
				LOGGER.debug("Skip vertex {}, metrics missing.", jobVertexIDs);
				continue;
			}

			double deltaGCCount = gcCount.get(tmId).f1;

			if (deltaGCCount > gcCountThreshold) {
				List<ExecutionVertexID> jobExecutionVertexIds = restServerClient.getTaskManagerTasks(tmId);
				if (jobExecutionVertexIds != null) {
					jobVertexIDs.addAll(jobExecutionVertexIds.stream().map(ExecutionVertexID::getJobVertexID).collect(Collectors.toList()));
				}
				if (deltaGCCount > gcCountSevereThreshold) {
					severe = true;
				}
			}
			LOGGER.debug("tm {} gc count {}", tmId, gcCount.get(tmId));
		}

		if (jobVertexIDs != null && !jobVertexIDs.isEmpty()) {
			LOGGER.info("Frequent full gc detected for vertices {}.", jobVertexIDs);
			return new JobVertexFrequentFullGC(jobID, new ArrayList<>(jobVertexIDs), severe);
		}
		return null;
	}
}
