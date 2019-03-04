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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
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
 * This detector detects full GCs' time of a job.
 * Detects {@link JobVertexLongTimeFullGC} on locating TaskManager of any task of the vertex,
 * full gc occur count is higher than the threshold within the interval.
 */
public class LongTimeFullGCDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(LongTimeFullGCDetector.class);

	public static final ConfigOption<Long> FULL_GC_TIME_THRESHOLD =
		ConfigOptions.key("healthmonitor.full-gc-detector.time.threshold").defaultValue(5000L);
	public static final ConfigOption<Long> FULL_GC_TIME_SEVERE_THRESHOLD =
		ConfigOptions.key("healthmonitor.full-gc-detector.time.severe-threshold").defaultValue(10000L);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;
	private HealthMonitor monitor;

	private long gcTimeThreshold;
	private long gcTimeSevereThreshold;
	private final long gcCheckInterval = 1L;

	private JobTMMetricSubscription gcTimeSubscription;
	private JobTMMetricSubscription gcCountSubscription;

	@Override
	public void open(HealthMonitor monitor) {

		this.monitor = monitor;
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		gcTimeThreshold = monitor.getConfig().getLong(FULL_GC_TIME_THRESHOLD);
		gcTimeSevereThreshold = monitor.getConfig().getLong(FULL_GC_TIME_SEVERE_THRESHOLD);

		gcTimeSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.FULL_GC_TIME_METRIC, gcCheckInterval, TimelineAggType.RANGE);
		gcCountSubscription = metricProvider.subscribeAllTMMetric(jobID, MetricNames.FULL_GC_COUNT_METRIC, gcCheckInterval, TimelineAggType.RANGE);
	}

	@Override
	public void close() {
		if (metricProvider != null && gcTimeSubscription != null && gcCountSubscription != null) {
			metricProvider.unsubscribe(gcTimeSubscription);
			metricProvider.unsubscribe(gcCountSubscription);
			gcTimeSubscription = null;
			gcCountSubscription = null;
		}
	}

	@Override
	public Symptom detect() {
		LOGGER.debug("Start detecting.");

		Map<String, Tuple2<Long, Double>> gcTime = gcTimeSubscription.getValue();
		Map<String, Tuple2<Long, Double>> gcCount = gcCountSubscription.getValue();
		if (gcTime == null || gcTime.isEmpty() || gcCount == null || gcCount.isEmpty()) {
			return null;
		}

		boolean severe = false;
		boolean critical = false;
		Set<JobVertexID> jobVertexIDs = new HashSet<>();
		for (String tmId : gcTime.keySet()) {
			if (!gcCount.containsKey(tmId)
				|| !MetricUtils.validateTmMetric(monitor, gcCheckInterval, gcTime.get(tmId), gcCount.get(tmId))
				|| gcCount.get(tmId).f1 < 1) {
				LOGGER.debug("Skip vertex {}, GC metrics missing.", jobVertexIDs);
				continue;
			}

			double perGCDeltaTime = gcTime.get(tmId).f1 / gcCount.get(tmId).f1;

			if (perGCDeltaTime > gcTimeThreshold) {
				List<ExecutionVertexID> jobExecutionVertexIds = restServerClient.getTaskManagerTasks(tmId);
				if (jobExecutionVertexIds != null) {
					jobVertexIDs.addAll(jobExecutionVertexIds.stream().map(ExecutionVertexID::getJobVertexID).collect(
						Collectors.toList()));
				}
				if (perGCDeltaTime > gcTimeSevereThreshold) {
					severe = true;
					critical = true;
				}
			}
			LOGGER.debug("tm {} gc time {}", tmId, perGCDeltaTime);
		}

		if (jobVertexIDs != null && !jobVertexIDs.isEmpty()) {
			LOGGER.info("Long time full gc detected for vertices {}.", jobVertexIDs);
			return new JobVertexLongTimeFullGC(jobID, new ArrayList<>(jobVertexIDs), severe, critical);
		}
		return null;
	}
}

