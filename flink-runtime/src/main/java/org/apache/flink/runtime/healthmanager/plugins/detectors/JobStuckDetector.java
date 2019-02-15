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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStuck;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.TASK_INPUT_COUNT;

/**
 * JobStuckDetector detects stuck of a job.
 * Detects {@link JobStuck} if tps is 0.
 */
public class JobStuckDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobStuckDetector.class);

	public static final ConfigOption<Long> JOB_STUCK_CHECK_INTERVAL =
		ConfigOptions.key("healthmonitor.job-stuck.interval.ms").defaultValue(60 * 1000L);

	private JobID jobID;
	private HealthMonitor healthMonitor;
	private MetricProvider metricProvider;
	private long checkInterval;

	private Map<JobVertexID, TaskMetricSubscription> inputTpsSubs;

	@Override
	public void open(HealthMonitor monitor) {
		healthMonitor = monitor;
		jobID = monitor.getJobID();
		metricProvider = monitor.getMetricProvider();
		checkInterval = monitor.getConfig().getLong(JOB_STUCK_CHECK_INTERVAL);

		inputTpsSubs = new HashMap<>();
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			inputTpsSubs.put(vertexId, metricProvider.subscribeTaskMetric(
					jobID, vertexId, TASK_INPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE));
		}
	}

	@Override
	public void close() {
		if (metricProvider != null && inputTpsSubs != null) {
			for (TaskMetricSubscription inputTpsSub : inputTpsSubs.values()) {
				if (inputTpsSub != null) {
					metricProvider.unsubscribe(inputTpsSub);
				}
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : inputTpsSubs.keySet()) {
			TaskMetricSubscription inputSub = inputTpsSubs.get(vertexId);

			if (!MetricUtils.validateTaskMetric(healthMonitor, checkInterval * 2, inputSub)) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			if (inputSub.getValue().f1 <= 0.0) {
				jobVertexIDs.add(vertexId);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			LOGGER.info("Job stuck detected for vertices {}.", jobVertexIDs);
			return new JobStuck(jobID, jobVertexIDs);
		}
		return null;
	}
}
