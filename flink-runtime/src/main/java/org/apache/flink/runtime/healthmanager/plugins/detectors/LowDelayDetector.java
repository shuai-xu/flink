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
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowDelay;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_DELAY;

/**
 * LowDelayDetector detects low delay of a job.
 * Detects {@link JobVertexLowDelay} if the max avg delay of tasks
 * of the same vertex is lower then the threshold.
 */
public class LowDelayDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(LowDelayDetector.class);

	public static final ConfigOption<Long> LOW_DELAY_THRESHOLD =
		ConfigOptions.key("healthmonitor.low-delay.threshold").defaultValue(5 * 60 * 1000L);

	private JobID jobID;
	private MetricProvider metricProvider;

	private long lowDelayCheckInterval;
	private long lowDelayThreshold;

	private Map<JobVertexID, TaskMetricSubscription> delaySubs;

	@Override
	public void open(HealthMonitor monitor) {
		jobID = monitor.getJobID();
		metricProvider = monitor.getMetricProvider();

		lowDelayCheckInterval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);
		lowDelayThreshold = monitor.getConfig().getLong(LOW_DELAY_THRESHOLD);

		delaySubs = new HashMap<>();
		for (JobVertexID vertexId : monitor.getJobConfig().getVertexConfigs().keySet()) {
			if (monitor.getJobConfig().getInputNodes().get(vertexId).size() == 0) {
				TaskMetricSubscription delaySub = metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_DELAY, MetricAggType.MAX, lowDelayCheckInterval, TimelineAggType.AVG);
				delaySubs.put(vertexId, delaySub);
			}
		}
	}

	@Override
	public void close() {
		if (metricProvider != null && delaySubs != null) {
			for (TaskMetricSubscription delaySub : delaySubs.values()) {
				if (delaySub != null) {
					metricProvider.unsubscribe(delaySub);
				}
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		long now = System.currentTimeMillis();

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : delaySubs.keySet()) {
			TaskMetricSubscription delaySub = delaySubs.get(vertexId);

			if (delaySub.getValue() == null || now - delaySub.getValue().f0 > lowDelayCheckInterval * 2) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			if (delaySub.getValue().f1 < lowDelayThreshold) {
				jobVertexIDs.add(vertexId);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			LOGGER.info("Low delay detected for vertices {}.", jobVertexIDs);
			return new JobVertexLowDelay(jobID, jobVertexIDs);
		}
		return null;
	}
}
