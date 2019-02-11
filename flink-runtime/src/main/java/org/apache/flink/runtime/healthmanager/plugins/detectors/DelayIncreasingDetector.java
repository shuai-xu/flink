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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDelayIncreasing;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_DELAY;

/**
 * DelayIncreasingDetector detects delay increasing of a job.
 * Detects {@link JobVertexDelayIncreasing} if avg delay increasing rate of tasks
 * of the same vertex is higher then the threshold.
 */
public class DelayIncreasingDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(DelayIncreasingDetector.class);

	private static final ConfigOption<Long> DELAY_INCREASING_THRESHOLD =
		ConfigOptions.key("healthmonitor.delay-increasing.threshold.msps").defaultValue(0L);

	private JobID jobID;
	private HealthMonitor healthMonitor;
	private MetricProvider metricProvider;

	private long delayIncreasingCheckInterval;
	private double delayIncreasingThreshold;

	private Map<JobVertexID, TaskMetricSubscription> delayRateSubs;

	@Override
	public void open(HealthMonitor monitor) {
		healthMonitor = monitor;
		jobID = monitor.getJobID();
		metricProvider = monitor.getMetricProvider();

		delayIncreasingCheckInterval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);
		delayIncreasingThreshold = monitor.getConfig().getLong(DELAY_INCREASING_THRESHOLD);

		delayRateSubs = new HashMap<>();
		for (JobVertexID vertexId : monitor.getJobConfig().getVertexConfigs().keySet()) {
			if (monitor.getJobConfig().getInputNodes().get(vertexId).size() == 0) {
				TaskMetricSubscription delaySub = metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_DELAY, MetricAggType.AVG, delayIncreasingCheckInterval, TimelineAggType.RATE);
				delayRateSubs.put(vertexId, delaySub);
			}
		}
	}

	@Override
	public void close() {
		if (metricProvider != null && delayRateSubs != null) {
			for (TaskMetricSubscription delaySub : delayRateSubs.values()) {
				if (delaySub != null) {
					metricProvider.unsubscribe(delaySub);
				}
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : delayRateSubs.keySet()) {
			TaskMetricSubscription delayRateSub = delayRateSubs.get(vertexId);

			if (!MetricUtils.validateTaskMetric(healthMonitor, delayIncreasingCheckInterval * 2, delayRateSub)) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			if (delayRateSub.getValue().f1 > delayIncreasingThreshold) {
				jobVertexIDs.add(vertexId);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			LOGGER.info("Delay increasing detected for vertices {}.", jobVertexIDs);
			return new JobVertexDelayIncreasing(jobID, jobVertexIDs);
		}
		return null;
	}
}
