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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLargeTimerCount;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.TASK_TIMER_COUNT;

/**
 * The LargeTimerCountDetector detects the timer count of a job.
 * Detects {@link JobVertexLargeTimerCount} if the max avg timer count
 * of tasks of the same vertex is higher then the threshold.
 * Generally, this detector is used to detect heap memory usage of window operator.
 */
public class LargeTimerCountDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(LargeTimerCountDetector.class);

	public static final ConfigOption<Long> LARGE_TIMER_COUNT_THRESHOLD =
		ConfigOptions.key("healthmonitor.timer-count.threshold").defaultValue(6000000L);

	private JobID jobID;
	private MetricProvider metricProvider;
	private HealthMonitor monitor;

	private final long timerCountCheckInterval = 1;
	private long largeTimerCountThreshold;

	private Map<JobVertexID, TaskMetricSubscription> timerCountSubs;

	@Override
	public void open(HealthMonitor monitor) {
		jobID = monitor.getJobID();
		metricProvider = monitor.getMetricProvider();
		this.monitor = monitor;

		//timerCountCheckInterval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);
		largeTimerCountThreshold = monitor.getConfig().getLong(LARGE_TIMER_COUNT_THRESHOLD);

		timerCountSubs = new HashMap<>();
		for (JobVertexID vertexId : monitor.getJobConfig().getVertexConfigs().keySet()) {
			TaskMetricSubscription timerCountSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_TIMER_COUNT, MetricAggType.MAX, timerCountCheckInterval, TimelineAggType.LATEST);
			timerCountSubs.put(vertexId, timerCountSub);
		}
	}

	@Override
	public void close() {
		if (metricProvider != null && timerCountSubs != null) {
			for (TaskMetricSubscription timerCountSub : timerCountSubs.values()) {
				if (timerCountSub != null) {
					metricProvider.unsubscribe(timerCountSub);
				}
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : timerCountSubs.keySet()) {
			TaskMetricSubscription timerCountSub = timerCountSubs.get(vertexId);

			if (!MetricUtils.validateTmMetric(
				monitor, timerCountCheckInterval, timerCountSub.getValue())) {
				LOGGER.debug("Skip vertex {} metric {}, metrics missing.", vertexId, timerCountSub.getValue());
				continue;
			}

			LOGGER.debug("Vertex {} timer count {}", vertexId, timerCountSub.getValue());

			if (timerCountSub.getValue().f1 > largeTimerCountThreshold) {
				jobVertexIDs.add(vertexId);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			LOGGER.info("Large timer count detected for vertices {}.", jobVertexIDs);
			return new JobVertexLargeTimerCount(jobID, jobVertexIDs);
		}
		return null;
	}
}
