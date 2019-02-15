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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * JobStableDetector generate an JobStable symptom when all task running keep running for a given period.
 */
public class JobStableDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobStableDetector.class);

	public static final ConfigOption<Boolean> INPUT_REQUIRED =
		ConfigOptions.key("healthmonitor.job-stable-detector.input-required").defaultValue(true);

	private HealthMonitor monitor;

	private long interval;

	private Map<JobVertexID, TaskMetricSubscription> inputCountSubs;

	private long lastStableTime = 0;

	private boolean inputRequired;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;

		inputCountSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		interval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);

		inputRequired = monitor.getConfig().getBoolean(INPUT_REQUIRED);

		if (inputRequired) {
			for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
				inputCountSubs.put(vertexId,
					monitor.getMetricProvider().subscribeTaskMetric(
						monitor.getJobID(),
						vertexId,
						MetricNames.TASK_INPUT_COUNT,
						MetricAggType.MIN,
						interval,
						TimelineAggType.EARLIEST));
			}
		}
	}

	@Override
	public void close() {

	}

	@Override
	public Symptom detect() throws Exception {

		RestServerClient.JobStatus status = this.monitor.getRestServerClient().getJobStatus(this.monitor.getJobID());
		long startRunningTime = Long.MIN_VALUE;
		for (Tuple2<Long, ExecutionState> state: status.getTaskStatus().values()) {
			if (!state.f1.equals(ExecutionState.RUNNING)) {
				LOGGER.debug("Some task not running yet!");
				return JobStable.UNSTABLE;
			}
			if (startRunningTime < state.f0) {
				startRunningTime = state.f0;
			}
		}

		if (inputRequired && this.monitor.getJobStartExecutionTime() > lastStableTime) {
			long startInputTime = Long.MIN_VALUE;
			for (JobVertexID vertexID : inputCountSubs.keySet()) {
				Tuple2<Long, Double> currentInputCount = inputCountSubs.get(vertexID).getValue();
				if (!MetricUtils.validateTaskMetric(monitor, interval * 2, inputCountSubs.get(vertexID))
						|| currentInputCount.f1 == 0) {
					LOGGER.debug("Some task has no input yet!");
					return JobStable.UNSTABLE;
				}
				if (startInputTime < currentInputCount.f0) {
					startInputTime = currentInputCount.f0;
				}
			}
			lastStableTime = startInputTime;
		}

		if (lastStableTime < startRunningTime) {
			lastStableTime = startRunningTime;
		}

		long stableTime = System.currentTimeMillis() - lastStableTime;
		LOGGER.debug("JobStable detected, stable time {} ms.", stableTime);
		return new JobStable(stableTime);
	}
}
