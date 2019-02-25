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
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
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

	public static final ConfigOption<Long> INTERVAL =
		ConfigOptions.key("healthmonitor.job-stable-detector.interval.ms").defaultValue(30_000L);

	private HealthMonitor monitor;
	private MetricProvider metricProvider;

	private long interval;

	private long lastStableTime = 0;

	private Map<JobVertexID, TaskMetricSubscription> initTimeSubs;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.metricProvider = monitor.getMetricProvider();

		initTimeSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		interval = monitor.getConfig().getLong(INTERVAL);

		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			initTimeSubs.put(vertexId,
				metricProvider.subscribeTaskMetric(
					monitor.getJobID(),
					vertexId,
					MetricNames.TASK_INIT_TIME,
					MetricAggType.MIN,
					interval,
					TimelineAggType.LATEST));
		}
	}

	@Override
	public void close() {
		if (metricProvider != null && initTimeSubs != null) {
			for (TaskMetricSubscription sub : initTimeSubs.values()) {
				if (sub != null) {
					metricProvider.unsubscribe(sub);
				}
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {

		RestServerClient.JobStatus status = this.monitor.getRestServerClient().getJobStatus(this.monitor.getJobID());
		long allTaskRunningTime = Long.MIN_VALUE;
		for (Tuple2<Long, ExecutionState> state: status.getTaskStatus().values()) {
			if (!state.f1.equals(ExecutionState.RUNNING)) {
				LOGGER.debug("Some task not running yet!");
				return JobStable.UNSTABLE;
			}
			if (allTaskRunningTime < state.f0) {
				allTaskRunningTime = state.f0;
			}
		}

		if (lastStableTime < allTaskRunningTime) {
			long allTaskInitializedTime = Long.MIN_VALUE;
			for (JobVertexID vertexID : initTimeSubs.keySet()) {
				TaskMetricSubscription sub = initTimeSubs.get(vertexID);
				if (!MetricUtils.validateTaskMetric(monitor, interval * 2, sub) ||
					sub.getValue().f0 < allTaskRunningTime ||
					sub.getValue().f1 < 0.0) {
					LOGGER.debug("Some task has not initialized yet!");
					return JobStable.UNSTABLE;
				}
				if (allTaskInitializedTime < sub.getValue().f0) {
					allTaskInitializedTime = sub.getValue().f0;
				}
			}
			lastStableTime = allTaskInitializedTime;
		}

		long stableTime = System.currentTimeMillis() - lastStableTime;
		LOGGER.debug("JobStable detected, stable time {} ms.", stableTime);
		return new JobStable(stableTime);
	}
}
