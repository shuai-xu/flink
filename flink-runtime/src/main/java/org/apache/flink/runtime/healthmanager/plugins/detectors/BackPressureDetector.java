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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexBackPressure;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.WAIT_OUTPUT_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.WAIT_OUTPUT_SUM;

/**
 * BackPressureDetector detects back pressure of a job.
 * Detects {@link JobVertexBackPressure} if avg waitOutput per record of tasks
 * of the same vertex is higher than threshold.
 */
public class BackPressureDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(BackPressureDetector.class);

	private static final ConfigOption<Long> BACK_PRESSURE_CHECK_INTERVAL =
		ConfigOptions.key("healthmonitor.back-pressure.interval.ms").defaultValue(60 * 1000L);
	private static final ConfigOption<Double> BACK_PRESSURE_THRESHOLD =
		ConfigOptions.key("healthmonitor.back-pressure.threshold.ms").defaultValue(0.0);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;

	private long checkInterval;
	private double threshold;


	private Map<JobVertexID, TaskMetricSubscription> waitOutputCountMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputCountMinSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputSumMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputSumMinSubs;

	@Override
	public void open(HealthMonitor monitor) {
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		checkInterval = monitor.getConfig().getLong(BACK_PRESSURE_CHECK_INTERVAL);
		threshold = monitor.getConfig().getDouble(BACK_PRESSURE_THRESHOLD);

		waitOutputCountMaxSubs = new HashMap<>();
		waitOutputCountMinSubs = new HashMap<>();
		waitOutputSumMaxSubs = new HashMap<>();
		waitOutputSumMinSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = restServerClient.getJobConfig(jobID);
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription waitOutputCountMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			waitOutputCountMaxSubs.put(vertexId, waitOutputCountMaxSub);

			TaskMetricSubscription waitOutputCountMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			waitOutputCountMinSubs.put(vertexId, waitOutputCountMinSub);

			TaskMetricSubscription waitOutputSumMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			waitOutputSumMaxSubs.put(vertexId, waitOutputSumMaxSub);

			TaskMetricSubscription waitOutputSumMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			waitOutputSumMinSubs.put(vertexId, waitOutputSumMinSub);
		}
	}

	@Override
	public void close() {
		if (metricProvider == null) {
			return;
		}

		if (waitOutputCountMaxSubs != null) {
			for (TaskMetricSubscription sub : waitOutputCountMaxSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (waitOutputCountMinSubs != null) {
			for (TaskMetricSubscription sub : waitOutputCountMinSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (waitOutputSumMaxSubs != null) {
			for (TaskMetricSubscription sub : waitOutputSumMaxSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (waitOutputSumMinSubs != null) {
			for (TaskMetricSubscription sub : waitOutputSumMinSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		long now = System.currentTimeMillis();

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : waitOutputCountMaxSubs.keySet()) {
			TaskMetricSubscription waitOutputCountMaxSub = waitOutputCountMaxSubs.get(vertexId);
			TaskMetricSubscription waitOutputCountMinSub = waitOutputCountMinSubs.get(vertexId);
			TaskMetricSubscription waitOutputSumMaxSub = waitOutputSumMaxSubs.get(vertexId);
			TaskMetricSubscription waitOutputSumMinSub = waitOutputSumMinSubs.get(vertexId);

			if (waitOutputCountMaxSub.getValue() == null || now - waitOutputCountMaxSub.getValue().f0 > checkInterval * 2 ||
				waitOutputCountMinSub.getValue() == null || now - waitOutputCountMinSub.getValue().f0 > checkInterval * 2 ||
				waitOutputSumMaxSub.getValue() == null || now - waitOutputSumMaxSub.getValue().f0 > checkInterval * 2 ||
				waitOutputSumMinSub.getValue() == null || now - waitOutputSumMinSub.getValue().f0 > checkInterval * 2) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			double outputRecords = waitOutputCountMaxSub.getValue().f1 - waitOutputCountMinSub.getValue().f1;
			double totalWait = waitOutputSumMaxSub.getValue().f1 - waitOutputSumMinSub.getValue().f1 / 1.0e9;
			double waitOutputPerRecord = outputRecords == 0.0 ? 0.0 : totalWait / outputRecords;

			if (waitOutputPerRecord > threshold) {
				jobVertexIDs.add(vertexId);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			LOGGER.info("Back pressure detected for vertices {}.", jobVertexIDs);
			return new JobVertexBackPressure(jobID, jobVertexIDs);
		}
		return null;
	}
}
