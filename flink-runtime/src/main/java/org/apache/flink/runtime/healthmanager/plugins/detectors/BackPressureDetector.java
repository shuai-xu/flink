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
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
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

	private static final ConfigOption<Double> BACK_PRESSURE_THRESHOLD =
		ConfigOptions.key("healthmonitor.back-pressure.threshold.ms").defaultValue(0.01);

	private JobID jobID;
	private MetricProvider metricProvider;
	private HealthMonitor healthMonitor;

	private long checkInterval;
	private double threshold;


	private Map<JobVertexID, TaskMetricSubscription> waitOutputCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputSumRangeSubs;

	@Override
	public void open(HealthMonitor monitor) {
		jobID = monitor.getJobID();
		healthMonitor = monitor;
		metricProvider = monitor.getMetricProvider();

		checkInterval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);
		threshold = monitor.getConfig().getDouble(BACK_PRESSURE_THRESHOLD);

		waitOutputCountRangeSubs = new HashMap<>();
		waitOutputSumRangeSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription waitOutputCountRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE);
			waitOutputCountRangeSubs.put(vertexId, waitOutputCountRangeSub);
			TaskMetricSubscription waitOutputSumRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE);
			waitOutputSumRangeSubs.put(vertexId, waitOutputSumRangeSub);
		}
	}

	@Override
	public void close() {
		if (metricProvider == null) {
			return;
		}

		if (waitOutputCountRangeSubs != null) {
			for (TaskMetricSubscription sub : waitOutputCountRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (waitOutputSumRangeSubs != null) {
			for (TaskMetricSubscription sub : waitOutputSumRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : waitOutputCountRangeSubs.keySet()) {
			TaskMetricSubscription waitOutputCountRangeSub = waitOutputCountRangeSubs.get(vertexId);
			TaskMetricSubscription waitOutputSumRangeSub = waitOutputSumRangeSubs.get(vertexId);

			if (!MetricUtils.validateTaskMetric(healthMonitor, checkInterval * 2, waitOutputCountRangeSub, waitOutputSumRangeSub)) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			double waitOutputCount = waitOutputCountRangeSub.getValue().f1;
			double waitOutputSum = waitOutputSumRangeSub.getValue().f1;
			double waitOutputPerRecord = waitOutputCount <= 0.0 ? 0.0 : waitOutputSum / waitOutputCount / 1.0e9;
			LOGGER.debug("vertex {} wait output {}", vertexId, waitOutputPerRecord);

			if (waitOutputPerRecord > threshold) {
				jobVertexIDs.add(vertexId);
				LOGGER.info("Back pressure detected for vertex {} waitOutput:{}.", vertexId, waitOutputPerRecord);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			return new JobVertexBackPressure(jobID, jobVertexIDs);
		}
		return null;
	}
}
