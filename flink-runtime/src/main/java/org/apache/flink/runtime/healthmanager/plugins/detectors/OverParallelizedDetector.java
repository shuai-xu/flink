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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOverParallelized;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_LATENCY_SUM;

/**
 * OverParallelizedDetector detects over parallelized of a job.
 * Detects {@link JobVertexOverParallelized} if:
 *   current_parallelism > (input_tps * tps_ratio) * latency_per_record
 */
public class OverParallelizedDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OverParallelizedDetector.class);

	private static final ConfigOption<Long> OVER_PARALLELIZED_CHECK_INTERVAL =
		ConfigOptions.key("healthmonitor.over-parallelized.interval.ms").defaultValue(60 * 1000L);
	private static final ConfigOption<Double> OVER_PARALLELIZED_RATIO =
		ConfigOptions.key("healthmonitor.over-parallelized.tps.ratio").defaultValue(1.5);

	private JobID jobID;
	private HealthMonitor monitor;
	private MetricProvider metricProvider;

	private double ratio;
	private long checkInterval;

	private Map<JobVertexID, TaskMetricSubscription> sourceLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> inputTpsSubs;

	@Override
	public void open(HealthMonitor monitor) {
		this.jobID = monitor.getJobID();
		this.monitor = monitor;
		this.metricProvider = monitor.getMetricProvider();

		ratio = monitor.getConfig().getDouble(OVER_PARALLELIZED_RATIO);
		checkInterval = monitor.getConfig().getLong(OVER_PARALLELIZED_CHECK_INTERVAL);

		sourceLatencyCountRangeSubs = new HashMap<>();
		sourceLatencySumRangeSubs = new HashMap<>();
		latencyCountRangeSubs = new HashMap<>();
		latencySumRangeSubs = new HashMap<>();
		inputTpsSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription latencyCountRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE);
			latencyCountRangeSubs.put(vertexId, latencyCountRangeSub);

			TaskMetricSubscription latencySumRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE);
			latencySumRangeSubs.put(vertexId, latencySumRangeSub);

			TaskMetricSubscription inputTpsSub;
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()
					&& jobConfig.getVertexConfigs().get(vertexId).getOperatorIds().size() == 1) {
				// source vertex
				inputTpsSub = metricProvider.subscribeTaskMetric(
					jobID, vertexId, MetricNames.TASK_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			} else {
				inputTpsSub = metricProvider.subscribeTaskMetric(
					jobID, vertexId, MetricNames.TASK_INPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			}
			inputTpsSubs.put(vertexId, inputTpsSub);

			// source latency
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()) {
				sourceLatencyCountRangeSubs.put(vertexId,
						metricProvider.subscribeTaskMetric(
								jobID, vertexId, SOURCE_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE));
				sourceLatencySumRangeSubs.put(vertexId,
						metricProvider.subscribeTaskMetric(
								jobID, vertexId, SOURCE_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE));
			}

		}
	}

	@Override
	public void close() {
		if (metricProvider == null) {
			return;
		}

		if (latencyCountRangeSubs != null) {
			for (TaskMetricSubscription sub : latencyCountRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (latencySumRangeSubs != null) {
			for (TaskMetricSubscription sub : latencySumRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (inputTpsSubs != null) {
			for (TaskMetricSubscription sub : inputTpsSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (sourceLatencyCountRangeSubs != null) {
			for (TaskMetricSubscription sub : sourceLatencyCountRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (sourceLatencySumRangeSubs != null) {
			for (TaskMetricSubscription sub : sourceLatencySumRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");
		long now = System.currentTimeMillis();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		if (jobConfig == null) {
			return null;
		}

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : latencyCountRangeSubs.keySet()) {
			TaskMetricSubscription sourceLatencyCountRangeSub = sourceLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sourceLatencySumRangeSub = sourceLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription countRangeSub = latencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sumRangeSub = latencySumRangeSubs.get(vertexId);
			TaskMetricSubscription inputTpsSub = inputTpsSubs.get(vertexId);

			if (countRangeSub.getValue() == null || now - countRangeSub.getValue().f0 > checkInterval * 2 ||
				sumRangeSub.getValue() == null || now - sumRangeSub.getValue().f0 > checkInterval * 2 ||
				inputTpsSub.getValue() == null || now - inputTpsSub.getValue().f0 > checkInterval * 2 ||
					inputTpsSub.getValue().f1 < 0) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			int parallelism = jobConfig.getVertexConfigs().get(vertexId).getParallelism();
			double inputRecordCount = countRangeSub.getValue().f1;
			double latencyPerRecord = inputRecordCount <= 0.0 ? 0.0 :
				sumRangeSub.getValue().f1 / 1.0e9 / inputRecordCount;
			double inputTps = inputTpsSub.getValue().f1;

			if (jobConfig.getInputNodes().get(vertexId).isEmpty() && sourceLatencyCountRangeSub.getValue().f1 > 0) {
				double sourceLatency =
						sourceLatencySumRangeSub.getValue().f1 / 1.0e9 /
								sourceLatencyCountRangeSub.getValue().f1;
				latencyPerRecord += sourceLatency;
			}

			LOGGER.debug("vertex {} input tps {} parallelism {} latency {}", vertexId, inputTps, parallelism, latencyPerRecord);

			if (parallelism > inputTps * ratio * latencyPerRecord) {
				jobVertexIDs.add(vertexId);
				LOGGER.info("Over parallelized detected for vertex {} tps:{} latency:{}.", vertexId, inputTps, latencyPerRecord);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			return new JobVertexOverParallelized(jobID, jobVertexIDs);
		}
		return null;
	}
}
