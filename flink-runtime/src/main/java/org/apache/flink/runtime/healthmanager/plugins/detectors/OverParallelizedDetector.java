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

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_LATENCY_SUM;

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

	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionCountSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> inputCountSubs;

	@Override
	public void open(HealthMonitor monitor) {
		this.jobID = monitor.getJobID();
		this.monitor = monitor;
		this.metricProvider = monitor.getMetricProvider();

		ratio = monitor.getConfig().getDouble(OVER_PARALLELIZED_RATIO);
		checkInterval = monitor.getConfig().getLong(OVER_PARALLELIZED_CHECK_INTERVAL);

		sourcePartitionCountSubs = new HashMap<>();
		sourcePartitionLatencyCountRangeSubs = new HashMap<>();
		sourcePartitionLatencySumRangeSubs = new HashMap<>();
		latencyCountRangeSubs = new HashMap<>();
		latencySumRangeSubs = new HashMap<>();
		inputCountSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription latencyCountRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE);
			latencyCountRangeSubs.put(vertexId, latencyCountRangeSub);

			TaskMetricSubscription latencySumRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE);
			latencySumRangeSubs.put(vertexId, latencySumRangeSub);

			TaskMetricSubscription inputTpsSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_INPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			inputCountSubs.put(vertexId, inputTpsSub);

			// source latency
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()) {
				sourcePartitionCountSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_PARTITION_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
				sourcePartitionLatencyCountRangeSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_PARTITION_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE));
				sourcePartitionLatencySumRangeSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_PARTITION_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.RANGE));
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

		if (inputCountSubs != null) {
			for (TaskMetricSubscription sub : inputCountSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (sourcePartitionCountSubs != null) {
			for (TaskMetricSubscription sub : sourcePartitionCountSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (sourcePartitionLatencyCountRangeSubs != null) {
			for (TaskMetricSubscription sub : sourcePartitionLatencyCountRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (sourcePartitionLatencySumRangeSubs != null) {
			for (TaskMetricSubscription sub : sourcePartitionLatencySumRangeSubs.values()) {
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
			TaskMetricSubscription sourcePartitionCountSub = sourcePartitionCountSubs.get(vertexId);
			TaskMetricSubscription sourcePartitionLatencyCountRangeSub = sourcePartitionLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sourcePartitionLatencySumRangeSub = sourcePartitionLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription latencyCountRangeSub = latencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription latencySumRangeSub = latencySumRangeSubs.get(vertexId);
			TaskMetricSubscription inputCountSub = inputCountSubs.get(vertexId);

			if (latencyCountRangeSub.getValue() == null || now - latencyCountRangeSub.getValue().f0 > checkInterval * 2 ||
				latencySumRangeSub.getValue() == null || now - latencySumRangeSub.getValue().f0 > checkInterval * 2 ||
				inputCountSub.getValue() == null || now - inputCountSub.getValue().f0 > checkInterval * 2 ||
					inputCountSub.getValue().f1 < 0) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			boolean isParallelReader = false;
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()) {
				if (sourcePartitionCountSub.getValue() == null ||
					now - sourcePartitionCountSub.getValue().f0 > checkInterval * 2 ||
					sourcePartitionCountSub.getValue().f1 <= 0.0 ||
					sourcePartitionLatencyCountRangeSub.getValue() == null ||
					now - sourcePartitionLatencyCountRangeSub.getValue().f0 > checkInterval * 2 ||
					sourcePartitionLatencyCountRangeSub.getValue().f1 <= 0.0 ||
					sourcePartitionLatencySumRangeSub.getValue() == null ||
					now - sourcePartitionLatencySumRangeSub.getValue().f0 > checkInterval * 2 ||
					sourcePartitionLatencySumRangeSub.getValue().f1 <= 0.0) {
				} else {
					isParallelReader = true;
				}
				LOGGER.debug("Treat vertex {} as {} reader.", vertexId, isParallelReader ? "parallel" : "non-parallel");
				LOGGER.debug("source partition count " + sourcePartitionCountSub.getValue()
					+ " source partition latency count range " + sourcePartitionLatencyCountRangeSub.getValue()
					+ " source partition latency sum range " + sourcePartitionLatencySumRangeSub.getValue());
			}

			int parallelism = jobConfig.getVertexConfigs().get(vertexId).getParallelism();
			double taskLatencyCount = latencyCountRangeSub.getValue().f1;
			double taskLatencySum = latencySumRangeSub.getValue().f1;
			double taskLatency = taskLatencyCount <= 0.0 ? 0.0 : taskLatencySum / taskLatencyCount / 1.0e9;
			double inputTps = inputCountSub.getValue().f1 * 1000 / checkInterval;

			double workload;
			if (isParallelReader) {
				double partitionCount = sourcePartitionCountSub.getValue().f1;
				double partitionLatencyCount = sourcePartitionLatencyCountRangeSub.getValue().f1;
				double partitionLatencySum = sourcePartitionLatencySumRangeSub.getValue().f1;
				double partitionLatency = partitionLatencyCount <= 0.0 ? 0.0 : partitionLatencySum / partitionCount / 1.0e9;
				workload = partitionLatency <= 0.0 ? 0.0 : partitionCount * taskLatency / partitionLatency;
			} else {
				workload = taskLatency * inputTps;
			}

			LOGGER.debug("vertex {} input tps {} parallelism {} latency {} workload {}", vertexId, inputTps, parallelism, taskLatency, workload);

			if (parallelism > workload * ratio) {
				jobVertexIDs.add(vertexId);
				LOGGER.info("Over parallelized detected for vertex {}, tps:{} latency:{} workload:{}.", vertexId, inputTps, taskLatency, workload);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			return new JobVertexOverParallelized(jobID, jobVertexIDs);
		}
		return null;
	}
}
