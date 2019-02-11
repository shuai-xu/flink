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
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOverParallelized;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions.MAX_PARTITION_PER_TASK;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_LATENCY_SUM;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PROCESS_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PROCESS_LATENCY_SUM;

/**
 * OverParallelizedDetector detects over parallelized of a job.
 * Detects {@link JobVertexOverParallelized} if:
 *   current_parallelism > (input_tps * tps_ratio) * latency_per_record
 */
public class OverParallelizedDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OverParallelizedDetector.class);

	private JobID jobID;
	private HealthMonitor monitor;
	private MetricProvider metricProvider;

	private double ratio;
	private long checkInterval;
	private int maxPartitionPerTask;

	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionCountSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceProcessLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceProcessLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> inputTpsSubs;

	@Override
	public void open(HealthMonitor monitor) {
		this.jobID = monitor.getJobID();
		this.monitor = monitor;
		this.metricProvider = monitor.getMetricProvider();

		ratio = monitor.getConfig().getDouble(HealthMonitorOptions.PARALLELISM_MAX_RATIO);
		checkInterval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);
		maxPartitionPerTask = monitor.getConfig().getInteger(MAX_PARTITION_PER_TASK);

		sourcePartitionCountSubs = new HashMap<>();
		sourcePartitionLatencyCountRangeSubs = new HashMap<>();
		sourcePartitionLatencySumRangeSubs = new HashMap<>();
		sourceProcessLatencyCountRangeSubs = new HashMap<>();
		sourceProcessLatencySumRangeSubs = new HashMap<>();
		latencyCountRangeSubs = new HashMap<>();
		latencySumRangeSubs = new HashMap<>();
		inputTpsSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription latencyCountRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST);
			latencyCountRangeSubs.put(vertexId, latencyCountRangeSub);

			TaskMetricSubscription latencySumRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST);
			latencySumRangeSubs.put(vertexId, latencySumRangeSub);

			TaskMetricSubscription inputTpsSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, MetricNames.TASK_INPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			inputTpsSubs.put(vertexId, inputTpsSub);

			// source latency
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()) {
				sourcePartitionCountSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_PARTITION_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
				sourcePartitionLatencyCountRangeSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_PARTITION_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
				sourcePartitionLatencySumRangeSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_PARTITION_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
				sourceProcessLatencyCountRangeSubs.put(vertexId,
						metricProvider.subscribeTaskMetric(
								jobID, vertexId, SOURCE_PROCESS_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
				sourceProcessLatencySumRangeSubs.put(vertexId,
						metricProvider.subscribeTaskMetric(
								jobID, vertexId, SOURCE_PROCESS_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
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
			TaskMetricSubscription sourceProcessLatencyCountRangeSub = sourceProcessLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sourceProcessLatencySumRangeSub = sourceProcessLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription latencyCountRangeSub = latencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription latencySumRangeSub = latencySumRangeSubs.get(vertexId);
			TaskMetricSubscription inputTpsSub = inputTpsSubs.get(vertexId);

			if (!MetricUtils.validateTaskMetric(monitor, checkInterval * 2,
					latencyCountRangeSub, latencySumRangeSub, inputTpsSub)) {
				LOGGER.debug("Skip vertex {}, metrics missing.", vertexId);
				continue;
			}

			boolean isParallelReader = false;
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()) {
				isParallelReader = MetricUtils.validateTaskMetric(monitor, checkInterval * 2,
						sourcePartitionCountSub,
						sourcePartitionLatencyCountRangeSub,
						sourcePartitionLatencySumRangeSub,
						sourceProcessLatencyCountRangeSub,
						sourceProcessLatencySumRangeSub) && sourcePartitionCountSub.getValue().f1 > 0;
				LOGGER.debug("Treat vertex {} as {} reader.", vertexId, isParallelReader ? "parallel" : "non-parallel");
				LOGGER.debug("source partition count " + sourcePartitionCountSub.getValue()
					+ " source partition latency count range " + sourcePartitionLatencyCountRangeSub.getValue()
					+ " source partition latency sum range " + sourcePartitionLatencySumRangeSub.getValue());
			}

			int parallelism = jobConfig.getVertexConfigs().get(vertexId).getParallelism();
			double taskLatencyCount = latencyCountRangeSub.getValue().f1;
			double taskLatencySum = latencySumRangeSub.getValue().f1;
			double taskLatency = taskLatencyCount <= 0.0 ? 0.0 : taskLatencySum / taskLatencyCount / 1.0e9;
			double inputTps = inputTpsSub.getValue().f1;

			double workload;
			double minParallelism = 1;
			if (isParallelReader) {
				// reset task latency.
				double processLatencyCount = sourceProcessLatencyCountRangeSub.getValue().f1;
				double processLatencySum = sourceProcessLatencySumRangeSub.getValue().f1;
				taskLatency = processLatencyCount <= 0.0 ? 0.0 : processLatencySum / processLatencyCount / 1.0e9;

				double partitionCount = sourcePartitionCountSub.getValue().f1;
				double partitionLatencyCount = sourcePartitionLatencyCountRangeSub.getValue().f1;
				double partitionLatencySum = sourcePartitionLatencySumRangeSub.getValue().f1;
				double partitionLatency = partitionLatencyCount <= 0.0 ? 0.0 : partitionLatencySum / partitionCount / 1.0e9;
				workload = partitionLatency <= 0.0 ? 0.0 : partitionCount * taskLatency / partitionLatency;

				minParallelism = Math.ceil(partitionCount / maxPartitionPerTask);
			} else {
				workload = taskLatency * inputTps;
			}

			LOGGER.debug("vertex {} input tps {} parallelism {} latency {} workload {}", vertexId, inputTps, parallelism, taskLatency, workload);

			if (parallelism > workload * ratio && parallelism > minParallelism) {
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
