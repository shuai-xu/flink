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
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OverParallelizedDetector detects over parallelized of a job.
 * Detects {@link JobVertexOverParallelized} if:
 *   current_parallelism > (input_tps * tps_ratio) * latency_per_record
 */
public class OverParallelizedDetector implements Detector {

	private static final String TASK_LATENCY_COUNT = "taskLatency.count";
	private static final String TASK_LATENCY_SUM = "taskLatency.sum";
	private static final String TASK_INPUT_TPS = "numRecordsInPerSecond.rate";
	private static final String SOURCE_INPUT_TPS = "parserTps.rate";

	private static final ConfigOption<Long> OVER_PARALLELIZED_CHECK_INTERVAL =
		ConfigOptions.key("healthmonitor.over-parallelized.interval.ms").defaultValue(5 * 60 * 1000L);
	private static final ConfigOption<Double> OVER_PARALLELIZED_RATIO =
		ConfigOptions.key("healthmonitor.over-parallelized.tps.ratio").defaultValue(1.5);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;

	private double ratio;
	private long checkInterval;

	private Map<JobVertexID, TaskMetricSubscription> latencyCountMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencyCountMinSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumMinSubs;
	private Map<JobVertexID, TaskMetricSubscription> inputTpsSubs;

	@Override
	public void open(HealthMonitor monitor) {
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		ratio = monitor.getConfig().getDouble(OVER_PARALLELIZED_RATIO);
		checkInterval = monitor.getConfig().getLong(OVER_PARALLELIZED_CHECK_INTERVAL);

		latencyCountMaxSubs = new HashMap<>();
		latencyCountMinSubs = new HashMap<>();
		latencySumMaxSubs = new HashMap<>();
		latencySumMinSubs = new HashMap<>();
		inputTpsSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = restServerClient.getJobConfig(jobID);
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription latencyCountMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			latencyCountMaxSubs.put(vertexId, latencyCountMaxSub);

			TaskMetricSubscription latencyCountMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			latencyCountMinSubs.put(vertexId, latencyCountMinSub);

			TaskMetricSubscription latencySumMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			latencySumMaxSubs.put(vertexId, latencySumMaxSub);

			TaskMetricSubscription latencySumMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			latencySumMinSubs.put(vertexId, latencySumMinSub);

			TaskMetricSubscription inputTpsSub;
			if (jobConfig.getInputNodes().get(vertexId).isEmpty()) {
				// source vertex
				inputTpsSub = metricProvider.subscribeTaskMetric(
					jobID, vertexId, SOURCE_INPUT_TPS, MetricAggType.SUM, checkInterval, TimelineAggType.AVG);
			} else {
				inputTpsSub = metricProvider.subscribeTaskMetric(
					jobID, vertexId, TASK_INPUT_TPS, MetricAggType.SUM, checkInterval, TimelineAggType.AVG);
			}
			inputTpsSubs.put(vertexId, inputTpsSub);
		}
	}

	@Override
	public void close() {
		if (metricProvider == null) {
			return;
		}

		if (latencyCountMaxSubs != null) {
			for (TaskMetricSubscription sub : latencyCountMaxSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (latencyCountMinSubs != null) {
			for (TaskMetricSubscription sub : latencyCountMinSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (latencySumMaxSubs != null) {
			for (TaskMetricSubscription sub : latencySumMaxSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (latencySumMinSubs != null) {
			for (TaskMetricSubscription sub : latencySumMinSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (inputTpsSubs != null) {
			for (TaskMetricSubscription sub : inputTpsSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}
	}

	@Override
	public Symptom detect() throws Exception {
		long now = System.currentTimeMillis();

		RestServerClient.JobConfig jobConfig = restServerClient.getJobConfig(jobID);
		if (jobConfig == null) {
			return null;
		}

		List<JobVertexID> jobVertexIDs = new ArrayList<>();
		for (JobVertexID vertexId : latencyCountMaxSubs.keySet()) {
			TaskMetricSubscription countMaxSub = latencyCountMaxSubs.get(vertexId);
			TaskMetricSubscription countMinSub = latencyCountMinSubs.get(vertexId);
			TaskMetricSubscription sumMaxSub = latencySumMaxSubs.get(vertexId);
			TaskMetricSubscription sumMinSub = latencySumMinSubs.get(vertexId);
			TaskMetricSubscription inputTpsSub = inputTpsSubs.get(vertexId);

			if (countMaxSub.getValue() == null || now - countMaxSub.getValue().f0 > checkInterval * 2 ||
				countMinSub.getValue() == null || now - countMinSub.getValue().f0 > checkInterval * 2 ||
				sumMaxSub.getValue() == null || now - sumMaxSub.getValue().f0 > checkInterval * 2 ||
				sumMinSub.getValue() == null || now - sumMaxSub.getValue().f0 > checkInterval * 2 ||
				inputTpsSub.getValue() == null || now - inputTpsSub.getValue().f0 > checkInterval * 2) {
				continue;
			}

			int parallelism = jobConfig.getVertexConfigs().get(vertexId).getParallelism();
			double inputRecordCount = countMaxSub.getValue().f1 - countMinSub.getValue().f1;
			double latencyPerRecord = inputRecordCount <= 0.0 ? 0.0 :
				(sumMaxSub.getValue().f1 - sumMinSub.getValue().f1) / 1.0e9 / inputRecordCount;
			double inputTps = inputTpsSub.getValue().f1;

			if (parallelism > inputTps * ratio * latencyPerRecord) {
				jobVertexIDs.add(vertexId);
			}
		}

		if (!jobVertexIDs.isEmpty()) {
			return new JobVertexOverParallelized(jobID, jobVertexIDs);
		}
		return null;
	}
}
