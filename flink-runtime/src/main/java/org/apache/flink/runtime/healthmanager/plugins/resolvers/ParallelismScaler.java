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

package org.apache.flink.runtime.healthmanager.plugins.resolvers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.RescaleJobParallelism;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexBackPressure;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDelayIncreasing;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFailover;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighDelay;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowDelay;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOverParallelized;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.TASK_INPUT_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.TASK_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.TASK_LATENCY_SUM;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.TASK_OUTPUT_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.WAIT_OUTPUT_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.WAIT_OUTPUT_SUM;

/**
 * Parallelism scaler resolves job parallelism scaling.
 * workload = calculation_time_per_record * input_tps
 *          = (latency_per_record - wait_output_per_record * output_tps / input_tps) * input_tps
 * target_parallelism_v1 : target_parallelism_v2 = workload_v1 : workload_v2
 * target_parallelism = workload * tps_scale_ratio
 */
public class ParallelismScaler implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(ParallelismScaler.class);

	private static final ConfigOption<Double> PARALLELISM_UP_SCALE_TPS_RATIO_OPTION =
		ConfigOptions.key("parallelism.up-scale.tps.ratio").defaultValue(1.5);
	private static final ConfigOption<Double> PARALLELISM_DOWN_SCALE_TPS_RATIO_OPTION =
		ConfigOptions.key("parallelism.down-scale.tps.ratio").defaultValue(1.2);
	private static final ConfigOption<Long> PARALLELISM_SCALE_TIME_OUT_OPTION =
		ConfigOptions.key("parallelism.scale.timeout.ms").defaultValue(180000L);
	private static final ConfigOption<Long> PARALLELISM_SCALE_INTERVAL =
		ConfigOptions.key("parallelism.scale.interval.ms").defaultValue(60 * 1000L);

	private JobID jobID;
	private HealthMonitor monitor;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;

	private double upScaleTpsRatio;
	private double downScaleTpsRatio;
	private long timeout;
	private long checkInterval;

	private Map<JobVertexID, TaskMetricSubscription> inputTpsSubs;
	private Map<JobVertexID, TaskMetricSubscription> outputTpsSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencyCountMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencyCountMinSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> latencySumMinSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputCountMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputCountMinSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputSumMaxSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputSumMinSubs;

	private Map<JobVertexID, List<JobVertexID>> subDagRoot2SubDagVertex;
	private Map<JobVertexID, JobVertexID> vertex2SubDagRoot;
	private Map<JobVertexID, List<JobVertexID>> subDagRoot2UpstreamVertices;
	private Map<JobVertexID, Boolean> isSink;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.restServerClient = monitor.getRestServerClient();
		this.metricProvider = monitor.getMetricProvider();

		this.upScaleTpsRatio = monitor.getConfig().getDouble(PARALLELISM_UP_SCALE_TPS_RATIO_OPTION);
		this.downScaleTpsRatio = monitor.getConfig().getDouble(PARALLELISM_DOWN_SCALE_TPS_RATIO_OPTION);
		this.timeout = monitor.getConfig().getLong(PARALLELISM_SCALE_TIME_OUT_OPTION);
		this.checkInterval = monitor.getConfig().getLong(PARALLELISM_SCALE_INTERVAL);

		inputTpsSubs = new HashMap<>();
		outputTpsSubs = new HashMap<>();
		latencyCountMaxSubs = new HashMap<>();
		latencyCountMinSubs = new HashMap<>();
		latencySumMaxSubs = new HashMap<>();
		latencySumMinSubs = new HashMap<>();
		waitOutputCountMaxSubs = new HashMap<>();
		waitOutputCountMinSubs = new HashMap<>();
		waitOutputSumMaxSubs = new HashMap<>();
		waitOutputSumMinSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			// input tps
			TaskMetricSubscription inputTpsSub;
			if (jobConfig.getInputNodes().get(vertexId).isEmpty() &&
					jobConfig.getVertexConfigs().get(vertexId).getOperatorIds().size() == 1) {
				inputTpsSub = metricProvider.subscribeTaskMetric(
					jobID, vertexId, TASK_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			} else {
				inputTpsSub = metricProvider.subscribeTaskMetric(
					jobID, vertexId, TASK_INPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			}
			inputTpsSubs.put(vertexId, inputTpsSub);

			// output tps
			TaskMetricSubscription outputTps = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			outputTpsSubs.put(vertexId, outputTps);

			// latency count
			TaskMetricSubscription latencyCountMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			latencyCountMaxSubs.put(vertexId, latencyCountMaxSub);
			TaskMetricSubscription latencyCountMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			latencyCountMinSubs.put(vertexId, latencyCountMinSub);

			// latency sum
			TaskMetricSubscription latencySumMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			latencySumMaxSubs.put(vertexId, latencySumMaxSub);
			TaskMetricSubscription latencySumMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			latencySumMinSubs.put(vertexId, latencySumMinSub);

			// wait output count
			TaskMetricSubscription waitOutputCountMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			waitOutputCountMaxSubs.put(vertexId, waitOutputCountMaxSub);
			TaskMetricSubscription waitOutputCountMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			waitOutputCountMinSubs.put(vertexId, waitOutputCountMinSub);

			// wait output sum
			TaskMetricSubscription waitOutputSumMaxSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MAX);
			waitOutputSumMaxSubs.put(vertexId, waitOutputSumMaxSub);
			TaskMetricSubscription waitOutputSumMinSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.MIN);
			waitOutputSumMinSubs.put(vertexId, waitOutputSumMinSub);

			// analyze job graph.
			analyzeJobGraph(jobConfig);
		}
	}

	@Override
	public void close() {
		if (metricProvider == null) {
			return;
		}

		if (inputTpsSubs != null) {
			for (TaskMetricSubscription sub : inputTpsSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (outputTpsSubs != null) {
			for (TaskMetricSubscription sub : outputTpsSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
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
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		// symptoms for parallelism rescaling
		JobVertexFrequentFullGC jobVertexFrequentFullGC = null;
		JobVertexFailover jobVertexFailover = null;

		// symptoms for up scaling
		JobVertexHighDelay jobVertexHighDelay = null;
		JobVertexDelayIncreasing jobVertexDelayIncreasing = null;
		JobVertexBackPressure jobVertexBackPressure = null;

		// symptoms for down scaling
		JobVertexLowDelay jobVertexLowDelay = null;
		JobVertexOverParallelized jobVertexOverParallelized = null;

		for (Symptom symptom : symptomList) {

			if (symptom instanceof JobVertexFrequentFullGC) {
				jobVertexFrequentFullGC = (JobVertexFrequentFullGC) symptom;
				LOGGER.debug("Frequent full gc detected for vertices {}.", jobVertexFrequentFullGC.getJobVertexIDs());
			}

			if (symptom instanceof  JobVertexFailover) {
				jobVertexFailover = (JobVertexFailover) symptom;
				LOGGER.debug("Failover detected for vertices {}.", jobVertexFailover.getJobVertexIDs());
			}

			if (symptom instanceof JobVertexHighDelay) {
				jobVertexHighDelay = (JobVertexHighDelay) symptom;
				LOGGER.debug("High delay detected for vertices {}.", jobVertexHighDelay.getJobVertexIDs());
			}

			if (symptom instanceof JobVertexDelayIncreasing) {
				jobVertexDelayIncreasing = (JobVertexDelayIncreasing) symptom;
				LOGGER.debug("Delay increasing detected for vertices {}.", jobVertexDelayIncreasing.getJobVertexIDs());
			}

			if (symptom instanceof JobVertexBackPressure) {
				jobVertexBackPressure = (JobVertexBackPressure) symptom;
				LOGGER.debug("Back pressure detected for vertices {}.", jobVertexBackPressure.getJobVertexIDs());
			}

			if (symptom instanceof JobVertexLowDelay) {
				jobVertexLowDelay = (JobVertexLowDelay) symptom;
				LOGGER.debug("Low delay detected for vertices {}.", jobVertexLowDelay.getJobVertexIDs());
			}

			if (symptom instanceof JobVertexOverParallelized) {
				jobVertexOverParallelized = (JobVertexOverParallelized) symptom;
				LOGGER.debug("Over parallelized detected for vertices {}.", jobVertexOverParallelized.getJobVertexIDs());
			}
		}

		if (jobVertexFrequentFullGC != null || jobVertexFailover != null) {
			LOGGER.debug("Job is not stable, should not rescale parallelism.");
			return null;
		}

		boolean needUpScaleForDelay = jobVertexHighDelay != null && jobVertexDelayIncreasing != null;
		boolean needUpScaleForBackPressure = jobVertexBackPressure != null;
		boolean needDownScale = jobVertexBackPressure == null && jobVertexOverParallelized != null;
		if (!needUpScaleForDelay && !needUpScaleForBackPressure && !needDownScale) {
			LOGGER.debug("No need to rescale parallelism.");
			return null;
		}

		// calculate dag topology, performance and workload
		Map<JobVertexID, TaskPerformance> performances = getTaskPerformances();
		if (performances == null) {
			LOGGER.debug("Metric Not completed.");
			return null;
		}
		Map<JobVertexID, Double> workloads = calculateWorkloads(performances);

		// find sub dags to upscale
		Set<JobVertexID> subDagRootsToUpScale = new HashSet<>();
		if (needUpScaleForDelay) {
			Set<JobVertexID> verticesToUpScale = new HashSet<>(jobVertexHighDelay.getJobVertexIDs());
			verticesToUpScale.retainAll(jobVertexDelayIncreasing.getJobVertexIDs());
			for (JobVertexID vertexId : verticesToUpScale) {
				subDagRootsToUpScale.add(vertex2SubDagRoot.get(vertexId));
			}
		}
		if (needUpScaleForBackPressure) {
			Set<JobVertexID> verticesToUpScale = new HashSet<>(jobVertexBackPressure.getJobVertexIDs());
			for (JobVertexID vertexId : verticesToUpScale) {
				subDagRootsToUpScale.add(vertex2SubDagRoot.get(vertexId));
			}
		}
		LOGGER.debug("Roots of sub-dags need to scale up: {}.", subDagRootsToUpScale);

		// find sub dags to downscale
		Set<JobVertexID> subDagRootsToDownScale = new HashSet<>();
		if (needDownScale) {
			Set<JobVertexID> verticesToDownScale = new HashSet<>(jobVertexOverParallelized.getJobVertexIDs());
			verticesToDownScale.retainAll(jobVertexOverParallelized.getJobVertexIDs());
			for (JobVertexID vertexId : verticesToDownScale) {
				subDagRootsToDownScale.add(vertex2SubDagRoot.get(vertexId));
			}
		}
		subDagRootsToDownScale.removeAll(subDagRootsToUpScale);
		LOGGER.debug("Roots of sub-dags need to scale down: {}.", subDagRootsToDownScale);

		// for sub dags that need to rescale, set target scale ratio
		Map<JobVertexID, Double> subDagTargetTpsRatio = new HashMap<>();
		for (JobVertexID subDagRoot : subDagRoot2SubDagVertex.keySet()) {
			if (subDagRootsToUpScale.contains(subDagRoot)) {
				subDagTargetTpsRatio.put(subDagRoot, upScaleTpsRatio);
			} else if (subDagRootsToDownScale.contains(subDagRoot)){
				subDagTargetTpsRatio.put(subDagRoot, downScaleTpsRatio);
			}
		}
		LOGGER.debug("Target tps ratio for sub-dags before adjusting: {}.", subDagTargetTpsRatio);

		// scale up downstream sub dags according to upstream sub dags
		adjustTargetTpsRatioBetweenSubDags(subDagTargetTpsRatio, performances);
		LOGGER.debug("Target tps ratio for sub-dags after adjusting: {}.", subDagTargetTpsRatio);

		// set parallelisms
		Map<JobVertexID, Integer> targetParallelisms = scaleParallelismWithRatio(subDagTargetTpsRatio, workloads);
		LOGGER.debug("Target parallelism for vertices before applying constraints: {}.", targetParallelisms);

		// generate action
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		if (!targetParallelisms.isEmpty()) {
			updateTargetParallelismsSubjectToConstraints(targetParallelisms, jobConfig);
			LOGGER.debug("Target parallelism for vertices after applying constraints: {}.", targetParallelisms);
			RescaleJobParallelism rescaleJobParallelism = new RescaleJobParallelism(jobID, timeout);
			for (JobVertexID vertexId : targetParallelisms.keySet()) {
				RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexId);
				if (targetParallelisms.get(vertexId) == vertexConfig.getParallelism()) {
					continue;
				}
				rescaleJobParallelism.addVertex(
					vertexId, vertexConfig.getParallelism(), targetParallelisms.get(vertexId), vertexConfig.getResourceSpec(), vertexConfig.getResourceSpec());
			}
			if (!rescaleJobParallelism.isEmpty()) {
				LOGGER.info("RescaleJobParallelism action generated: {}.", rescaleJobParallelism);
				return rescaleJobParallelism;
			}
		}
		return null;
	}

	private void analyzeJobGraph(RestServerClient.JobConfig jobConfig) {
		subDagRoot2SubDagVertex = new HashMap<>();
		vertex2SubDagRoot = new HashMap<>();
		subDagRoot2UpstreamVertices = new HashMap<>();
		isSink = new HashMap<>();

		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			subDagRoot2SubDagVertex.put(vertexId, new ArrayList<>());
			subDagRoot2SubDagVertex.get(vertexId).add(vertexId);
			vertex2SubDagRoot.put(vertexId, vertexId);
			subDagRoot2UpstreamVertices.put(vertexId, new ArrayList<>());
			isSink.put(vertexId, true);
		}

		for (JobVertexID vertexId : jobConfig.getInputNodes().keySet()) {
			List<JobVertexID> upstreamVertices = jobConfig.getInputNodes().get(vertexId);
			if (upstreamVertices.isEmpty()) {
				// source
				continue;
			}

			if (upstreamVertices.size() == 1) {
				// one upstream vertex

				JobVertexID upstreamVertex = upstreamVertices.get(0);
				JobVertexID upstreamSubDagRoot = vertex2SubDagRoot.get(upstreamVertex);

				// add downstream sub dag vertices to upstream sub dag and remove downstream sub dag
				for (JobVertexID subDagVertex : subDagRoot2SubDagVertex.get(vertexId)) {
					subDagRoot2SubDagVertex.get(upstreamSubDagRoot).add(subDagVertex);
					vertex2SubDagRoot.put(subDagVertex, upstreamSubDagRoot);
				}
				subDagRoot2SubDagVertex.remove(vertexId);
				subDagRoot2UpstreamVertices.remove(vertexId);

				isSink.put(upstreamVertex, false);
			} else {
				// multiple downstream vertex

				for (JobVertexID upstreamVertex : upstreamVertices) {
					subDagRoot2UpstreamVertices.get(vertexId).add(upstreamVertex);

					isSink.put(upstreamVertex, false);
				}
			}
		}
	}

	private Map<JobVertexID, Double> calculateWorkloads(Map<JobVertexID, TaskPerformance> performances) {
		Map<JobVertexID, Double> workloads = new HashMap<>();
		for (JobVertexID vertexId : performances.keySet()) {
			TaskPerformance performance = performances.get(vertexId);
			double inputTps = performance.getInputTps();
			double outputTps = performance.getOutputTps();
			if (inputTps <= 0.0 || outputTps <= 0.0) {
				outputTps = 0.0;
				inputTps = 1.0;
			}
			double totalWaitOutput = performance.getWaitOutputPerRecord() * outputTps / inputTps;
			double workload = (performance.getLatencyPerRecord() - totalWaitOutput) * performance.getInputTps();
			workloads.put(vertexId, workload);
		}
		return workloads;
	}

	private void adjustTargetTpsRatioBetweenSubDags(Map<JobVertexID, Double> subDagTargetTpsRatio, Map<JobVertexID, TaskPerformance> performances){
		boolean hasDagScaleUp = true;
		while (hasDagScaleUp) {
			hasDagScaleUp = false;
			for (JobVertexID downStreamSubDagRoot : subDagRoot2UpstreamVertices.keySet()) {
				double downstreamInputTps = performances.get(downStreamSubDagRoot).getInputTps();
				double upstreamTargetOutputTps = 0.0;
				for (JobVertexID upstreamVertex : subDagRoot2UpstreamVertices.get(downStreamSubDagRoot)) {
					JobVertexID upstreamSubDagRoot = vertex2SubDagRoot.get(upstreamVertex);
					upstreamTargetOutputTps += performances.get(upstreamVertex).getOutputTps() * subDagTargetTpsRatio.getOrDefault(upstreamSubDagRoot, 1.0);
				}

				if (downstreamInputTps * subDagTargetTpsRatio.getOrDefault(downStreamSubDagRoot, 1.0) < upstreamTargetOutputTps) {
					if (downstreamInputTps == 0.0) {
						break;
					}
					double downstreamTargetTpsRatio = Math.min(upstreamTargetOutputTps / downstreamInputTps, upScaleTpsRatio);
					if (downstreamTargetTpsRatio > subDagTargetTpsRatio.getOrDefault(downStreamSubDagRoot, 1.0)) {
						hasDagScaleUp = true;
					}
					subDagTargetTpsRatio.put(downStreamSubDagRoot, downstreamTargetTpsRatio);
				}
			}
		}
	}

	private Map<JobVertexID, Integer> scaleParallelismWithRatio(
		Map<JobVertexID, Double> subDagTargetTpsRatio, Map<JobVertexID, Double> workloads) {

		Map<JobVertexID, Integer> targetParallelisms = new HashMap<>();
		for (JobVertexID subDagRoot : subDagTargetTpsRatio.keySet()) {
			double ratio = subDagTargetTpsRatio.get(subDagRoot);
			for (JobVertexID vertexId : subDagRoot2SubDagVertex.get(subDagRoot)) {
				targetParallelisms.put(vertexId, (int) Math.ceil(workloads.get(vertexId) * ratio));
			}
		}
		return targetParallelisms;
	}

	private void updateTargetParallelismsSubjectToConstraints(Map<JobVertexID, Integer> targetParallelisms, RestServerClient.JobConfig jobConfig) {
		Map<AbstractID, Integer> colocationGroupMaxParallelism = new HashMap<>();
		Map<AbstractID, Integer> colocationGroupMinMaxParallelism = new HashMap<>();

		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexId);

			// missing vertex
			if (!targetParallelisms.containsKey(vertexId)) {
				targetParallelisms.put(vertexId, vertexConfig.getParallelism());
			}

			// parallelism < max parallelism
			if (vertexConfig.getMaxParallelism() > 0 && targetParallelisms.get(vertexId) > vertexConfig.getMaxParallelism()) {
				LOGGER.info("Target parallelism exceed max parallelism, use max parallelism. vertexId: {}, targetParallelism: {}, maxParallelism: {}.",
					vertexId, targetParallelisms.get(vertexId), vertexConfig.getMaxParallelism());
				targetParallelisms.put(vertexId, vertexConfig.getMaxParallelism());
			}

			// parallelism > 0
			if (targetParallelisms.get(vertexId) <= 0) {
				LOGGER.warn("Target parallelism less than 1 (SHOULD NOT HAPPEN), use 1. vertexId: {}, targetParallelism: {}.",
					vertexId, targetParallelisms.get(vertexId));
				targetParallelisms.put(vertexId, 1);
			}

			// calculate parallelism and max parallelism for co-location groups
			AbstractID colocationGroupId = vertexConfig.getColocationGroupId();
			if (colocationGroupId != null) {
				if (colocationGroupMaxParallelism.containsKey(colocationGroupId)) {
					if (vertexConfig.getParallelism() > colocationGroupMaxParallelism.get(colocationGroupId)) {
						colocationGroupMaxParallelism.put(colocationGroupId, vertexConfig.getParallelism());
					}
				} else {
					colocationGroupMaxParallelism.put(colocationGroupId, vertexConfig.getParallelism());
				}

				if (vertexConfig.getMaxParallelism() > 0) {
					if (colocationGroupMinMaxParallelism.containsKey(colocationGroupId)) {
						if (vertexConfig.getMaxParallelism() < colocationGroupMinMaxParallelism.get(
							colocationGroupId)) {
							colocationGroupMinMaxParallelism.put(colocationGroupId, vertexConfig.getMaxParallelism());
						}
					} else {
						colocationGroupMinMaxParallelism.put(colocationGroupId, vertexConfig.getMaxParallelism());
					}
				}
			}
		}

		// set parallelism for co-location groups
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexId);
			AbstractID colocationGroupId = vertexConfig.getColocationGroupId();
			if (colocationGroupId != null) {
				int parallelism = colocationGroupMaxParallelism.get(colocationGroupId);
				if (colocationGroupMinMaxParallelism.containsKey(colocationGroupId)) {
					int maxParallelism = colocationGroupMinMaxParallelism.get(colocationGroupId);
					parallelism = Math.min(parallelism, maxParallelism);
				}
				LOGGER.info("Set vertex target parallelism to max target parallelism in colocation group. "
					+ "vertexId: {}, vertexTargetParallelism: {}, colocationGroupId: {}, colocationGroupMaxTargetParallelism: {}.",
					vertexId, targetParallelisms.get(vertexId), colocationGroupId, parallelism);
				targetParallelisms.put(vertexId, parallelism);
			}
		}
	}

	private Map<JobVertexID, TaskPerformance> getTaskPerformances() {
		long now = System.currentTimeMillis();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		if (jobConfig == null) {
			return null;
		}

		Map<JobVertexID, TaskPerformance> performances = new HashMap<>();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription inputTpsSub = inputTpsSubs.get(vertexId);
			TaskMetricSubscription outputTpsSub = outputTpsSubs.get(vertexId);
			TaskMetricSubscription latencyCountMaxSub = latencyCountMaxSubs.get(vertexId);
			TaskMetricSubscription latencyCountMinSub = latencyCountMinSubs.get(vertexId);
			TaskMetricSubscription latencySumMaxSub = latencySumMaxSubs.get(vertexId);
			TaskMetricSubscription latencySumMinSub = latencySumMinSubs.get(vertexId);
			TaskMetricSubscription waitOutputCountMaxSub = waitOutputCountMaxSubs.get(vertexId);
			TaskMetricSubscription waitOutputCountMinSub = waitOutputCountMinSubs.get(vertexId);
			TaskMetricSubscription waitOutputSumMaxSub = waitOutputSumMaxSubs.get(vertexId);
			TaskMetricSubscription waitOutputSumMinSub = waitOutputSumMinSubs.get(vertexId);

			boolean isSink = this.isSink.get(vertexId);

			// can not rescale without required metrics
			if (inputTpsSub.getValue() == null || now - inputTpsSub.getValue().f0 > checkInterval * 2 ||
				latencyCountMaxSub.getValue() == null || now - latencyCountMaxSub.getValue().f0 > checkInterval * 2 ||
				latencyCountMinSub.getValue() == null || now - latencyCountMinSub.getValue().f0 > checkInterval * 2 ||
				latencySumMaxSub.getValue() == null || now - latencySumMaxSub.getValue().f0 > checkInterval * 2 ||
				latencySumMinSub.getValue() == null || now - latencySumMinSub.getValue().f0 > checkInterval * 2) {
				return null;
			}
			if (!isSink &&
				(outputTpsSub.getValue() == null || now - outputTpsSub.getValue().f0 > checkInterval * 2 ||
				waitOutputCountMaxSub.getValue() == null || now - waitOutputCountMaxSub.getValue().f0 > checkInterval * 2 ||
				waitOutputCountMinSub.getValue() == null || now - waitOutputCountMinSub.getValue().f0 > checkInterval * 2 ||
				waitOutputSumMaxSub.getValue() == null || now - waitOutputSumMaxSub.getValue().f0 > checkInterval * 2 ||
				waitOutputSumMinSub.getValue() == null || now - waitOutputSumMinSub.getValue().f0 > checkInterval * 2)) {
				return null;
			}

			double inputRecords = latencyCountMaxSub.getValue().f1 - latencyCountMinSub.getValue().f1;
			double outputRecords = isSink ? 0.0 : waitOutputCountMaxSub.getValue().f1 - waitOutputCountMinSub.getValue().f1;
			double latency = inputRecords <= 0.0 ? 0.0 :
				(latencySumMaxSub.getValue().f1 - latencySumMinSub.getValue().f1) / 1.0e9 / inputRecords;
			double outputWait = outputRecords <= 0.0 ? 0.0 :
				(waitOutputSumMaxSub.getValue().f1 - waitOutputSumMinSub.getValue().f1) / 1.0e9 / outputRecords;
			TaskPerformance performance = new TaskPerformance(
				vertexId,
				inputTpsSub.getValue().f1,
				outputTpsSub.getValue().f1,
				inputRecords,
				outputRecords,
				latency,
				outputWait
			);

			performances.put(vertexId, performance);
		}
		return performances;
	}

	/**
	 * Task performance metrics.
	 */
	public static class TaskPerformance {
		private final JobVertexID jobVertexID;
		private final double inputTps;
		private final double outputTps;
		private final double inputRecords;
		private final double outputRecords;
		private final double latencyPerRecord;
		private final double waitOutputPerRecord;

		public TaskPerformance(
			JobVertexID jobVertexId,
			double inputTps,
			double outputTps,
			double inputRecords,
			double outputRecords,
			double latencyPerRecord,
			double waitOutputPerRecord) {

			this.jobVertexID = jobVertexId;
			this.inputTps = inputTps;
			this.outputTps = outputTps;
			this.inputRecords = inputRecords;
			this.outputRecords = outputRecords;
			this.latencyPerRecord = latencyPerRecord;
			this.waitOutputPerRecord = waitOutputPerRecord;
		}

		public JobVertexID getJobVertexID() {
			return jobVertexID;
		}

		public double getInputTps() {
			return inputTps;
		}

		public double getOutputTps() {
			return outputTps;
		}

		public double getInputRecords() {
			return inputRecords;
		}

		public double getOutputRecords() {
			return outputRecords;
		}

		public double getLatencyPerRecord() {
			return latencyPerRecord;
		}

		public double getWaitOutputPerRecord() {
			return waitOutputPerRecord;
		}
	}
}
