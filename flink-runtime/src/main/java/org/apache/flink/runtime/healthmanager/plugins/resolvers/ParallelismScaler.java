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
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ResourceManagerOptions;
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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStuck;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexBackPressure;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDelayIncreasing;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFailover;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighDelay;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOverParallelized;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MaxResourceLimitUtil;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions.MAX_PARTITION_PER_TASK;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_DELAY;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_LATENCY_SUM;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PARTITION_LATENCY_SUM;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PROCESS_LATENCY_COUNT;
import static org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames.SOURCE_PROCESS_LATENCY_SUM;
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

	private JobID jobID;
	private HealthMonitor monitor;
	private MetricProvider metricProvider;

	private double upScaleTpsRatio;
	private double downScaleTpsRatio;
	private long timeout;
	private long checkInterval;
	private int maxPartitionPerTask;
	private long stableTime;

	private double maxCpuLimit;
	private int maxMemoryLimit;

	// metric subscriptions

	private Map<JobVertexID, TaskMetricSubscription> inputTpsSubs;
	private Map<JobVertexID, TaskMetricSubscription> outputTpsSubs;
	private Map<JobVertexID, TaskMetricSubscription> taskLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> taskLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceProcessLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceProcessLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> waitOutputSumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionCountSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionLatencyCountRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourcePartitionLatencySumRangeSubs;
	private Map<JobVertexID, TaskMetricSubscription> sourceDelayRateSubs;

	// symptoms

	private JobVertexHighDelay highDelaySymptom;
	private JobVertexDelayIncreasing delayIncreasingSymptom;
	private JobVertexBackPressure backPressureSymptom;
	private JobVertexOverParallelized overParallelizedSymptom;
	private JobStable jobStableSymptom;
	private JobVertexFrequentFullGC frequentFullGCSymptom;
	private JobVertexFailover failoverSymptom;
	private JobStuck jobStuckSymptom;

	// diagnose

	private boolean needScaleUpForDelay;
	private boolean needScaleUpForBackpressure;
	private boolean needScaleDown;
	private Set<JobVertexID> vertexToDownScale = new HashSet<>();

	// topology

	private Map<JobVertexID, List<JobVertexID>> subDagRoot2SubDagVertex;
	private Map<JobVertexID, JobVertexID> vertex2SubDagRoot;
	private Map<JobVertexID, List<JobVertexID>> subDagRoot2UpstreamVertices;
	private Map<JobVertexID, Boolean> isSink;
	private Map<JobVertexID, Boolean> isSource;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.metricProvider = monitor.getMetricProvider();

		this.upScaleTpsRatio = monitor.getConfig().getDouble(HealthMonitorOptions.PARALLELISM_MAX_RATIO);
		this.downScaleTpsRatio = monitor.getConfig().getDouble(HealthMonitorOptions.PARALLELISM_MIN_RATIO);
		this.timeout = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_TIME_OUT);
		this.checkInterval = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL);
		this.maxCpuLimit = monitor.getConfig().getDouble(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_CPU_CORE);
		this.maxMemoryLimit = monitor.getConfig().getInteger(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_MEMORY_MB);
		this.maxPartitionPerTask = monitor.getConfig().getInteger(MAX_PARTITION_PER_TASK);
		this.stableTime = monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_STABLE_TIME);

		inputTpsSubs = new HashMap<>();
		outputTpsSubs = new HashMap<>();
		taskLatencyCountRangeSubs = new HashMap<>();
		taskLatencySumRangeSubs = new HashMap<>();
		sourceLatencyCountRangeSubs = new HashMap<>();
		sourceLatencySumRangeSubs = new HashMap<>();
		sourceProcessLatencyCountRangeSubs = new HashMap<>();
		sourceProcessLatencySumRangeSubs = new HashMap<>();
		waitOutputCountRangeSubs = new HashMap<>();
		waitOutputSumRangeSubs = new HashMap<>();
		sourcePartitionCountSubs = new HashMap<>();
		sourcePartitionLatencyCountRangeSubs = new HashMap<>();
		sourcePartitionLatencySumRangeSubs = new HashMap<>();
		sourceDelayRateSubs = new HashMap<>();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		// analyze job graph.
		analyzeJobGraph(jobConfig);

		// subscribe metrics.
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			// input tps
			TaskMetricSubscription inputTpsSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_INPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			inputTpsSubs.put(vertexId, inputTpsSub);

			// output tps
			TaskMetricSubscription outputTps = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.RATE);
			outputTpsSubs.put(vertexId, outputTps);

			// source latency
			if (isSource.get(vertexId)) {
				sourceLatencyCountRangeSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));
				sourceLatencySumRangeSubs.put(vertexId,
					metricProvider.subscribeTaskMetric(
						jobID, vertexId, SOURCE_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST));

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
				sourceDelayRateSubs.put(vertexId,
						metricProvider.subscribeTaskMetric(
								jobID, vertexId, SOURCE_DELAY, MetricAggType.AVG, checkInterval, TimelineAggType.RATE));
			}

			// task latency
			TaskMetricSubscription latencyCountRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST);
			taskLatencyCountRangeSubs.put(vertexId, latencyCountRangeSub);
			TaskMetricSubscription latencySumRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, TASK_LATENCY_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST);
			taskLatencySumRangeSubs.put(vertexId, latencySumRangeSub);

			// wait output
			TaskMetricSubscription waitOutputCountRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_COUNT, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST);
			waitOutputCountRangeSubs.put(vertexId, waitOutputCountRangeSub);
			TaskMetricSubscription waitOutputSumRangeSub = metricProvider.subscribeTaskMetric(
				jobID, vertexId, WAIT_OUTPUT_SUM, MetricAggType.SUM, checkInterval, TimelineAggType.LATEST);
			waitOutputSumRangeSubs.put(vertexId, waitOutputSumRangeSub);
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

		if (taskLatencyCountRangeSubs != null) {
			for (TaskMetricSubscription sub : taskLatencyCountRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
		}

		if (taskLatencySumRangeSubs != null) {
			for (TaskMetricSubscription sub : taskLatencySumRangeSubs.values()) {
				metricProvider.unsubscribe(sub);
			}
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
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		// Step-1. Diagnose

		parseSymptoms(symptomList);
		if (!diagnose()) {
			return null;
		}

		// Step-2. Cut SubDags

		analyzeJobGraph(monitor.getJobConfig());

		// Step-3. Prepare Metrics

		Map<JobVertexID, TaskMetrics> taskMetrics = prepareTaskMetrics();
		if (taskMetrics == null) {
			LOGGER.debug("Can not rescale, metrics are not completed.");
			return null;
		}

		// Step-4. calculate tpsRatio for sub dags

		Map<JobVertexID, Double> subDagTargetTpsRatio = getSubDagTargetTpsRatio(taskMetrics);

        // Step-5. set parallelisms

		Map<JobVertexID, Integer> targetParallelisms = getVertexTargetParallelisms(subDagTargetTpsRatio, taskMetrics);
		LOGGER.debug("Target parallelism for vertices before applying constraints: {}.", targetParallelisms);

		updateTargetParallelismsSubjectToConstraints(targetParallelisms, monitor.getJobConfig());
		LOGGER.debug("Target parallelism for vertices after applying constraints: {}.", targetParallelisms);

		// Step-6. generate parallelism rescale action

		RescaleJobParallelism rescaleJobParallelism = generateRescaleParallelismAction(targetParallelisms, monitor.getJobConfig());

		if (rescaleJobParallelism != null && !rescaleJobParallelism.isEmpty()) {
			LOGGER.info("RescaleJobParallelism action generated: {}.", rescaleJobParallelism);
			return rescaleJobParallelism;
		}

		return null;
	}

	private void parseSymptoms(List<Symptom> symptomList) {
		// clear old symptoms
		jobStableSymptom = null;
		frequentFullGCSymptom = null;
		failoverSymptom = null;
		jobStuckSymptom = null;
		highDelaySymptom = null;
		delayIncreasingSymptom = null;
		backPressureSymptom = null;
		overParallelizedSymptom = null;

		// read new symptoms
		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobStable) {
				jobStableSymptom = (JobStable) symptom;
			}

			if (symptom instanceof JobVertexFrequentFullGC) {
				frequentFullGCSymptom = (JobVertexFrequentFullGC) symptom;
				LOGGER.debug("Frequent full gc detected for vertices {}.", frequentFullGCSymptom.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobVertexFailover) {
				failoverSymptom = (JobVertexFailover) symptom;
				LOGGER.debug("Failover detected for vertices {}.", failoverSymptom.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobStuck) {
				jobStuckSymptom = (JobStuck) symptom;
				LOGGER.debug("Stuck detected for vertices {}.", jobStuckSymptom.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobVertexHighDelay) {
				highDelaySymptom = (JobVertexHighDelay) symptom;
				LOGGER.debug("High delay detected for vertices {}.", highDelaySymptom.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobVertexDelayIncreasing) {
				delayIncreasingSymptom = (JobVertexDelayIncreasing) symptom;
				LOGGER.debug("Delay increasing detected for vertices {}.", delayIncreasingSymptom.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobVertexBackPressure) {
				backPressureSymptom = (JobVertexBackPressure) symptom;
				LOGGER.debug("Back pressure detected for vertices {}.", backPressureSymptom.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobVertexOverParallelized) {
				overParallelizedSymptom = (JobVertexOverParallelized) symptom;
				LOGGER.debug("Over parallelized detected for vertices {}.", overParallelizedSymptom.getJobVertexIDs());
				continue;
			}
		}
	}

	private boolean diagnose() {
		if (jobStableSymptom == null ||
			jobStableSymptom.getStableTime() < stableTime ||
			frequentFullGCSymptom != null && frequentFullGCSymptom.isSevere() ||
			failoverSymptom != null) {

			LOGGER.debug("Job is not stable, should not rescale parallelism.");
			return false;
		}

		needScaleUpForDelay = highDelaySymptom != null && delayIncreasingSymptom != null &&
			CollectionUtils.intersection(highDelaySymptom.getJobVertexIDs(), delayIncreasingSymptom.getJobVertexIDs()).size() > 0;
		needScaleUpForBackpressure = backPressureSymptom != null;
		needScaleDown = overParallelizedSymptom != null && backPressureSymptom == null;

		if (!needScaleUpForDelay && !needScaleUpForBackpressure && !needScaleDown) {
			LOGGER.debug("No need to rescale parallelism.");
			return false;
		}

		return true;
	}

	private void analyzeJobGraph(RestServerClient.JobConfig jobConfig) {
		subDagRoot2SubDagVertex = new HashMap<>();
		vertex2SubDagRoot = new HashMap<>();
		subDagRoot2UpstreamVertices = new HashMap<>();
		isSink = new HashMap<>();
		isSource = new HashMap<>();

		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			subDagRoot2SubDagVertex.put(vertexId, new ArrayList<>());
			subDagRoot2SubDagVertex.get(vertexId).add(vertexId);
			vertex2SubDagRoot.put(vertexId, vertexId);
			subDagRoot2UpstreamVertices.put(vertexId, new ArrayList<>());
			isSink.put(vertexId, true);
			isSource.put(vertexId, false);
		}

		for (JobVertexID vertexId : jobConfig.getInputNodes().keySet()) {
			List<Tuple2<JobVertexID, String>> upstreamVertices = jobConfig.getInputNodes().get(vertexId);
			if (upstreamVertices.isEmpty()) {
				// source
				isSource.put(vertexId, true);
				continue;
			}

			if (upstreamVertices.size() == 1) {
				// one upstream vertex

				JobVertexID upstreamVertex = upstreamVertices.get(0).f0;
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

				for (Tuple2<JobVertexID, String> upstreamVertex : upstreamVertices) {
					subDagRoot2UpstreamVertices.get(vertexId).add(upstreamVertex.f0);

					isSink.put(upstreamVertex.f0, false);
				}
			}
		}
	}

	private Map<JobVertexID, TaskMetrics> prepareTaskMetrics() {
		long now = System.currentTimeMillis();

		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		if (jobConfig == null) {
			return null;
		}

		Map<JobVertexID, TaskMetrics> metrics = new HashMap<>();

		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			TaskMetricSubscription inputTpsSub = inputTpsSubs.get(vertexId);
			TaskMetricSubscription outputTpsSub = outputTpsSubs.get(vertexId);
			TaskMetricSubscription sourceLatencyCountRangeSub = sourceLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sourceLatencySumRangeSub = sourceLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription sourceProcessLatencyCountRangeSub = sourceProcessLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sourceProcessLatencySumRangeSub = sourceProcessLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription taskLatencyCountRangeSub = taskLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription taskLatencySumRangeSub = taskLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription waitOutputCountRangeSub = waitOutputCountRangeSubs.get(vertexId);
			TaskMetricSubscription waitOutputSumRangeSub = waitOutputSumRangeSubs.get(vertexId);
			TaskMetricSubscription sourcePartitionCountSub = sourcePartitionCountSubs.get(vertexId);
			TaskMetricSubscription sourcePartitionLatencyCountRangeSub = sourcePartitionLatencyCountRangeSubs.get(vertexId);
			TaskMetricSubscription sourcePartitionLatencySumRangeSub = sourcePartitionLatencySumRangeSubs.get(vertexId);
			TaskMetricSubscription sourceDelayIncreasingRateSub = sourceDelayRateSubs.get(vertexId);
			// check metrics complete

			if (inputTpsSub.getValue() == null || now - inputTpsSub.getValue().f0 > checkInterval * 2 ||
				taskLatencyCountRangeSub.getValue() == null || now - taskLatencyCountRangeSub.getValue().f0 > checkInterval * 2 ||
				taskLatencySumRangeSub.getValue() == null || now - taskLatencySumRangeSub.getValue().f0 > checkInterval * 2) {
				LOGGER.debug("input metric missing " + vertexId);
				LOGGER.debug("input tps " + inputTpsSub.getValue()
					+ " task latency count range " + taskLatencyCountRangeSub.getValue()
					+ " task latency sum range " + taskLatencySumRangeSub.getValue());
				return null;
			}

			if (!isSink.get(vertexId) &&
				!MetricUtils.validateTaskMetric(monitor, checkInterval * 2,
						outputTpsSub, waitOutputCountRangeSub, waitOutputSumRangeSub)) {
				LOGGER.debug("output metric missing " + vertexId);
				LOGGER.debug("output tps " + outputTpsSub.getValue()
					+ "wait output count range " + waitOutputCountRangeSub.getValue()
					+ "wait output sum range " + waitOutputSumRangeSub.getValue());
				return null;
			}

			if (isSource.get(vertexId) &&
					!MetricUtils.validateTaskMetric(monitor, checkInterval * 2,
							sourceLatencyCountRangeSub, sourceLatencySumRangeSub)) {
				LOGGER.debug("input metric missing for source " + vertexId);
				LOGGER.debug("source latency count range " + sourceLatencyCountRangeSub.getValue()
					+ "source latency sum range " + sourceLatencySumRangeSub.getValue());
				return null;
			}

			boolean isParallelReader = false;
			if (isSource.get(vertexId)) {
				isParallelReader = MetricUtils.validateTaskMetric(monitor, checkInterval * 2,
						sourcePartitionCountSub, sourcePartitionLatencyCountRangeSub, sourcePartitionLatencySumRangeSub) && sourcePartitionCountSub.getValue().f1 > 0;
				LOGGER.debug("Treat vertex {} as {} reader.", vertexId, isParallelReader ? "parallel" : "non-parallel");
				LOGGER.debug("source partition count " + sourcePartitionCountSub.getValue()
					+ " source partition latency count range " + sourcePartitionLatencyCountRangeSub.getValue()
					+ " source partition latency sum range " + sourcePartitionLatencySumRangeSub.getValue());
			}

			double inputTps = inputTpsSub.getValue().f1;
			double outputTps = outputTpsSub.getValue().f1;

			double taskLatencyCount = taskLatencyCountRangeSub.getValue().f1;
			double taskLatencySum = taskLatencySumRangeSub.getValue().f1;
			double taskLatency = taskLatencyCount <= 0.0 ? 0.0 : taskLatencySum / taskLatencyCount / 1.0e9;

			double sourceLatency = 0.0;
			if (isSource.get(vertexId)) {
				double sourceLatencyCount = sourceLatencyCountRangeSub.getValue().f1;
				double sourceLatencySum = sourceLatencySumRangeSub.getValue().f1;
				sourceLatency = sourceLatencyCount <= 0.0 ? 0.0 : sourceLatencySum / sourceLatencyCount / 1.0e9;
			}

			double waitOutput = 0.0;
			if (!isSink.get(vertexId)) {
				double waitOutputCount = waitOutputCountRangeSub.getValue().f1;
				double waitOutputSum = waitOutputSumRangeSub.getValue().f1;
				waitOutput = waitOutputCount <= 0.0 ? 0.0 : waitOutputSum / waitOutputCount / 1.0e9;
			}
			double waitOutputPerInputRecord = inputTps <= 0.0 ? 0.0 : waitOutput * outputTps / inputTps;

			double workload;
			if (isParallelReader) {
				// reset task latency.
				double processLatencyCount = sourceProcessLatencyCountRangeSub.getValue().f1;
				double processLatencySum = sourceProcessLatencySumRangeSub.getValue().f1;
				taskLatency = processLatencyCount <= 0.0 ? 0.0 : processLatencySum / processLatencyCount / 1.0e9;

				double partitionCount = sourcePartitionCountSub.getValue().f1;
				double partitionLatencyCount = sourcePartitionLatencyCountRangeSub.getValue().f1;
				double partitionLatencySum = sourcePartitionLatencySumRangeSub.getValue().f1;
				double partitionLatency = partitionLatencyCount <= 0.0 ? 0.0 : partitionLatencySum / partitionCount / 1.0e9;
				workload = partitionLatency <= 0.0 ? 0.0 : partitionCount * (taskLatency - waitOutputPerInputRecord) / partitionLatency;
			} else {
				workload = (taskLatency - waitOutputPerInputRecord) * inputTps;
			}

			double delayIncreasingRate = 0;
			if (sourceDelayIncreasingRateSub != null) {
				delayIncreasingRate = sourceDelayIncreasingRateSub.getValue().f1 / 1.0e3;
			}

			TaskMetrics taskMetrics = new TaskMetrics(
				vertexId,
				isParallelReader,
				inputTps,
				outputTps,
				taskLatency,
				sourceLatency,
				waitOutputPerInputRecord,
				workload,
				delayIncreasingRate
			);

			LOGGER.debug("Metrics for vertex {}.", taskMetrics.toString());

			metrics.put(vertexId, taskMetrics);
		}
		return metrics;
	}

	private Map<JobVertexID, Double> getSubDagTargetTpsRatio(
			Map<JobVertexID, TaskMetrics> taskMetrics) {
		Map<JobVertexID, Double> subDagTargetTpsRatio = new HashMap<>();

		// find sub dags to upscale

		Set<JobVertexID> subDagRootsToUpScale = new HashSet<>();

		Set<JobVertexID> backPressureVertices = new HashSet<>();
		if (needScaleUpForBackpressure) {
			backPressureVertices.addAll(backPressureSymptom.getJobVertexIDs());
			for (JobVertexID vertexID : backPressureVertices) {
				subDagRootsToUpScale.add(vertex2SubDagRoot.get(vertexID));
				subDagTargetTpsRatio.put(vertex2SubDagRoot.get(vertexID), upScaleTpsRatio);
			}
		}

		if (needScaleUpForDelay) {
			Collection<JobVertexID> verticesToUpScale = CollectionUtils.intersection(
				highDelaySymptom.getJobVertexIDs(), delayIncreasingSymptom.getJobVertexIDs());
			for (JobVertexID vertexId : verticesToUpScale) {
				subDagRootsToUpScale.add(vertex2SubDagRoot.get(vertexId));
				double ratio = 1 / (1 - taskMetrics.get(vertexId).delayIncreasingRate);
				if (ratio < upScaleTpsRatio) {
					ratio = upScaleTpsRatio;
				}
				subDagTargetTpsRatio.put(vertex2SubDagRoot.get(vertexId), ratio);
			}
		}

		LOGGER.debug("Roots of sub-dags need to scale up: {}.", subDagRootsToUpScale);

		// find sub dags to downscale

		vertexToDownScale.clear();
		if (needScaleDown) {
			Set<JobVertexID> verticesToDownScale = new HashSet<>(overParallelizedSymptom.getJobVertexIDs());
			for (JobVertexID vertexId : verticesToDownScale) {
				vertexToDownScale.add(vertexId);
			}
		}

		vertexToDownScale.removeAll(subDagRootsToUpScale);

		LOGGER.debug("Roots of sub-dags need to scale down: {}.", vertexToDownScale);

		// for sub dags that need to rescale, set target scale ratio
		LOGGER.debug("Target scale up tps ratio for sub-dags before adjusting: {}.", subDagTargetTpsRatio);

		// scale up downstream sub dags according to upstream sub dags

		boolean hasDagScaleUp = true;
		while (hasDagScaleUp) {
			hasDagScaleUp = false;
			for (JobVertexID downStreamSubDagRoot : subDagRoot2UpstreamVertices.keySet()) {
				for (JobVertexID upStreamVertex : subDagRoot2UpstreamVertices.get(downStreamSubDagRoot)) {
					JobVertexID upStreamSubDagRoot = vertex2SubDagRoot.get(upStreamVertex);

					if (!subDagTargetTpsRatio.containsKey(upStreamSubDagRoot)) {
						continue;
					}

					if (!subDagTargetTpsRatio.containsKey(downStreamSubDagRoot) ||
						subDagTargetTpsRatio.get(downStreamSubDagRoot) < subDagTargetTpsRatio.get(upStreamSubDagRoot)) {

						subDagTargetTpsRatio.put(downStreamSubDagRoot, subDagTargetTpsRatio.get(upStreamSubDagRoot));
						hasDagScaleUp = true;
					}
				}
			}
		}

		LOGGER.debug("Target scale up tps ratio for sub-dags after adjusting: {}.", subDagTargetTpsRatio);

		return subDagTargetTpsRatio;
	}

	private Map<JobVertexID, Integer> getVertexTargetParallelisms(
		Map<JobVertexID, Double> subDagTargetTpsRatio, Map<JobVertexID, TaskMetrics> taskMetrics) {

		Map<JobVertexID, Integer> targetParallelisms = new HashMap<>();
		for (JobVertexID subDagRoot : subDagTargetTpsRatio.keySet()) {
			double ratio = subDagTargetTpsRatio.get(subDagRoot);
			for (JobVertexID vertexId : subDagRoot2SubDagVertex.get(subDagRoot)) {
				if (taskMetrics.get(vertexId).getWorkload() > 0) {
					if (taskMetrics.get(vertexId).isParallelSource) {
						targetParallelisms.put(vertexId, (int) Math.ceil(taskMetrics.get(vertexId).getWorkload()));
					} else {
						targetParallelisms.put(vertexId, (int) Math.ceil(taskMetrics.get(vertexId).getWorkload() * ratio));
					}
				}
			}
		}

		for (JobVertexID vertexID : vertexToDownScale) {
			if (!targetParallelisms.containsKey(vertexID)) {
				if (taskMetrics.get(vertexID).getWorkload() > 0) {
					if (taskMetrics.get(vertexID).isParallelSource) {
						targetParallelisms.put(vertexID, (int) Math.ceil(taskMetrics.get(vertexID).getWorkload()));
					} else {
						targetParallelisms.put(vertexID, (int) Math.ceil(taskMetrics.get(vertexID).getWorkload() * downScaleTpsRatio));
					}
				}
			}
		}
		return targetParallelisms;
	}

	private void updateTargetParallelismsSubjectToConstraints(Map<JobVertexID, Integer> targetParallelisms, RestServerClient.JobConfig jobConfig) {

		// EqualParallelismGroups (EPG)

		// Group vertices that must have equal parallelism.
		// A group is identified by its Leader, which could be any vertex in the group.
		// All vertices in the group are Members, including the Leader.
		// The target parallelism of a group should be the max value among all the Members' target parallelisms.
		// Members of a group shares the same max parallelism, which is the min value among all the Members' max parallelisms.

		Map<JobVertexID, Set<JobVertexID>> epgLeader2Members = new HashMap<>();
		Map<JobVertexID, JobVertexID> epgMember2Leader = new HashMap<>();
		Map<JobVertexID, Integer> epgLeader2TargetParallelism = new HashMap<>();
		Map<JobVertexID, Integer> epgLeader2MaxParallelism = new HashMap<>();

		// Initially, each vertex belongs to a separate group.

		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexId);

			int targetParallelism = vertexConfig.getParallelism();
			int maxParallelism = vertexConfig.getMaxParallelism();

			// limit thread of parallel reader.
			if (targetParallelisms.containsKey(vertexId)) {
				targetParallelism = targetParallelisms.get(vertexId);
				if (isSource.get(vertexId) && sourcePartitionCountSubs.get(vertexId).getValue() != null) {
					double partitionCount = sourcePartitionCountSubs.get(vertexId).getValue().f1;
					if (partitionCount / targetParallelism > maxPartitionPerTask) {
						targetParallelism = (int) Math.ceil(partitionCount / maxPartitionPerTask);
					}
					if (partitionCount > 0 && maxParallelism > partitionCount) {
						maxParallelism = (int) partitionCount;
					}
				}

			}

			// parallelism > 0
			if (targetParallelism < 1) {
				targetParallelism = 1;
			}

			// parallelism <= max
			if (targetParallelism > maxParallelism && maxParallelism > 0) {
				targetParallelism = maxParallelism;
			}

			HashSet<JobVertexID> members = new HashSet<>();
			members.add(vertexId);

			epgLeader2Members.put(vertexId, members);
			epgMember2Leader.put(vertexId, vertexId);
			epgLeader2TargetParallelism.put(vertexId, targetParallelism);
			epgLeader2MaxParallelism.put(vertexId, maxParallelism);
		}

		// merge groups according to co-location groups

		Map<AbstractID, JobVertexID> colocationGroupId2Leader = new HashMap<>();
		for (JobVertexID vertexId : jobConfig.getVertexConfigs().keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexId);
			AbstractID colocationGroupId = vertexConfig.getColocationGroupId();

			if (colocationGroupId == null) {
				continue;
			}

			if (colocationGroupId2Leader.containsKey(colocationGroupId)) {
				JobVertexID currentGroupLeader = epgMember2Leader.get(vertexId);
				JobVertexID targetGroupLeader = colocationGroupId2Leader.get(colocationGroupId);

				mergeEqualParallelismGroups(
					currentGroupLeader,
					targetGroupLeader,
					epgLeader2Members,
					epgMember2Leader,
					epgLeader2TargetParallelism,
					epgLeader2MaxParallelism);
			} else {
				colocationGroupId2Leader.put(colocationGroupId, epgMember2Leader.get(vertexId));
			}
		}

		// merge groups according to forward streams

		for (JobVertexID downStreamVertex : jobConfig.getInputNodes().keySet()) {
			for (Tuple2<JobVertexID, String> edge : jobConfig.getInputNodes().get(downStreamVertex)) {
				JobVertexID upStreamVertex = edge.f0;
				String shipStrategy = edge.f1;
				if (shipStrategy.equals("FORWARD")) {
					JobVertexID currentGroupLeader = epgMember2Leader.get(upStreamVertex);
					JobVertexID targetGroupLeader = epgMember2Leader.get(downStreamVertex);

					mergeEqualParallelismGroups(
						currentGroupLeader,
						targetGroupLeader,
						epgLeader2Members,
						epgMember2Leader,
						epgLeader2TargetParallelism,
						epgLeader2MaxParallelism);
				}
			}
		}

		// ProportionalParallelismGroups (PPG)

		// Group EqualParallelismGroups whose parallelism must be proportional to each other.
		// A group is identified by its Leader, which could be any EPG in the group.
		// All EPGs in the group are Members, including the Leader.
		// Each PPG has a base, and each EPG in the PPG has a factor.
		// The parallelism of the member EPG is base * factor.

		Map<JobVertexID, Set<JobVertexID>> ppgLeader2Members = new HashMap<>();
		Map<JobVertexID, JobVertexID> ppgMember2Leader = new HashMap<>();
		Map<JobVertexID, Integer> ppgLeader2Base = new HashMap<>();
		Map<JobVertexID, Integer> ppgMember2Factor = new HashMap<>();

		// merge groups according to rescale streams

		for (JobVertexID downStreamVertex : jobConfig.getInputNodes().keySet()) {
			for (Tuple2<JobVertexID, String> edge : jobConfig.getInputNodes().get(downStreamVertex)) {
				JobVertexID upStreamVertex = edge.f0;
				String shipStrategy = edge.f1;
				if (shipStrategy.equals("RESCALE")) {
					JobVertexID upStreamEpg = epgMember2Leader.get(upStreamVertex);
					if (!ppgMember2Leader.containsKey(upStreamEpg)) {
						HashSet<JobVertexID> members = new HashSet<>();
						members.add(upStreamEpg);
						ppgLeader2Members.put(upStreamEpg, members);
						ppgMember2Leader.put(upStreamEpg, upStreamEpg);
						ppgLeader2Base.put(upStreamEpg, epgLeader2TargetParallelism.get(upStreamEpg));
						ppgMember2Factor.put(upStreamEpg, 1);
					}

					JobVertexID downStreamEpg = epgMember2Leader.get(downStreamVertex);
					if (!ppgMember2Leader.containsKey(downStreamEpg)) {
						HashSet<JobVertexID> members = new HashSet<>();
						members.add(downStreamEpg);
						ppgLeader2Members.put(downStreamEpg, members);
						ppgMember2Leader.put(downStreamEpg, downStreamEpg);
						ppgLeader2Base.put(downStreamEpg, epgLeader2TargetParallelism.get(downStreamEpg));
						ppgMember2Factor.put(downStreamEpg, 1);
					}

					mergeProportionalParallelismGroups(
						upStreamEpg,
						downStreamEpg,
						epgLeader2Members,
						epgLeader2TargetParallelism,
						epgLeader2MaxParallelism,
						ppgLeader2Members,
						ppgMember2Leader,
						ppgLeader2Base,
						ppgMember2Factor,
						jobConfig);
				}
			}
		}

		// update target parallelisms

		targetParallelisms.clear();
		for (JobVertexID epgLeader : epgLeader2Members.keySet()) {
			int targetParallelism = epgLeader2TargetParallelism.get(epgLeader);
			for (JobVertexID vertexId : epgLeader2Members.get(epgLeader)) {
				targetParallelisms.put(vertexId, targetParallelism);
			}
		}
	}

	private void mergeEqualParallelismGroups(
		JobVertexID currentGroupLeader,
		JobVertexID targetGroupLeader,
		Map<JobVertexID, Set<JobVertexID>> leader2Members,
		Map<JobVertexID, JobVertexID> member2Leader,
		Map<JobVertexID, Integer> leader2TargetParallelism,
		Map<JobVertexID, Integer> leader2MaxParallelism) {
		if (currentGroupLeader.equals(targetGroupLeader)) {
			return;
		}

		int currentGroupTargetParallelism = leader2TargetParallelism.get(currentGroupLeader);
		int currentGroupMaxParallelism = leader2MaxParallelism.get(currentGroupLeader);
		int targetGroupTargetParallelism = leader2TargetParallelism.get(targetGroupLeader);
		int targetGroupMaxParallelism = leader2MaxParallelism.get(targetGroupLeader);

		int targetParallelism = Math.max(currentGroupTargetParallelism, targetGroupTargetParallelism);
		int maxParallelism = Math.min(currentGroupMaxParallelism, targetGroupMaxParallelism);
		if (targetParallelism > maxParallelism) {
			targetParallelism = maxParallelism;
		}

		leader2Members.get(targetGroupLeader).addAll(leader2Members.get(currentGroupLeader));
		leader2Members.get(currentGroupLeader).forEach(member -> member2Leader.put(member, targetGroupLeader));
		leader2TargetParallelism.put(targetGroupLeader, targetParallelism);
		leader2MaxParallelism.put(targetGroupLeader, maxParallelism);

		leader2Members.remove(currentGroupLeader);
		leader2TargetParallelism.remove(currentGroupLeader);
		leader2MaxParallelism.remove(currentGroupLeader);
	}

	private void mergeProportionalParallelismGroups(
		JobVertexID upStreamEpg,
		JobVertexID downStreamEpg,
		Map<JobVertexID, Set<JobVertexID>> epgLeader2Members,
		Map<JobVertexID, Integer> epgLeader2TargetParallelism,
		Map<JobVertexID, Integer> epgLeader2MaxParallelism,
		Map<JobVertexID, Set<JobVertexID>> ppgLeader2Members,
		Map<JobVertexID, JobVertexID> ppgMember2Leader,
		Map<JobVertexID, Integer> ppgLeader2Base,
		Map<JobVertexID, Integer> ppgMember2Factor,
		RestServerClient.JobConfig jobConfig) {

		JobVertexID upStreamPpgLeader = ppgMember2Leader.get(upStreamEpg);
		JobVertexID downStreamPpgLeader = ppgMember2Leader.get(downStreamEpg);

		int upStreamBase = ppgLeader2Base.get(upStreamPpgLeader);
		int upStreamFactor = ppgMember2Factor.get(upStreamEpg);
		int downStreamBase = ppgLeader2Base.get(downStreamPpgLeader);
		int downStreamFactor = ppgMember2Factor.get(downStreamEpg);

		// up stream greater plan
		// (upStreamBase + upStreamBaseIncrease) * upStreamFactor = k * (downStreamBase + downStreamBaseIncrease) * downStreamFactor

		int upStreamGreaterUpStreamBaseIncrease = 0;
		int upStreamGreaterDownStreamBaseIncrease;
		int upStreamGreaterK;
		while (true) {
			int upStreamParallelism = (upStreamBase + upStreamGreaterUpStreamBaseIncrease) * upStreamFactor;
			if (upStreamParallelism % downStreamFactor == 0 &&
				upStreamParallelism / downStreamFactor >= downStreamBase) {
				upStreamGreaterK = upStreamParallelism / downStreamFactor / downStreamBase;
				upStreamGreaterDownStreamBaseIncrease = upStreamParallelism / downStreamFactor / upStreamGreaterK - downStreamBase;
				break;
			}
			upStreamGreaterUpStreamBaseIncrease++;
		}

		boolean upStreamGreaterFeasible = true;
		for (JobVertexID upStreamPpgMemberEpg : ppgLeader2Members.get(upStreamPpgLeader)) {
			int upStreamPpgMemberParallelism = (upStreamBase + upStreamGreaterUpStreamBaseIncrease) * ppgMember2Factor.get(upStreamPpgMemberEpg);
			if (upStreamPpgMemberParallelism > epgLeader2MaxParallelism.get(upStreamPpgMemberEpg)) {
				upStreamGreaterFeasible = false;
				break;
			}
		}
		for (JobVertexID downStreamPpgMemberEpg : ppgLeader2Members.get(downStreamPpgLeader)) {
			int downStreamPpgMemberParallelism = (downStreamBase + upStreamGreaterDownStreamBaseIncrease) * ppgMember2Factor.get(downStreamPpgMemberEpg);
			if (downStreamPpgMemberParallelism > epgLeader2MaxParallelism.get(downStreamPpgMemberEpg)) {
				upStreamGreaterFeasible = false;
				break;
			}
		}

		// down stream greater plan
		// k * (upStreamBase + upStreamBaseIncrease) * upStreamFactor = (downStreamBase + downStreamBaseIncrease) * downStreamFactor

		int downStreamGreaterDownStreamBaseIncrease = 0;
		int downStreamGreaterUpStreamBaseIncrease;
		int downStreamGreaterK;
		while (true) {
			int downStreamParallelism = (downStreamBase + downStreamGreaterDownStreamBaseIncrease) * downStreamFactor;
			if (downStreamParallelism % upStreamFactor == 0 &&
				downStreamParallelism / upStreamFactor >= upStreamBase) {
				downStreamGreaterK = downStreamParallelism / upStreamFactor / upStreamBase;
				downStreamGreaterUpStreamBaseIncrease = downStreamParallelism / upStreamFactor / downStreamGreaterK - upStreamBase;
				break;
			}
			downStreamGreaterDownStreamBaseIncrease++;
		}

		boolean downStreamGreaterFeasible = true;
		for (JobVertexID downStreamPpgMemberEpg : ppgLeader2Members.get(downStreamPpgLeader)) {
			int downStreamPpgMemberParallelism = (downStreamBase + downStreamGreaterDownStreamBaseIncrease) * ppgMember2Factor.get(downStreamPpgMemberEpg);
			if (downStreamPpgMemberParallelism > epgLeader2MaxParallelism.get(downStreamPpgMemberEpg)) {
				downStreamGreaterFeasible = false;
				break;
			}
		}
		for (JobVertexID upStreamPpgMemberEpg : ppgLeader2Members.get(upStreamPpgLeader)) {
			int upStreamPpgMemberParallelism = (upStreamBase + downStreamGreaterUpStreamBaseIncrease) * ppgMember2Factor.get(upStreamPpgMemberEpg);
			if (upStreamPpgMemberParallelism > epgLeader2MaxParallelism.get(upStreamPpgMemberEpg)) {
				downStreamGreaterFeasible = false;
				break;
			}
		}

		// choose plan

		boolean useUpStreamGreaterPlan;
		if (upStreamGreaterFeasible && downStreamGreaterFeasible) {

			// calculate cost

			ResourceSpec upStreamBaseIncreaseCost = new ResourceSpec.Builder().build();
			for (JobVertexID upStreamPpgMemberEpg : ppgLeader2Members.get(upStreamPpgLeader)) {
				ResourceSpec memberEpgIncreaseCost = new ResourceSpec.Builder().build();
				for (JobVertexID upStreamPpgMemberVertex : epgLeader2Members.get(upStreamPpgMemberEpg)) {
					memberEpgIncreaseCost = memberEpgIncreaseCost.merge(jobConfig.getVertexConfigs().get(upStreamPpgMemberVertex).getResourceSpec());
				}
				for (int i = 0; i < ppgMember2Factor.get(upStreamPpgMemberEpg); ++i) {
					upStreamBaseIncreaseCost = upStreamBaseIncreaseCost.merge(memberEpgIncreaseCost);
				}
			}

			ResourceSpec downStreamBaseIncreaseCost = new ResourceSpec.Builder().build();
			for (JobVertexID downStreamPpgMemberEpg : ppgLeader2Members.get(downStreamPpgLeader)) {
				ResourceSpec memberEpgIncreaseCost = new ResourceSpec.Builder().build();
				for (JobVertexID downStreamPpgMemberVertex : epgLeader2Members.get(downStreamPpgMemberEpg)) {
					memberEpgIncreaseCost = memberEpgIncreaseCost.merge(jobConfig.getVertexConfigs().get(downStreamPpgMemberVertex).getResourceSpec());
				}
				for (int i = 0; i < ppgMember2Factor.get(downStreamPpgMemberEpg); ++i) {
					downStreamBaseIncreaseCost = downStreamBaseIncreaseCost.merge(memberEpgIncreaseCost);
				}
			}

			ResourceSpec upStreamGreaterCost = new ResourceSpec.Builder().build();
			for (int i = 0; i < upStreamGreaterUpStreamBaseIncrease; ++i) {
				upStreamGreaterCost = upStreamGreaterCost.merge(upStreamBaseIncreaseCost);
			}
			for (int i = 0; i < upStreamGreaterDownStreamBaseIncrease; ++i) {
				upStreamGreaterCost = upStreamGreaterCost.merge(downStreamBaseIncreaseCost);
			}

			ResourceSpec downStreamGreaterCost = new ResourceSpec.Builder().build();
			for (int i = 0; i < downStreamGreaterDownStreamBaseIncrease; ++i) {
				downStreamGreaterCost = downStreamGreaterCost.merge(downStreamBaseIncreaseCost);
			}
			for (int i = 0; i < downStreamGreaterUpStreamBaseIncrease; ++i) {
				downStreamGreaterCost = downStreamGreaterCost.merge(upStreamBaseIncreaseCost);
			}

			int upStreamGreaterCostMem = upStreamGreaterCost.getHeapMemory() + upStreamGreaterCost.getDirectMemory() + upStreamGreaterCost.getNativeMemory();
			int downStreamGreaterCostMem = downStreamGreaterCost.getHeapMemory() + downStreamGreaterCost.getDirectMemory() + downStreamGreaterCost.getNativeMemory();

			// compare cost

			if (upStreamGreaterCostMem > downStreamGreaterCostMem) {
				useUpStreamGreaterPlan = false;
			} else if (upStreamGreaterCostMem < downStreamGreaterCostMem) {
				useUpStreamGreaterPlan = true;
			} else {
				if (upStreamGreaterCost.getCpuCores() > downStreamGreaterCost.getCpuCores()) {
					useUpStreamGreaterPlan = false;
				} else {
					useUpStreamGreaterPlan = true;
				}
			}
		} else if (upStreamGreaterFeasible) {
			useUpStreamGreaterPlan = true;
		} else if (downStreamGreaterFeasible) {
			useUpStreamGreaterPlan = false;
		} else {
			LOGGER.debug("Could not merge ProportionalParallelism Groups {} and {}.",
				ppgLeader2Members.get(upStreamPpgLeader), ppgLeader2Members.get(downStreamPpgLeader));
			return;
		}

		// update Proportional Parallelism Groups

		if (useUpStreamGreaterPlan) {
			int downStreamNewBase = downStreamBase + upStreamGreaterDownStreamBaseIncrease;
			ppgLeader2Base.put(downStreamPpgLeader, downStreamNewBase);

			ppgLeader2Members.get(downStreamPpgLeader).addAll(ppgLeader2Members.get(upStreamPpgLeader));
			for (JobVertexID upStreamPpgMemberEpg : ppgLeader2Members.get(upStreamPpgLeader)) {
				ppgMember2Leader.put(upStreamPpgMemberEpg, downStreamPpgLeader);
				ppgMember2Factor.put(upStreamPpgMemberEpg, ppgMember2Factor.get(upStreamPpgMemberEpg) * upStreamGreaterK);
			}

			ppgLeader2Members.remove(upStreamPpgLeader);
			ppgLeader2Base.remove(upStreamPpgLeader);

			for (JobVertexID downStreamPpgMemberEpg : ppgLeader2Members.get(downStreamPpgLeader)) {
				epgLeader2TargetParallelism.put(downStreamPpgMemberEpg, downStreamNewBase * ppgMember2Factor.get(downStreamPpgMemberEpg));
			}
		} else {
			int upStreamNewBase = upStreamBase + downStreamGreaterUpStreamBaseIncrease;
			ppgLeader2Base.put(upStreamPpgLeader, upStreamNewBase);

			ppgLeader2Members.get(upStreamPpgLeader).addAll((ppgLeader2Members.get(downStreamPpgLeader)));
			for (JobVertexID downStreamPpgMemberEpg : ppgLeader2Members.get(downStreamPpgLeader)) {
				ppgMember2Leader.put(downStreamPpgMemberEpg, upStreamPpgLeader);
				ppgMember2Factor.put(downStreamPpgMemberEpg, ppgMember2Factor.get(downStreamPpgMemberEpg) * downStreamGreaterK);
			}

			ppgLeader2Members.remove(downStreamPpgLeader);
			ppgLeader2Base.remove(downStreamPpgLeader);

			for (JobVertexID upStreamPpgMemberEpg : ppgLeader2Members.get(upStreamPpgLeader)) {
				epgLeader2TargetParallelism.put(upStreamPpgMemberEpg, upStreamNewBase * ppgMember2Factor.get(upStreamPpgMemberEpg));
			}
		}
	}

	private RescaleJobParallelism generateRescaleParallelismAction(Map<JobVertexID, Integer> targetParallelisms, RestServerClient.JobConfig jobConfig) {

		if (targetParallelisms.isEmpty()) {
			return null;
		}

		// generate rescale action from target parallelisms

		RescaleJobParallelism rescaleJobParallelism = new RescaleJobParallelism(jobID, timeout);

		for (JobVertexID vertexId : targetParallelisms.keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexId);
			if (targetParallelisms.get(vertexId) == vertexConfig.getParallelism()) {
				continue;
			}
			rescaleJobParallelism.addVertex(
				vertexId, vertexConfig.getParallelism(), targetParallelisms.get(vertexId),
				vertexConfig.getResourceSpec(), vertexConfig.getResourceSpec());
		}

		// update rescale action subject to max resource limit

		if (maxCpuLimit != Double.MAX_VALUE || maxMemoryLimit != Integer.MAX_VALUE) {
			RestServerClient.JobConfig targetJobConfig = rescaleJobParallelism.getAppliedJobConfig(jobConfig);
			double targetTotalCpu = targetJobConfig.getJobTotalCpuCores();
			int targetTotalMem = targetJobConfig.getJobTotalMemoryMb();

			if (targetTotalCpu > maxCpuLimit || targetTotalMem > maxMemoryLimit) {

				LOGGER.debug(
					"Try to scale down parallelism: total resource of target job config <cpu, mem>=<{}, {}> exceed max limit <cpu, mem>=<{}, {}>.",
					targetTotalCpu, targetTotalMem, maxCpuLimit, maxMemoryLimit);

				RestServerClient.JobConfig adjustedJobConfig = MaxResourceLimitUtil
					.scaleDownJobConfigToMaxResourceLimit(
						targetJobConfig, maxCpuLimit, maxMemoryLimit);

				if (adjustedJobConfig == null) {
					LOGGER.debug("Give up adjusting.");
					return null;
				}

				rescaleJobParallelism = new RescaleJobParallelism(jobID, timeout);

				for (JobVertexID vertexId : adjustedJobConfig.getVertexConfigs().keySet()) {
					RestServerClient.VertexConfig originVertexConfig = jobConfig.getVertexConfigs().get(vertexId);
					RestServerClient.VertexConfig adjustedVertexConfig = adjustedJobConfig.getVertexConfigs().get(
						vertexId);
					if (originVertexConfig.getParallelism() != adjustedVertexConfig.getParallelism()) {
						rescaleJobParallelism.addVertex(vertexId,
							originVertexConfig.getParallelism(),
							adjustedVertexConfig.getParallelism(),
							originVertexConfig.getResourceSpec(),
							adjustedVertexConfig.getResourceSpec());
					}
				}
			}
		}

		return rescaleJobParallelism;
	}

	/**
	 * Task metrics.
	 */
	public static class TaskMetrics {
		private final JobVertexID jobVertexID;
		private final boolean isParallelSource;
		private final double inputTps;
		private final double outputTps;
		private final double taskLatencyPerRecord;
		private final double sourceLatencyPerRecord;
		private final double waitOutputPerRecord;
		private final double workload;
		private final double delayIncreasingRate;

		public TaskMetrics(
			JobVertexID jobVertexId,
			boolean isParallelSource,
			double inputTps,
			double outputTps,
			double taskLatencyPerRecord,
			double sourceLatencyPerRecord,
			double waitOutputPerRecord,
			double workload,
			double delayIncreasingRate) {

			this.jobVertexID = jobVertexId;
			this.isParallelSource = isParallelSource;
			this.inputTps = inputTps;
			this.outputTps = outputTps;
			this.taskLatencyPerRecord = taskLatencyPerRecord;
			this.sourceLatencyPerRecord = sourceLatencyPerRecord;
			this.waitOutputPerRecord = waitOutputPerRecord;
			this.workload = workload;
			this.delayIncreasingRate = delayIncreasingRate;
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

		public double getTaskLatencyPerRecord() {
			return taskLatencyPerRecord;
		}

		public double getSourceLatencyPerRecord() {
			return sourceLatencyPerRecord;
		}

		public double getWaitOutputPerRecord() {
			return waitOutputPerRecord;
		}

		public double getWorkload() {
			return workload;
		}

		@Override
		public String toString() {
			return "TaskMetrics{JobVertexID:" + jobVertexID
				+ ", isParallelSource:" + isParallelSource
				+ ", inputTps:" + inputTps
				+ ", outputTps:" + outputTps
				+ ", taskLatencyPerRecord:" + taskLatencyPerRecord
				+ ", sourceLatencyPerRecord:" + sourceLatencyPerRecord
				+ ", waitOutputPerRecord:" + waitOutputPerRecord
				+ ", workload:" + workload + "}";
		}

		public double getDelayIncreasingRate() {
			return delayIncreasingRate;
		}
	}
}
