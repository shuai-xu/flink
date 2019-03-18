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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowCpu;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MaxResourceLimitUtil;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CPU adjuster which can resolve vertex cpu high.
 * If cpu high detected, increase CPU of corresponding vertices by given ratio.
 */
public class CpuAdjuster implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(CpuAdjuster.class);

	private JobID jobID;
	private HealthMonitor monitor;
	private double scaleUpRatio;
	private double scaleDownRatio;
	private long timeout;
	private long opportunisticActionDelay;
	private long stableTime;

	private double maxCpuLimit;
	private int maxMemoryLimit;

	private Map<JobVertexID, Double> vertexToScaleUpMaxUtility;
	private long opportunisticActionDelayStart;
	private long checkpointIntervalThreshold;

	private JobVertexHighCpu jobVertexHighCpu;
	private JobVertexLowCpu jobVertexLowCpu;
	private JobStable jobStable;
	private JobVertexFrequentFullGC jobVertexFrequentFullGC;
	private JobVertexLongTimeFullGC jobVertexLongTimeFullGC;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.scaleUpRatio = monitor.getConfig().getDouble(HealthMonitorOptions.RESOURCE_SCALE_UP_RATIO);
		this.scaleDownRatio = monitor.getConfig().getDouble(HealthMonitorOptions.RESOURCE_SCALE_DOWN_RATIO);
		this.timeout = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_TIME_OUT);
		this.opportunisticActionDelay = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_OPPORTUNISTIC_ACTION_DELAY);
		this.stableTime = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_STABLE_TIME);
		this.checkpointIntervalThreshold =  monitor.getConfig().getLong(HealthMonitorOptions.PARALLELISM_SCALE_CHECKPOINT_THRESHOLD);

		this.maxCpuLimit = MaxResourceLimitUtil.getMaxCpu(monitor.getConfig());
		this.maxMemoryLimit = MaxResourceLimitUtil.getMaxMem(monitor.getConfig());

		vertexToScaleUpMaxUtility = new HashMap<>();
		opportunisticActionDelayStart = -1;
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		if (opportunisticActionDelayStart < monitor.getJobStartExecutionTime()) {
			opportunisticActionDelayStart = -1;
			vertexToScaleUpMaxUtility.clear();
		}

		if (!diagnose(symptomList)) {
			return null;
		}

		Map<JobVertexID, Double> targetCpu = new HashMap<>();
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		if (jobVertexLowCpu != null) {
			targetCpu.putAll(scaleDownVertexCpu(jobConfig));
		}

		if (jobVertexHighCpu != null || !vertexToScaleUpMaxUtility.isEmpty()) {
			targetCpu.putAll(scaleUpVertexCpu(jobConfig));
		}

		if (targetCpu.isEmpty()) {
			return null;
		}

		AdjustJobCpu adjustJobCpu = new AdjustJobCpu(jobID, timeout);
		for (Map.Entry<JobVertexID, Double> entry : targetCpu.entrySet()) {
			JobVertexID vertexID = entry.getKey();
			double target = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			ResourceSpec targetResource = new ResourceSpec.Builder(currentResource)
					.setCpuCores(target).build();

			adjustJobCpu.addVertex(
				vertexID, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
		}

		if (maxCpuLimit != Double.MAX_VALUE || maxMemoryLimit != Integer.MAX_VALUE) {
			RestServerClient.JobConfig targetJobConfig = adjustJobCpu.getAppliedJobConfig(jobConfig);
			double targetTotalCpu = targetJobConfig.getJobTotalCpuCores();
			int targetTotalMem = targetJobConfig.getJobTotalMemoryMb();
			if (targetTotalCpu > maxCpuLimit || targetTotalMem > maxMemoryLimit) {
				LOGGER.debug("Give up adjusting: total resource of target job config <cpu, mem>=<{}, {}> exceed max limit <cpu, mem>=<{}, {}>.",
					targetTotalCpu, targetTotalMem, maxCpuLimit, maxMemoryLimit);
				return null;
			}
		}

		adjustJobCpu.exculdeMinorDiffVertices(monitor.getConfig());

		if (!adjustJobCpu.isEmpty()) {
			long lastCheckpointTime = 0;
			try {
				CheckpointStatistics completedCheckpointStats = monitor.getRestServerClient().getLatestCheckPointStates(monitor.getJobID());
				if (completedCheckpointStats != null) {
					lastCheckpointTime = completedCheckpointStats.getLatestAckTimestamp();
				}
			} catch (Exception e) {
				// fail to get checkpoint info.
			}

			long now = System.currentTimeMillis();
			if (opportunisticActionDelayStart > 0 &&
				now - opportunisticActionDelayStart > opportunisticActionDelay &&
				now - lastCheckpointTime < checkpointIntervalThreshold) {
				LOGGER.debug("Upgrade opportunistic action to immediate action.");
				adjustJobCpu.setActionMode(Action.ActionMode.IMMEDIATE);
			} else {
				if (opportunisticActionDelayStart < 0) {
					opportunisticActionDelayStart = now;
				}
				adjustJobCpu.setActionMode(Action.ActionMode.OPPORTUNISTIC);
			}
			LOGGER.info("AdjustJobCpu action generated: {}.", adjustJobCpu);
			return adjustJobCpu;
		}
		return null;
	}

	@VisibleForTesting
	public boolean diagnose(List<Symptom> symptomList) {
		jobVertexHighCpu = null;
		jobVertexLowCpu = null;
		jobStable = null;
		jobVertexFrequentFullGC = null;
		jobVertexLongTimeFullGC = null;

		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobStable) {
				jobStable = (JobStable) symptom;
				continue;
			}

			if (symptom instanceof JobVertexHighCpu) {
				jobVertexHighCpu = (JobVertexHighCpu) symptom;
				LOGGER.debug("High cpu detected for vertices with max utilities {}.", jobVertexHighCpu.getUtilities());
				continue;
			}

			if (symptom instanceof  JobVertexLowCpu) {
				jobVertexLowCpu = (JobVertexLowCpu) symptom;
				LOGGER.debug("Low cpu detected for vertices with max utilities {}.", jobVertexLowCpu.getUtilities());
			}

			if (symptom instanceof JobVertexFrequentFullGC) {
				jobVertexFrequentFullGC = (JobVertexFrequentFullGC) symptom;
				LOGGER.debug("Frequent full gc detected for vertices {}.", jobVertexFrequentFullGC.getJobVertexIDs());
			}

			if (symptom instanceof JobVertexLongTimeFullGC) {
				jobVertexLongTimeFullGC = (JobVertexLongTimeFullGC) symptom;
				LOGGER.debug("Long time full gc detected for vertices {}.", jobVertexLongTimeFullGC.getJobVertexIDs());
			}
		}

		if (jobStable == null || jobStable.getStableTime() < stableTime) {
			LOGGER.debug("Job unstable, should not rescale.");
			return false;
		}

		if ((jobVertexFrequentFullGC != null && jobVertexFrequentFullGC.isSevere()) ||
			(jobVertexLongTimeFullGC != null && jobVertexLongTimeFullGC.isSevere())) {
			LOGGER.debug("GC is severe, should not rescale.");
			return false;
		}

		if (jobVertexHighCpu == null && jobVertexLowCpu == null) {
			LOGGER.debug("No need to rescale.");
			return false;
		}

		return true;
	}

	@VisibleForTesting
	public Map<JobVertexID, Double> scaleUpVertexCpu(RestServerClient.JobConfig jobConfig) {
		for (Map.Entry<JobVertexID, Double> entry : jobVertexHighCpu.getUtilities().entrySet()) {
			if (!vertexToScaleUpMaxUtility.containsKey(entry.getKey()) || vertexToScaleUpMaxUtility.get(entry.getKey()) < entry.getValue()) {
				vertexToScaleUpMaxUtility.put(entry.getKey(), entry.getValue());
			}
		}

		Map<JobVertexID, Double> results = new HashMap<>();
		for (JobVertexID jvId : vertexToScaleUpMaxUtility.keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			double utility = vertexToScaleUpMaxUtility.get(jvId);
			double targetCpu = currentResource.getCpuCores() * Math.max(1.0, utility) * scaleUpRatio;
			results.put(jvId, targetCpu);
			LOGGER.debug("Scale up, target cpu for vertex {} is {}.", jvId, targetCpu);
		}

		return results;
	}

	@VisibleForTesting
	public Map<JobVertexID, Double> scaleDownVertexCpu(RestServerClient.JobConfig jobConfig) {
		Map<JobVertexID, Double> results = new HashMap<>();
		for (Map.Entry<JobVertexID, Double> entry : jobVertexLowCpu.getUtilities().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			double utility = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			double targetCpu = currentResource.getCpuCores();
			if (targetCpu == 0.0) {
				targetCpu = 1.0;
			}
			targetCpu = targetCpu * utility * scaleDownRatio;
			results.put(vertexID, targetCpu);
			LOGGER.debug("Scale down, target cpu for vertex {} is {}.", vertexID, targetCpu);
		}
		return results;
	}
}
