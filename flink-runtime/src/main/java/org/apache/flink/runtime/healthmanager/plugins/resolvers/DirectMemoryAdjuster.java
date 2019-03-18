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
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobDirectMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDirectOOM;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
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
 * Direct Memory adjuster which can resolve vertex direct oom.
 * If direct oom detected, increase direct memory of corresponding vertices by given ratio.
 */
public class DirectMemoryAdjuster implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(DirectMemoryAdjuster.class);

	private JobID jobID;
	private HealthMonitor monitor;
	private double scaleUpRatio;
	private double scaleDownRatio;
	private long timeout;
	private long opportunisticActionDelay;
	private long stableTime;
	private long checkpointIntervalThreshold;

	private double maxCpuLimit;
	private int maxMemoryLimit;

	private long opportunisticActionDelayStart;

	private JobStable jobStable;
	private JobVertexDirectOOM jobVertexDirectOOM;
	private JobVertexLowMemory jobVertexLowMemory;

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
		}

		if (!diagnose(symptomList)) {
			return null;
		}

		Map<JobVertexID, Integer> targetDirect = new HashMap<>();
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		if (jobVertexLowMemory != null) {
			targetDirect.putAll(scaleDownVertexDirectMem(jobConfig));
		}

		if (jobVertexDirectOOM != null) {
			targetDirect.putAll(scaleUpVertexDirectMem(jobConfig));
		}

		if (targetDirect.isEmpty()) {
			return null;
		}

		AdjustJobDirectMemory adjustJobDirectMemory = new AdjustJobDirectMemory(jobID, timeout);
		for (Map.Entry<JobVertexID, Integer> entry : targetDirect.entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int targetDirectMemory = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			ResourceSpec targetResource =
				new ResourceSpec.Builder(currentResource)
					.setDirectMemoryInMB(targetDirectMemory)
					.build();
			adjustJobDirectMemory.addVertex(
				vertexID, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
		}

		if (maxCpuLimit != Double.MAX_VALUE || maxMemoryLimit != Integer.MAX_VALUE) {
			RestServerClient.JobConfig targetJobConfig = adjustJobDirectMemory.getAppliedJobConfig(jobConfig);
			double targetTotalCpu = targetJobConfig.getJobTotalCpuCores();
			int targetTotalMem = targetJobConfig.getJobTotalMemoryMb();
			if (targetTotalCpu > maxCpuLimit || targetTotalMem > maxMemoryLimit) {
				LOGGER.debug("Give up adjusting: total resource of target job config <cpu, mem>=<{}, {}> exceed max limit <cpu, mem>=<{}, {}>.",
					targetTotalCpu, targetTotalMem, maxCpuLimit, maxMemoryLimit);
				return null;
			}
		}

		adjustJobDirectMemory.exculdeMinorDiffVertices(monitor.getConfig());

		if (!adjustJobDirectMemory.isEmpty()) {
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
			if (jobVertexDirectOOM != null) {
				adjustJobDirectMemory.setActionMode(Action.ActionMode.IMMEDIATE);
			} else if (opportunisticActionDelayStart > 0 &&
				now - opportunisticActionDelayStart > opportunisticActionDelay &&
				now - lastCheckpointTime < checkpointIntervalThreshold) {
				LOGGER.debug("Upgrade opportunistic action to immediate action.");
				adjustJobDirectMemory.setActionMode(Action.ActionMode.IMMEDIATE);
			} else {
				if (opportunisticActionDelayStart < 0) {
					opportunisticActionDelayStart = now;
				}
				adjustJobDirectMemory.setActionMode(Action.ActionMode.OPPORTUNISTIC);
			}
			LOGGER.info("AdjustJobDirectMemory action generated: {}.", adjustJobDirectMemory);
			return adjustJobDirectMemory;
		}
		return null;
	}

	@VisibleForTesting
	public boolean diagnose(List<Symptom> symptomList) {
		jobStable = null;
		jobVertexDirectOOM = null;
		jobVertexLowMemory = null;

		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobStable) {
				jobStable = (JobStable) symptom;
				continue;
			}

			if (symptom instanceof JobVertexDirectOOM) {
				jobVertexDirectOOM = (JobVertexDirectOOM) symptom;
				continue;
			}

			if (symptom instanceof JobVertexLowMemory) {
				jobVertexLowMemory = (JobVertexLowMemory) symptom;
			}
		}

		if (jobVertexDirectOOM != null) {
			LOGGER.debug("Direct OOM detected, should rescale.");
			return true;
		}

		if (jobStable == null || jobStable.getStableTime() < stableTime) {
			LOGGER.debug("Job unstable, should not rescale.");
			return false;
		}

		if (jobVertexLowMemory == null) {
			LOGGER.debug("No need to rescale.");
			return false;
		}

		return true;
	}

	@VisibleForTesting
	public Map<JobVertexID, Integer> scaleUpVertexDirectMem(RestServerClient.JobConfig jobConfig) {
		Map<JobVertexID, Integer> results = new HashMap<>();
		for (JobVertexID vertexID : jobVertexDirectOOM.getJobVertexIDs()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			int targetDirectMemory;
			if (currentResource.getDirectMemory() == 0) {
				targetDirectMemory = (int) Math.ceil(1 * scaleUpRatio);
			} else {
				targetDirectMemory = (int) Math.ceil(currentResource.getDirectMemory() * scaleUpRatio);
			}
			results.put(vertexID, targetDirectMemory);
			LOGGER.debug("Scale up, target direct memory for vertex {} is {}.", vertexID, targetDirectMemory);
		}
		return results;
	}

	@VisibleForTesting
	public Map<JobVertexID, Integer> scaleDownVertexDirectMem(RestServerClient.JobConfig jobConfig) {
		Map<JobVertexID, Integer> results = new HashMap<>();
		for (Map.Entry<JobVertexID, Double> entry : jobVertexLowMemory.getNonHeapUtilities().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			double utility = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			int targetDirectMemory = vertexConfig.getResourceSpec().getDirectMemory();
			if (targetDirectMemory == 0) {
				targetDirectMemory = 1;
			}
			if (utility * scaleDownRatio < 1) {
				targetDirectMemory = (int) Math.ceil(targetDirectMemory * utility * scaleDownRatio);
			}
			results.put(vertexID, targetDirectMemory);
			LOGGER.debug("Scale down, target direct memory for vertex {} is {}.", vertexID, targetDirectMemory);
		}
		return results;
	}
}
