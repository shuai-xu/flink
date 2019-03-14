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
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobNativeMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighNativeMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexTmKilledDueToMemoryExceed;
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
 * Native Memory adjuster which can resolve vertex native memory overuse.
 * If memory overuse detected:
 *   new_direct_memory = max_among_tasks_of_same_vertex { tm_memory_overuse / num_of_tasks } * ratio + original_direct_memory
 */
public class NativeMemoryAdjuster implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(NativeMemoryAdjuster.class);

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

	private Map<JobVertexID, Double> vertexToScaleUpMaxUtilities;
	private long opportunisticActionDelayStart;

	private JobStable jobStable;
	private JobVertexHighNativeMemory jobVertexHighNativeMemory;
	private JobVertexTmKilledDueToMemoryExceed jobVertexTmKilledDueToMemoryExceed;
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

		vertexToScaleUpMaxUtilities = new HashMap<>();
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
			vertexToScaleUpMaxUtilities.clear();
		}

		if (!diagnose(symptomList)) {
			return null;
		}

		Map<JobVertexID, Integer> targetNative = new HashMap<>();
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		if (jobVertexLowMemory != null) {
			targetNative.putAll(scaleDownVertexNativeMemory(jobConfig));
		}

		if (jobVertexHighNativeMemory != null || jobVertexTmKilledDueToMemoryExceed != null || vertexToScaleUpMaxUtilities != null) {
			targetNative.putAll(scaleUpVertexNativeMemory(jobConfig));
		}

		if (targetNative.isEmpty()) {
			return null;
		}

		AdjustJobNativeMemory adjustJobNativeMemory = new AdjustJobNativeMemory(jobID, timeout);
		for (Map.Entry<JobVertexID, Integer> entry : targetNative.entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int targetNativeMemory = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			ResourceSpec targetResource = new ResourceSpec.Builder(currentResource)
					.setNativeMemoryInMB(targetNativeMemory).build();

			adjustJobNativeMemory.addVertex(
				vertexID, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
		}

		if (maxCpuLimit != Double.MAX_VALUE || maxMemoryLimit != Integer.MAX_VALUE) {
			RestServerClient.JobConfig targetJobConfig = adjustJobNativeMemory.getAppliedJobConfig(jobConfig);
			double targetTotalCpu = targetJobConfig.getJobTotalCpuCores();
			int targetTotalMem = targetJobConfig.getJobTotalMemoryMb();
			if (targetTotalCpu > maxCpuLimit || targetTotalMem > maxMemoryLimit) {
				LOGGER.debug("Give up adjusting: total resource of target job config <cpu, mem>=<{}, {}> exceed max limit <cpu, mem>=<{}, {}>.",
					targetTotalCpu, targetTotalMem, maxCpuLimit, maxMemoryLimit);
				return null;
			}
		}

		if (!adjustJobNativeMemory.isEmpty()) {
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
			if ((jobVertexHighNativeMemory != null && jobVertexHighNativeMemory.isSevere()) ||
				jobVertexTmKilledDueToMemoryExceed != null) {
				adjustJobNativeMemory.setActionMode(Action.ActionMode.IMMEDIATE);
			} else if (opportunisticActionDelayStart > 0 &&
				now - opportunisticActionDelayStart > opportunisticActionDelay &&
				now - lastCheckpointTime < checkpointIntervalThreshold) {
				LOGGER.debug("Upgrade opportunistic action to immediate action.");
				adjustJobNativeMemory.setActionMode(Action.ActionMode.IMMEDIATE);
			} else {
				if (opportunisticActionDelayStart < 0) {
					opportunisticActionDelayStart = now;
				}
				adjustJobNativeMemory.setActionMode(Action.ActionMode.OPPORTUNISTIC);
			}
			LOGGER.info("AdjustJobNativeMemory action generated: {}.", adjustJobNativeMemory);
			return adjustJobNativeMemory;
		}
		return null;
	}

	@VisibleForTesting
	public boolean diagnose(List<Symptom> symptomList) {
		jobStable = null;
		jobVertexHighNativeMemory = null;
		jobVertexLowMemory = null;
		jobVertexTmKilledDueToMemoryExceed = null;

		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobStable) {
				jobStable = (JobStable) symptom;
				continue;
			}

			if (symptom instanceof JobVertexHighNativeMemory) {
				jobVertexHighNativeMemory = (JobVertexHighNativeMemory) symptom;
				continue;
			}

			if (symptom instanceof  JobVertexTmKilledDueToMemoryExceed) {
				jobVertexTmKilledDueToMemoryExceed = (JobVertexTmKilledDueToMemoryExceed) symptom;
				continue;
			}

			if (symptom instanceof JobVertexLowMemory) {
				jobVertexLowMemory = (JobVertexLowMemory) symptom;
			}
		}

		if (jobVertexTmKilledDueToMemoryExceed != null) {
			LOGGER.debug("Task manager killed due to memory exceed detected, should rescale.");
			return true;
		}

		if (jobVertexHighNativeMemory != null && jobVertexHighNativeMemory.isCritical()) {
			LOGGER.debug("Critical native memory high detected, should rescale.");
			return true;
		}

		if (jobStable == null || jobStable.getStableTime() < stableTime) {
			LOGGER.debug("Job unstable, should not rescale.");
			return false;
		}

		if (jobVertexHighNativeMemory == null && jobVertexLowMemory == null) {
			LOGGER.debug("No need to rescale.");
			return false;
		}

		return true;
	}

	@VisibleForTesting
	public Map<JobVertexID, Integer> scaleUpVertexNativeMemory(RestServerClient.JobConfig jobConfig) {
		if (jobVertexHighNativeMemory != null) {
			for (Map.Entry<JobVertexID, Double> entry : jobVertexHighNativeMemory.getUtilities().entrySet()) {
				if (!vertexToScaleUpMaxUtilities.containsKey(entry.getKey()) || vertexToScaleUpMaxUtilities.get(
					entry.getKey()) < entry.getValue()) {
					vertexToScaleUpMaxUtilities.put(entry.getKey(), entry.getValue());
				}
			}
		}

		if (jobVertexTmKilledDueToMemoryExceed != null) {
			for (Map.Entry<JobVertexID, Double> entry : jobVertexTmKilledDueToMemoryExceed.getUtilities().entrySet()) {
				if (!vertexToScaleUpMaxUtilities.containsKey(entry.getKey()) || vertexToScaleUpMaxUtilities.get(
					entry.getKey()) < entry.getValue()) {
					vertexToScaleUpMaxUtilities.put(entry.getKey(), entry.getValue());
				}
			}
		}

		Map<JobVertexID, Integer> results = new HashMap<>();
		for (JobVertexID jvId : vertexToScaleUpMaxUtilities.keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			int targetNativeMemory;
			if (currentResource.getNativeMemory() == 0) {
				targetNativeMemory = (int) Math.ceil(vertexToScaleUpMaxUtilities.get(jvId) * scaleUpRatio);
			} else {
				targetNativeMemory = (int) Math.ceil(
					currentResource.getNativeMemory() * Math.max(1.0, vertexToScaleUpMaxUtilities.get(jvId)) * scaleUpRatio);
			}
			results.put(jvId, targetNativeMemory);
			LOGGER.debug("Scale up, target native memory for vertex {} is {}.", jvId, targetNativeMemory);
		}

		return results;
	}

	@VisibleForTesting
	public Map<JobVertexID, Integer> scaleDownVertexNativeMemory(RestServerClient.JobConfig jobConfig) {
		Map<JobVertexID, Integer> results = new HashMap<>();
		for (Map.Entry<JobVertexID, Double> entry : jobVertexLowMemory.getNativeUtilities().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			double utility = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			int targetNativeMemory = vertexConfig.getResourceSpec().getNativeMemory();
			if (targetNativeMemory == 0) {
				targetNativeMemory = 1;
			}
			targetNativeMemory = (int) Math.ceil(targetNativeMemory * utility * scaleDownRatio);
			results.put(vertexID, targetNativeMemory);
			LOGGER.debug("Scale down, target native memory for vertex {} is {}.", vertexID, targetNativeMemory);
		}
		return results;
	}
}
