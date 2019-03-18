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
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobHeapMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHeapOOM;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MaxResourceLimitUtil;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Heap Memory adjuster which can resolve vertex heap oom and full gc.
 * If heap oom or frequent full gc detected, increase heap memory of corresponding vertices by given ratio.
 */
public class HeapMemoryAdjuster implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(HeapMemoryAdjuster.class);

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

	private Set<JobVertexID> vertexToScaleUp;
	private long opportunisticActionDelayStart;

	private JobStable jobStable;
	private JobVertexHeapOOM jobVertexHeapOOM;
	private JobVertexFrequentFullGC jobVertexFrequentFullGC;
	private JobVertexLongTimeFullGC jobVertexLongTimeFullGC;
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

		vertexToScaleUp = new HashSet<>();
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
			vertexToScaleUp.clear();
		}

		if (!diagnose(symptomList)) {
			return null;
		}

		Map<JobVertexID, Integer> targetHeap = new HashMap<>();
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();

		if (jobVertexLowMemory != null) {
			targetHeap.putAll(scaleDownVertexHeapMem(jobConfig));
		}

		if (jobVertexHeapOOM != null || jobVertexFrequentFullGC != null || jobVertexLongTimeFullGC != null || !vertexToScaleUp.isEmpty()) {
			targetHeap.putAll(scaleUpVertexHeapMem(jobConfig));
		}

		if (targetHeap.isEmpty()) {
			return null;
		}

		AdjustJobHeapMemory adjustJobHeapMemory = new AdjustJobHeapMemory(jobID, timeout);
		for (Map.Entry<JobVertexID, Integer> entry : targetHeap.entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int targetHeapMemory = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			ResourceSpec targetResource =
				new ResourceSpec.Builder(currentResource)
					.setHeapMemoryInMB(targetHeapMemory)
					.build();
			adjustJobHeapMemory.addVertex(
				vertexID, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
		}

		if (maxCpuLimit != Double.MAX_VALUE || maxMemoryLimit != Integer.MAX_VALUE) {
			RestServerClient.JobConfig targetJobConfig = adjustJobHeapMemory.getAppliedJobConfig(jobConfig);
			double targetTotalCpu = targetJobConfig.getJobTotalCpuCores();
			int targetTotalMem = targetJobConfig.getJobTotalMemoryMb();
			if (targetTotalCpu > maxCpuLimit || targetTotalMem > maxMemoryLimit) {
				LOGGER.debug("Give up adjusting: total resource of target job config <cpu, mem>=<{}, {}> exceed max limit <cpu, mem>=<{}, {}>.",
					targetTotalCpu, targetTotalMem, maxCpuLimit, maxMemoryLimit);
				return null;
			}
		}

		adjustJobHeapMemory.exculdeMinorDiffVertices(monitor.getConfig());

		if (!adjustJobHeapMemory.isEmpty()) {
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
			if (jobVertexHeapOOM != null ||
				(jobVertexFrequentFullGC != null && jobVertexFrequentFullGC.isSevere()) ||
				(jobVertexLongTimeFullGC != null && jobVertexLongTimeFullGC.isSevere())) {
				adjustJobHeapMemory.setActionMode(Action.ActionMode.IMMEDIATE);
			} else if (opportunisticActionDelayStart > 0 &&
				now - opportunisticActionDelayStart > opportunisticActionDelay &&
				now - lastCheckpointTime < checkpointIntervalThreshold) {
				LOGGER.debug("Upgrade opportunistic action to immediate action.");
				adjustJobHeapMemory.setActionMode(Action.ActionMode.IMMEDIATE);
			} else {
				if (opportunisticActionDelayStart < 0) {
					opportunisticActionDelayStart = now;
				}
				adjustJobHeapMemory.setActionMode(Action.ActionMode.OPPORTUNISTIC);
			}
			LOGGER.info("AdjustJobHeapMemory action generated: {}.", adjustJobHeapMemory);
			return adjustJobHeapMemory;
		}
		return null;
	}

	@VisibleForTesting
	public boolean diagnose(List<Symptom> symptomList) {
		jobStable = null;
		jobVertexHeapOOM = null;
		jobVertexFrequentFullGC = null;
		jobVertexLongTimeFullGC = null;
		jobVertexLowMemory = null;

		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobStable) {
				jobStable = (JobStable) symptom;
				continue;
			}

			if (symptom instanceof JobVertexHeapOOM) {
				jobVertexHeapOOM = (JobVertexHeapOOM) symptom;
				continue;
			}

			if (symptom instanceof JobVertexFrequentFullGC) {
				jobVertexFrequentFullGC = (JobVertexFrequentFullGC) symptom;
				continue;
			}

			if (symptom instanceof JobVertexLongTimeFullGC) {
				jobVertexLongTimeFullGC = (JobVertexLongTimeFullGC) symptom;
			}

			if (symptom instanceof JobVertexLowMemory) {
				jobVertexLowMemory = (JobVertexLowMemory) symptom;
			}
		}

		if (jobVertexHeapOOM != null) {
			LOGGER.debug("Heap OOM detected, should rescale.");
			return true;
		}

		if (jobVertexLongTimeFullGC != null && jobVertexLongTimeFullGC.isCritical()) {
			LOGGER.debug("Critical long time full GC detected, should rescale.");
			return true;
		}

		if (jobStable == null || jobStable.getStableTime() < stableTime) {
			LOGGER.debug("Job unstable, should not rescale.");
			return false;
		}

		if (jobVertexFrequentFullGC == null && jobVertexLongTimeFullGC == null && jobVertexLowMemory == null) {
			LOGGER.debug("No need to rescale.");
			return false;
		}

		return true;
	}

	@VisibleForTesting
	public Map<JobVertexID, Integer> scaleUpVertexHeapMem(RestServerClient.JobConfig jobConfig) {
		if (jobVertexHeapOOM != null) {
			vertexToScaleUp.addAll(jobVertexHeapOOM.getJobVertexIDs());
		}

		if (jobVertexFrequentFullGC != null) {
			vertexToScaleUp.addAll(jobVertexFrequentFullGC.getJobVertexIDs());
		}

		if (jobVertexLongTimeFullGC != null) {
			vertexToScaleUp.addAll(jobVertexLongTimeFullGC.getJobVertexIDs());
		}

		Map<JobVertexID, Integer> results = new HashMap<>();
		for (JobVertexID vertexID : vertexToScaleUp) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			int targetHeapMemory;
			if (currentResource.getHeapMemory() == 0) {
				targetHeapMemory = (int) Math.ceil(1 * scaleUpRatio);
			} else {
				targetHeapMemory = (int) Math.ceil(currentResource.getHeapMemory() * scaleUpRatio);
			}
			results.put(vertexID, targetHeapMemory);
			LOGGER.debug("Scale up, target heap memory for vertex {} is {}.", vertexID, targetHeapMemory);
		}
		return results;
	}

	@VisibleForTesting
	public Map<JobVertexID, Integer> scaleDownVertexHeapMem(RestServerClient.JobConfig jobConfig) {
		Map<JobVertexID, Integer> results = new HashMap<>();
		for (Map.Entry<JobVertexID, Double> entry : jobVertexLowMemory.getHeapUtilities().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			double utility = entry.getValue();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(vertexID);
			int targetHeapMemory = vertexConfig.getResourceSpec().getHeapMemory();
			if (targetHeapMemory == 0) {
				targetHeapMemory = 1;
			}
			if (utility * scaleDownRatio < 1) {
				targetHeapMemory = (int) Math.ceil(targetHeapMemory * utility * scaleDownRatio);
			}
			results.put(vertexID, targetHeapMemory);
			LOGGER.debug("Scale down, target heap memory for vertex {} is {}.", vertexID, targetHeapMemory);
		}
		return results;
	}
}
