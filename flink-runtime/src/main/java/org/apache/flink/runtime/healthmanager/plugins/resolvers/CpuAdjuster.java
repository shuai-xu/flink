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
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowCpu;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

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
	private double scaleRatio;
	private long timeout;
	private long opportunisticActionDelay;
	private long stableTime;

	private double maxCpuLimit;
	private int maxMemoryLimit;

	private Map<JobVertexID, Double> vertexMaxUtility;
	private long opportunisticActionDelayStart;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.scaleRatio = monitor.getConfig().getDouble(HealthMonitorOptions.RESOURCE_SCALE_RATIO);
		this.timeout = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_TIME_OUT);
		this.maxCpuLimit = monitor.getConfig().getDouble(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_CPU_CORE);
		this.maxMemoryLimit = monitor.getConfig().getInteger(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_MEMORY_MB);
		this.opportunisticActionDelay = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_OPPORTUNISTIC_ACTION_DELAY);
		this.stableTime = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_STABLE_TIME);

		vertexMaxUtility = new HashMap<>();
		opportunisticActionDelayStart = -1;
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		if (opportunisticActionDelayStart < monitor.getLastExecution()) {
			opportunisticActionDelayStart = -1;
			vertexMaxUtility.clear();
		}

		JobVertexHighCpu jobVertexHighCpu = null;
		JobVertexLowCpu jobVertexLowCpu = null;
		JobStable jobStable = null;
		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobStable) {
				jobStable = (JobStable) symptom;
			}

			if (symptom instanceof JobVertexHighCpu) {
				jobVertexHighCpu = (JobVertexHighCpu) symptom;
			}

			if (symptom instanceof  JobVertexLowCpu) {
				jobVertexLowCpu = (JobVertexLowCpu) symptom;
			}
		}

		if (jobStable == null || jobStable.getStableTime() < stableTime) {
			LOGGER.debug("Job unstable, should not rescale.");
			return null;
		}

		if (jobVertexHighCpu != null) {
			LOGGER.debug("High cpu detected for vertices with max utilities {}.", jobVertexHighCpu.getUtilities());
			for (Map.Entry<JobVertexID, Double> entry : jobVertexHighCpu.getUtilities().entrySet()) {
				if (!vertexMaxUtility.containsKey(entry.getKey()) || vertexMaxUtility.get(entry.getKey()) < entry.getValue()) {
					vertexMaxUtility.put(entry.getKey(), entry.getValue());
				}
			}
		}

		if (jobVertexLowCpu != null) {
			LOGGER.debug("Low cpu detected for vertices with max utilities {}.", jobVertexLowCpu.getUtilities());
			// TODO add cpu down scale strategy
			// utilities.putAll(jobVertexLowCpu.getUtilities());
		}

		if (vertexMaxUtility.isEmpty()) {
			return null;
		}

		AdjustJobCpu adjustJobCpu = new AdjustJobCpu(jobID, timeout);
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID jvId : vertexMaxUtility.keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			double utility = vertexMaxUtility.get(jvId);
			double targetCpu = currentResource.getCpuCores() * Math.max(1.0, utility) * scaleRatio;
			LOGGER.debug("Target cpu for vertex {} is {}.", jvId, targetCpu);
			ResourceSpec targetResource = new ResourceSpec.Builder(currentResource)
					.setCpuCores(targetCpu).build();

			adjustJobCpu.addVertex(
				jvId, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
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

		if (!adjustJobCpu.isEmpty()) {
			long now = System.currentTimeMillis();
			if ((jobVertexHighCpu != null && jobVertexHighCpu.isSevere()) ||
				(opportunisticActionDelayStart > 0 &&  now - opportunisticActionDelayStart > opportunisticActionDelay)) {
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
}
