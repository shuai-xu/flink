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
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobDirectMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDirectOOM;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Direct Memory adjuster which can resolve vertex direct oom.
 * If direct oom detected, increase direct memory of corresponding vertices by given ratio.
 */
public class DirectMemoryAdjuster implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(DirectMemoryAdjuster.class);

	private JobID jobID;
	private HealthMonitor monitor;
	private double scaleRatio;
	private long timeout;

	private double maxCpuLimit;
	private int maxMemoryLimit;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.scaleRatio = monitor.getConfig().getDouble(HealthMonitorOptions.RESOURCE_SCALE_RATIO);
		this.timeout = monitor.getConfig().getLong(HealthMonitorOptions.RESOURCE_SCALE_TIME_OUT);
		this.maxCpuLimit = monitor.getConfig().getDouble(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_CPU_CORE);
		this.maxMemoryLimit = monitor.getConfig().getInteger(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_MEMORY_MB);
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		Set<JobVertexID> jobVertexIDs = new HashSet<>();
		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobVertexDirectOOM) {
				JobVertexDirectOOM jobVertexDirectOOM = (JobVertexDirectOOM) symptom;
				LOGGER.debug("Direct OOM detected for vertices {}.", jobVertexDirectOOM.getJobVertexIDs());
				jobVertexIDs.addAll(jobVertexDirectOOM.getJobVertexIDs());
			}
		}

		if (jobVertexIDs.isEmpty()) {
			return null;
		}

		AdjustJobDirectMemory adjustJobDirectMemory = new AdjustJobDirectMemory(jobID, timeout);
		adjustJobDirectMemory.setActionMode(Action.ActionMode.IMMEDIATE);
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID jvId : jobVertexIDs) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			int targetDirectMemory = (int) (currentResource.getDirectMemory() * scaleRatio);
			LOGGER.debug("Target direct memory for vertex {} is {}.", jvId, targetDirectMemory);
			ResourceSpec targetResource =
				new ResourceSpec.Builder(currentResource)
					.setDirectMemoryInMB(targetDirectMemory)
					.build();

			adjustJobDirectMemory.addVertex(
				jvId, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
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

		if (!adjustJobDirectMemory.isEmpty()) {
			LOGGER.info("AdjustJobDirectMemory action generated: {}.", adjustJobDirectMemory);
			return adjustJobDirectMemory;
		}
		return null;
	}
}
