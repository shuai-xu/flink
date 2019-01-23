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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobNativeMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexNativeMemOveruse;
import org.apache.flink.runtime.jobgraph.JobVertexID;

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

	private static final ConfigOption<Double> NATIVE_SCALE_OPTION =
		ConfigOptions.key("native.memory.scale.ratio").defaultValue(0.5);

	private static final ConfigOption<Long> NATIVE_SCALE_TIME_OUT_OPTION =
		ConfigOptions.key("native.memory.scale.timeout.ms").defaultValue(180000L);

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
		this.scaleRatio = monitor.getConfig().getDouble(NATIVE_SCALE_OPTION);
		this.timeout = monitor.getConfig().getLong(NATIVE_SCALE_TIME_OUT_OPTION);
		this.maxCpuLimit = monitor.getConfig().getDouble(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_CPU_CORE);
		this.maxMemoryLimit = monitor.getConfig().getInteger(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_MEMORY_MB);
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		Map<JobVertexID, Double> vertexMaxOveruse = new HashMap<>();
		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobVertexNativeMemOveruse) {
				JobVertexNativeMemOveruse jobVertexNativeMemOveruse = (JobVertexNativeMemOveruse) symptom;
				LOGGER.debug("Native memory overuse detected for vertices with max overuses {}.", jobVertexNativeMemOveruse.getOveruses());
				Map<JobVertexID, Double> overuses = jobVertexNativeMemOveruse.getOveruses();
				for (JobVertexID jvId : overuses.keySet()) {
					if (!vertexMaxOveruse.containsKey(jvId) || vertexMaxOveruse.get(jvId) < overuses.get(jvId)) {
						vertexMaxOveruse.put(jvId, overuses.get(jvId));
					}
				}
			}
		}

		if (vertexMaxOveruse.isEmpty()) {
			return null;
		}

		AdjustJobNativeMemory adjustJobNativeMemory = new AdjustJobNativeMemory(jobID, timeout);
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID jvId : vertexMaxOveruse.keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			int targetNativeMemory = (int) (vertexMaxOveruse.get(jvId) * (1 + scaleRatio));
			LOGGER.debug("Target native memory for vertex {} is {}.", jvId, targetNativeMemory);
			ResourceSpec targetResource =
				new ResourceSpec.Builder()
					.setNativeMemoryInMB(targetNativeMemory)
					.build()
					.merge(currentResource);

			adjustJobNativeMemory.addVertex(
				jvId, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
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
			LOGGER.info("AdjustJobNativeMemory action generated: {}.", adjustJobNativeMemory);
			return adjustJobNativeMemory;
		}
		return null;
	}
}
