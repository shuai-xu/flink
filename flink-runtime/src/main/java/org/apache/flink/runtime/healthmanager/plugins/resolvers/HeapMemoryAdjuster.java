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
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobHeapMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHeapOOM;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Heap Memory adjuster which can resolve vertex heap oom and full gc.
 * If heap oom or frequent full gc detected, increase heap memory of corresponding vertices by given ratio.
 */
public class HeapMemoryAdjuster implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(HeapMemoryAdjuster.class);

	private static final ConfigOption<Double> HEAP_SCALE_OPTION =
			ConfigOptions.key("heap.memory.scale.ratio").defaultValue(0.5);

	private static final ConfigOption<Long> HEAP_SCALE_TIME_OUT_OPTION =
			ConfigOptions.key("heap.memory.scale.timeout.ms").defaultValue(180000L);

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
		this.scaleRatio = monitor.getConfig().getDouble(HEAP_SCALE_OPTION);
		this.timeout = monitor.getConfig().getLong(HEAP_SCALE_TIME_OUT_OPTION);
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
			if (symptom instanceof JobVertexHeapOOM) {
				JobVertexHeapOOM jobVertexHeapOOM = (JobVertexHeapOOM) symptom;
				LOGGER.debug("Heap OOM detected for vertices {}.", jobVertexHeapOOM.getJobVertexIDs());
				jobVertexIDs.addAll(jobVertexHeapOOM.getJobVertexIDs());
				continue;
			}

			if (symptom instanceof JobVertexFrequentFullGC) {
				JobVertexFrequentFullGC jobVertexFrequentFullGC = (JobVertexFrequentFullGC) symptom;
				LOGGER.debug("Frequent full gc detected for vertices {}.", jobVertexFrequentFullGC.getJobVertexIDs());
				jobVertexIDs.addAll(jobVertexFrequentFullGC.getJobVertexIDs());
			}
		}

		if (jobVertexIDs.isEmpty()) {
			return null;
		}

		AdjustJobHeapMemory adjustJobHeapMemory = new AdjustJobHeapMemory(jobID, timeout);
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID jvId : jobVertexIDs) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			int targetHeapMemory = (int) (currentResource.getHeapMemory() * scaleRatio);
			ResourceSpec targetResource =
				new ResourceSpec.Builder()
					.setHeapMemoryInMB(targetHeapMemory)
					.build()
					.merge(currentResource);

			adjustJobHeapMemory.addVertex(
				jvId, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
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

		if (!adjustJobHeapMemory.isEmpty()) {
			LOGGER.info("AdjustJobHeapMemory action generated: {}.", adjustJobHeapMemory);
			return adjustJobHeapMemory;
		}
		return null;
	}
}
