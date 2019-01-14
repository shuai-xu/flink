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
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowCpu;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CPU adjuster which can resolve vertex cpu high.
 * If cpu high detected, increase CPU of corresponding vertices by given ratio.
 */
public class CpuAdjuster implements Resolver {

	private static final ConfigOption<Double> CPU_SCALE_RATIO =
		ConfigOptions.key("cpu.scale.ratio").defaultValue(0.5);

	private static final ConfigOption<Long> CPU_SCALE_TIME_OUT_OPTION =
		ConfigOptions.key("cpu.scale.timeout.ms").defaultValue(180000L);

	private JobID jobID;
	private HealthMonitor monitor;
	private double scaleRatio;
	private long timeout;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.scaleRatio = monitor.getConfig().getDouble(CPU_SCALE_RATIO);
		this.timeout = monitor.getConfig().getLong(CPU_SCALE_TIME_OUT_OPTION);
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {

		JobVertexHighCpu jobVertexHighCpu = null;
		JobVertexLowCpu jobVertexLowCpu = null;
		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobVertexHighCpu) {
				jobVertexHighCpu = (JobVertexHighCpu) symptom;
			}

			if (symptom instanceof  JobVertexLowCpu) {
				jobVertexLowCpu = (JobVertexLowCpu) symptom;
			}
		}

		Map<JobVertexID, Double> utilities = new HashMap<>();
		if (jobVertexHighCpu != null) {
			utilities.putAll(jobVertexHighCpu.getUtilities());
		}
		if (jobVertexLowCpu != null) {
			utilities.putAll(jobVertexLowCpu.getUtilities());
		}

		Map<JobVertexID, Double> vertexMaxUtility = new HashMap<>();
		for (JobVertexID jvId : utilities.keySet()) {
			if (!vertexMaxUtility.containsKey(jvId) || vertexMaxUtility.get(jvId) < utilities.get(jvId)) {
				vertexMaxUtility.put(jvId, utilities.get(jvId));
			}
		}

		AdjustJobCpu adjustJobCpu = new AdjustJobCpu(jobID, timeout);
		RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
		for (JobVertexID jvId : vertexMaxUtility.keySet()) {
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jvId);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			double targetCpu = currentResource.getCpuCores() * vertexMaxUtility.get(jvId) * (1.0 + scaleRatio);
			ResourceSpec.Builder builder = new ResourceSpec.Builder()
					.setCpuCores(targetCpu)
					.setDirectMemoryInMB(currentResource.getDirectMemory())
					.setHeapMemoryInMB(currentResource.getHeapMemory())
					.setNativeMemoryInMB(currentResource.getNativeMemory())
					.setStateSizeInMB(currentResource.getStateSize());
			for (Resource resource : currentResource.getExtendedResources().values()) {
				builder.addExtendedResource(resource);
			}
			ResourceSpec targetResource = builder.build();

			adjustJobCpu.addVertex(
				jvId, vertexConfig.getParallelism(), vertexConfig.getParallelism(), currentResource, targetResource);
		}
		return adjustJobCpu.isEmpty() ? null : adjustJobCpu;
	}
}
