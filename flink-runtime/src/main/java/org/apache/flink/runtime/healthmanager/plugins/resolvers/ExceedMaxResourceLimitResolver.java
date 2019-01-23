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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.RescaleJobParallelism;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobExceedMaxResourceLimit;
import org.apache.flink.runtime.healthmanager.plugins.utils.MaxResourceLimitUtil;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Resolve job total resource exceed max limit.
 * If job exceed max resource limit detected, scale down job parallelism.
 */
public class ExceedMaxResourceLimitResolver implements Resolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExceedMaxResourceLimitResolver.class);

	private static final ConfigOption<Double> RATIO_OPTION =
		ConfigOptions.key("exceed-max-total-resource.rescale.ratio").defaultValue(0.9);

	private static final ConfigOption<Long> TIME_OUT_OPTION =
		ConfigOptions.key("exceed-max-total-resource.rescale.timeout.ms").defaultValue(180000L);

	private JobID jobID;
	private HealthMonitor monitor;
	private double ratio;
	private long timeout;

	private double maxCpuLimit;
	private int maxMemoryLimit;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.ratio = monitor.getConfig().getDouble(RATIO_OPTION);
		this.timeout = monitor.getConfig().getLong(TIME_OUT_OPTION);
		this.maxCpuLimit = monitor.getConfig().getDouble(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_CPU_CORE);
		this.maxMemoryLimit = monitor.getConfig().getInteger(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_MEMORY_MB);
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {
		LOGGER.debug("Start resolving.");

		JobExceedMaxResourceLimit jobExceedMaxResourceLimit = null;
		for (Symptom symptom : symptomList) {
			if (symptom instanceof  JobExceedMaxResourceLimit) {
				jobExceedMaxResourceLimit = (JobExceedMaxResourceLimit) symptom;
				LOGGER.debug("Job exceed max resource limit detected.");
				break;
			}
		}

		if (jobExceedMaxResourceLimit == null) {
			return null;
		}

		RestServerClient.JobConfig currentJobConfig = monitor.getJobConfig();
		double totalCpu = currentJobConfig.getJobTotalCpuCores();
		int totalMem = currentJobConfig.getJobTotalMemoryMb();

		RestServerClient.JobConfig targetJobConfig;
		if (totalCpu > maxCpuLimit || totalMem > maxMemoryLimit) {
			LOGGER.debug("Current job total resource exceed max limit. Down scale job to max resource limit <cpu, mem>=<{}, {}>.",
				maxCpuLimit, maxMemoryLimit);
			targetJobConfig = MaxResourceLimitUtil.scaleDownJobConfigToMaxResourceLimit(currentJobConfig, maxCpuLimit, maxMemoryLimit);
		} else {
			LOGGER.debug("Current job total resource does not exceed max limit. Down scale job by ratio {}. Limit <cpu, mem>=<{}, {}>.",
				ratio, totalCpu * ratio, (int) (totalMem * ratio));
			targetJobConfig = MaxResourceLimitUtil.scaleDownJobConfigToMaxResourceLimit(currentJobConfig, totalCpu * ratio, (int) (totalMem * ratio));
		}

		RescaleJobParallelism rescaleJobParallelism = new RescaleJobParallelism(jobID, timeout);
		for (JobVertexID vertexId : targetJobConfig.getVertexConfigs().keySet()) {
			RestServerClient.VertexConfig originVertexConfig = currentJobConfig.getVertexConfigs().get(vertexId);
			RestServerClient.VertexConfig adjustedVertexConfig = targetJobConfig.getVertexConfigs().get(vertexId);
			if (originVertexConfig.getParallelism() != adjustedVertexConfig.getParallelism()) {
				rescaleJobParallelism.addVertex(vertexId,
					originVertexConfig.getParallelism(),
					adjustedVertexConfig.getParallelism(),
					originVertexConfig.getResourceSpec(),
					adjustedVertexConfig.getResourceSpec());
			}
		}

		if (!rescaleJobParallelism.isEmpty()) {
			LOGGER.info("RescaleJobParallelism action generated: {}.", rescaleJobParallelism);
			return rescaleJobParallelism;
		}
		return null;
	}
}
