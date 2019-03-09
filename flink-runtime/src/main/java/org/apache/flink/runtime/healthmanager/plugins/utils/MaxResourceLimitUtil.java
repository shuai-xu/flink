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

package org.apache.flink.runtime.healthmanager.plugins.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Util for max resource limit.
 */
public class MaxResourceLimitUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxResourceLimitUtil.class);

	/**
	 * JM cpu cores.
	 * This option is a duplicate of YarnConfigOptions.JOB_APP_MASTER_CORE, to avoid
	 * circular dependency between flink-runtime and flink-yanr.
	 */
	public static final ConfigOption<Integer> JM_CPU = key("job.app-master-core").defaultValue(1);

	public static double getMaxCpu(Configuration config) {
		return config.getDouble(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_CPU_CORE)
			- config.getInteger(JM_CPU);
	}

	public static int getMaxMem(Configuration config) {
		return (int) (config.getInteger(ResourceManagerOptions.MAX_TOTAL_RESOURCE_LIMIT_MEMORY_MB)
			* (1 - config.getDouble(HealthMonitorOptions.FRAMEWORK_MEMORY_RATIO)));
	}

	public static RestServerClient.JobConfig scaleDownJobConfigToMaxResourceLimit(
		RestServerClient.JobConfig jobConfig,
		Map<JobVertexID, Integer> minParallelisms,
		double maxCpuLimit,
		int maxMemoryLimit) {

		if (maxCpuLimit <= 0.0 || maxMemoryLimit <= 0) {
			LOGGER.warn("Max resource limit <cpu, memory>=<{}, {}> could not be satisfied.", maxCpuLimit, maxMemoryLimit);
			return null;
		}

		RestServerClient.JobConfig adjustedJobConfig = new RestServerClient.JobConfig(jobConfig);
		boolean downScaled = true;

		while (adjustedJobConfig.getJobTotalCpuCores() > maxCpuLimit || adjustedJobConfig.getJobTotalMemoryMb() > maxMemoryLimit) {
			if (!downScaled) {
				LOGGER.warn("Max resource limit <cpu, memory>=<{}, {}> could not be satisfied.", maxCpuLimit, maxMemoryLimit);
				return null;
			}

			downScaled = false;

			double ratio = Math.min(maxCpuLimit / adjustedJobConfig.getJobTotalCpuCores(),
				((double) maxMemoryLimit) / adjustedJobConfig.getJobTotalMemoryMb());

			LOGGER.debug("Scaling down by ratio {}.", ratio);

			for (JobVertexID vertexId : adjustedJobConfig.getVertexConfigs().keySet()) {
				RestServerClient.VertexConfig originVertexConfig = adjustedJobConfig.getVertexConfigs().get(vertexId);

				int parallelism = (int) Math.floor(originVertexConfig.getParallelism() * ratio);
				parallelism = parallelism < 1 ? 1 : parallelism;
				if (parallelism < minParallelisms.getOrDefault(vertexId, 1)) {
					parallelism = minParallelisms.get(vertexId);
				}
				if (parallelism < originVertexConfig.getParallelism()) {
					downScaled = true;
				}

				RestServerClient.VertexConfig adjustedVertexConfig = new RestServerClient.VertexConfig(
					parallelism,
					originVertexConfig.getMaxParallelism(),
					originVertexConfig.getResourceSpec(),
					originVertexConfig.getOperatorIds(),
					originVertexConfig.getColocationGroupId()
				);

				adjustedJobConfig.getVertexConfigs().put(vertexId, adjustedVertexConfig);
			}
			LOGGER.debug("Resource after scaling down: <cpu, memory>=<{}, {}>.", adjustedJobConfig.getJobTotalCpuCores(), adjustedJobConfig.getJobTotalMemoryMb());
		}

		return adjustedJobConfig;
	}
}
