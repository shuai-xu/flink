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
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustResourceProfile;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOOM;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

/**
 * Heap Memory adjuster which can resolve vertex oom.
 */
public class HeapMemoryAdjuster implements Resolver {

	private static final ConfigOption<Double> HEAP_SCALE_OPTION =
			ConfigOptions.key("heap.memory.scale.ratio").defaultValue(0.5);

	private static final ConfigOption<Long> HEAP_SCALE_TIME_OUT_OPTION =
			ConfigOptions.key("heap.memory.scale.timeout.ms").defaultValue(180000L);

	private JobID jobID;
	private HealthMonitor monitor;
	private double scaleRatio;
	private long timeout;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.jobID = monitor.getJobID();
		this.scaleRatio = monitor.getConfig().getDouble(HEAP_SCALE_OPTION);
		this.timeout = monitor.getConfig().getLong(HEAP_SCALE_TIME_OUT_OPTION);
	}

	@Override
	public void close() {

	}

	@Override
	public Action resolve(List<Symptom> symptomList) {

		JobVertexID jobVertexID = null;
		for (Symptom symptom : symptomList) {
			if (symptom instanceof JobVertexOOM) {
				JobVertexOOM oomInfo = (JobVertexOOM) symptom;
				jobVertexID = oomInfo.getJobVertexID();
				break;
			}
		}
		if (jobVertexID != null) {
			RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
			RestServerClient.VertexConfig vertexConfig = jobConfig.getVertexConfigs().get(jobVertexID);
			ResourceSpec currentResource = vertexConfig.getResourceSpec();
			ResourceSpec targetResource =
					new ResourceSpec.Builder()
							.setHeapMemoryInMB((int) (currentResource.getHeapMemory() * scaleRatio))
							.build()
							.merge(currentResource);
			return new AdjustResourceProfile(
					jobID,
					jobVertexID,
					vertexConfig.getParallelism(),
					vertexConfig.getParallelism(),
					currentResource,
					targetResource,
					timeout);
		}
		return null;
	}
}
