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

package org.apache.flink.runtime.healthmanager.plugins.detectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexTmKilledDueToMemoryExceed;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * KilledDueToMemoryExceedDetector detects task manager killed due to memory exceed.
 */
public class KilledDueToMemoryExceedDetector implements Detector {
	private static final Logger LOGGER = LoggerFactory.getLogger(KilledDueToMemoryExceedDetector.class);
	private static final String TARGET_ERR_MSG = "Container Killed due to memory exceeds ";

	private JobID jobID;
	private HealthMonitor monitor;
	private RestServerClient restServerClient;

	private long lastDetectTime;
	private Map<String, List<JobVertexID>> tmTasks;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();

		lastDetectTime = System.currentTimeMillis();
		tmTasks = new HashMap<>();
	}

	@Override
	public void close() {

	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");
		long now = System.currentTimeMillis();

		Map<String, List<Exception>> tmExceptions = restServerClient.getTaskManagerExceptions(lastDetectTime, now);
		lastDetectTime = now;

		JobVertexTmKilledDueToMemoryExceed jobVertexTmKilledDueToMemoryExceed = null;
		if (tmExceptions != null) {
			RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
			Map<JobVertexID, Double> vertexMaxUtilities = new HashMap<>();
			for (Map.Entry<String, List<Exception>> entry : tmExceptions.entrySet()) {
				String tmId = entry.getKey();
				for (Exception exception : entry.getValue()) {
					if (!exception.getMessage().contains(TARGET_ERR_MSG)) {
						continue;
					}

					double exceedTime = getExceedTime(exception.getLocalizedMessage());
					List<JobVertexID> vertices = tmTasks.get(tmId);

					LOGGER.debug("TM {} with tasks {} killed due to memory exceed {} times.",
						tmId, vertices, exceedTime);

					if (vertices == null) {
						continue;
					}

					for (JobVertexID vertexID : vertices) {
						ResourceSpec currentResource = jobConfig.getVertexConfigs().get(vertexID).getResourceSpec();
						double usage = (currentResource.getHeapMemory() + currentResource.getDirectMemory() + currentResource.getNativeMemory()) * exceedTime;
						double capacity = currentResource.getNativeMemory();
						if (capacity == 0.0) {
							capacity = 1.0;
						}
						double utility = usage / capacity;
						if (!vertexMaxUtilities.containsKey(vertexID) || utility > vertexMaxUtilities.get(vertexID)) {
							vertexMaxUtilities.put(vertexID, utility);
						}
					}
				}
			}

			if (!vertexMaxUtilities.isEmpty()) {
				LOGGER.info("TM killed due to memory exceed detected for vertices with max utility {}.", vertexMaxUtilities);
				jobVertexTmKilledDueToMemoryExceed = new JobVertexTmKilledDueToMemoryExceed(jobID, vertexMaxUtilities);
			}
		}

		updateTmTasks();
		return jobVertexTmKilledDueToMemoryExceed;
	}

	private void updateTmTasks() {
		for (Map.Entry<String, List<ExecutionVertexID>> entry : restServerClient.getAllTaskManagerTasks().entrySet()) {
			tmTasks.put(entry.getKey(),
				entry.getValue().stream().map(executionVertexID -> executionVertexID.getJobVertexID()).collect(Collectors.toList()));
		}
	}

	private double getExceedTime(String msg) {
		msg = msg.substring(msg.indexOf(TARGET_ERR_MSG) + TARGET_ERR_MSG.length());
		return Double.valueOf(msg.split(" ")[0]);
	}
}
