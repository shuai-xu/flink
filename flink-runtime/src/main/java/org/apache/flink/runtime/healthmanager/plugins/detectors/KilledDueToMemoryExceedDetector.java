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
	private static final String ERR_MSG_MEMORY_EXCEED_YARN_V3 = "Container Killed due to memory exceeds ";
	private static final String ERR_MSG_MEMORY_EXCEED_YARN_V2 = "is running beyond physical memory limits. Current usage: ";
	private static final String ERR_MSG_MACHINE_MEMORY_HEAVY_YARN_V3 = "QosContainersMonitor killing, reason: machine memory is too heavy";

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

		if (lastDetectTime < monitor.getJobStartExecutionTime()) {
			lastDetectTime = monitor.getJobStartExecutionTime();
		}

		Map<String, List<Exception>> tmExceptions = restServerClient.getTaskManagerExceptions(lastDetectTime, now);
		lastDetectTime = now;

		JobVertexTmKilledDueToMemoryExceed jobVertexTmKilledDueToMemoryExceed = null;
		if (tmExceptions != null) {
			RestServerClient.JobConfig jobConfig = monitor.getJobConfig();
			Map<JobVertexID, Double> vertexMaxUtilities = new HashMap<>();
			for (Map.Entry<String, List<Exception>> entry : tmExceptions.entrySet()) {
				String tmId = entry.getKey();
				for (Exception exception : entry.getValue()) {
					double exceedTime = getExceedTime(exception.getLocalizedMessage());
					if (exceedTime < 0.0) {
						continue;
					}
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

		if (msg.contains(ERR_MSG_MEMORY_EXCEED_YARN_V3)) {
			msg = msg.substring(msg.indexOf(ERR_MSG_MEMORY_EXCEED_YARN_V3) + ERR_MSG_MEMORY_EXCEED_YARN_V3.length());
			return Double.valueOf(msg.split(" ")[0]);
		}

		if (msg.contains(ERR_MSG_MACHINE_MEMORY_HEAVY_YARN_V3)) {
			return 1.0;
		}

		if (msg.contains(ERR_MSG_MEMORY_EXCEED_YARN_V2)) {
			msg = msg.substring(msg.indexOf(ERR_MSG_MEMORY_EXCEED_YARN_V2) + ERR_MSG_MEMORY_EXCEED_YARN_V2.length());
			String[] tokens = msg.split(" ");

			int unit;

			double usage = Double.valueOf(tokens[0]);
			switch (tokens[1].charAt(0)) {
				case 'E' : unit = 6; break;
				case 'P' : unit = 5; break;
				case 'T' : unit = 4; break;
				case 'G' : unit = 3; break;
				case 'M' : unit = 2; break;
				case 'K' : unit = 1; break;
				default: unit = 0;
			}
			while (unit-- > 0) {
				usage *= 1024;
			}

			double capacity = Double.valueOf(tokens[3]);
			switch (tokens[4].charAt(0)) {
				case 'E' : unit = 6; break;
				case 'P' : unit = 5; break;
				case 'T' : unit = 4; break;
				case 'G' : unit = 3; break;
				case 'M' : unit = 2; break;
				case 'K' : unit = 1; break;
				default: unit = 0;
			}
			while (unit-- > 0) {
				capacity *= 1024;
			}

			return usage / capacity;
		}

		return -1.0;
	}
}
