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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDirectOOM;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * DirectOOMDetector detects direct oom failure of a job.
 */
public class DirectOOMDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(DirectOOMDetector.class);

	private JobID jobID;
	private RestServerClient restServerClient;

	private long lastDetectTime;

	@Override
	public void open(HealthMonitor monitor) {

		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();

		lastDetectTime = System.currentTimeMillis();
	}

	@Override
	public void close() {

	}

	@Override
	public Symptom detect() throws Exception {
		LOGGER.debug("Start detecting.");

		long now = System.currentTimeMillis();

		Map<JobVertexID, List<JobException>> exceptions = restServerClient.getFailover(jobID, lastDetectTime, now);

		lastDetectTime = now;
		if (exceptions == null) {
			return null;
		}

		List<JobVertexID> jobVertexIDs = new LinkedList<>();
		for (JobVertexID vertexID: exceptions.keySet()) {
			for (JobException exception : exceptions.get(vertexID)) {
				if (exception.getMessage().contains("Direct buffer memory")) {
					jobVertexIDs.add(vertexID);
					break;
				}
			}
		}
		if (!jobVertexIDs.isEmpty()) {
			LOGGER.info("Direct OOM detected for vertices {}.", jobVertexIDs);
			return new JobVertexDirectOOM(jobID, jobVertexIDs);
		}
		return null;
	}
}
