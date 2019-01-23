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
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobExceedMaxResourceLimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * ExceedMaxResourceLimitDetector detects job and framework
 * resource need exceed max limit.
 */
public class ExceedMaxResourceLimitDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExceedMaxResourceLimitDetector.class);

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

		JobExceedMaxResourceLimit jobExceedMaxResourceLimit = null;
		Map<Long, Exception> exceptions = restServerClient.getTotalResourceLimitExceptions();
		for (Map.Entry<Long, Exception> entry : exceptions.entrySet()) {
			if (entry.getKey() < lastDetectTime) {
				continue;
			}

			jobExceedMaxResourceLimit = new JobExceedMaxResourceLimit(jobID);
			break;
		}

		lastDetectTime = System.currentTimeMillis();

		if (jobExceedMaxResourceLimit != null) {
			LOGGER.info("Direct job exceed max resource limit.");
			return jobExceedMaxResourceLimit;
		}
		return null;
	}
}
