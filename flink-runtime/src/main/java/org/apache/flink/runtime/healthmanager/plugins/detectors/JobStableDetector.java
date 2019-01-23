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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobUnstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobStableDetector generate an JobStable symptom when all task running keep running for a given period.
 */
public class JobStableDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobStableDetector.class);

	private static final ConfigOption<Long> JOB_STABLE_THRESHOLD =
			ConfigOptions.key("healthmonitor.job.stable.threshold.ms").defaultValue(3 * 60 * 1000L);

	private HealthMonitor monitor;

	private long threshold;

	@Override
	public void open(HealthMonitor monitor) {
		this.monitor = monitor;
		this.threshold = monitor.getConfig().getLong(JOB_STABLE_THRESHOLD);
	}

	@Override
	public void close() {

	}

	@Override
	public Symptom detect() throws Exception {
		RestServerClient.JobStatus status = this.monitor.getRestServerClient().getJobStatus(this.monitor.getJobID());

		long now = System.currentTimeMillis();
		for (Tuple2<Long, ExecutionState> state: status.getTaskStatus().values()) {
			if (!state.f1.equals(ExecutionState.RUNNING)) {
				LOGGER.debug("Some task not running yet!");
				return JobUnstable.INSTANCE;
			}
			if (now - state.f0 < threshold) {
				LOGGER.debug("Some task not stable yet!");
				return JobUnstable.INSTANCE;
			}
		}
		return JobStable.INSTANCE;
	}
}
