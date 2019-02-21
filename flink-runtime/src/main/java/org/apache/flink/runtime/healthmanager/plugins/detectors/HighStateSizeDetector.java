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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighStateSize;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Detector which check whether the state size of a job vertex exceed given threshold.
 */
public class HighStateSizeDetector implements Detector {

	private static final Logger LOGGER = LoggerFactory.getLogger(HighStateSizeDetector.class);

	public static final ConfigOption<Long> STATE_SIZE_THRESHOLD =
			ConfigOptions.key("healthmonitor.state.size.threshold").defaultValue(10L * 1024 * 1024 * 1024);

	private HealthMonitor healthMonitor;

	private long threshold;

	@Override
	public void open(HealthMonitor monitor) {
		healthMonitor = monitor;
		threshold = monitor.getConfig().getLong(STATE_SIZE_THRESHOLD);
	}

	@Override
	public void close() {
	}

	@Override
	public Symptom detect() throws Exception {
		List<JobVertexID> highStateSizeVertices = new LinkedList<>();
		Map<JobVertexID, TaskCheckpointStatistics> checkpointInfo = healthMonitor.getRestServerClient().getJobVertexCheckPointStates(healthMonitor.getJobID());
		for (Map.Entry<JobVertexID, TaskCheckpointStatistics> entry: checkpointInfo.entrySet()) {
			if (entry.getValue().getStateSize() > threshold) {
				LOGGER.debug("vertex {} state size [{}] reach threshold.", entry.getKey(), entry.getValue().getStateSize());
				highStateSizeVertices.add(entry.getKey());
			}
		}
		if (highStateSizeVertices.isEmpty()) {
			return null;
		} else {
			return new JobVertexHighStateSize(healthMonitor.getJobID(), highStateSizeVertices);
		}
	}
}
