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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighStateSize;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatistics;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;

/**
 * Tests for high state size detector.
 */
public class HighStateSizeDetectorTest extends DetectorTestBase {

	@Test
	public void detect() throws Exception {

		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL, 1000L);
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_STATE_SIZE_THRESHOLD, 100L);

		// initial job vertex config.
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
				2, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
				2, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs.put(vertex1, vertex1Config1);
		vertexConfigs.put(vertex2, vertex2Config1);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, TaskCheckpointStatistics> checkpointInfo = new HashMap<>();
		checkpointInfo.put(vertex1,
				new TaskCheckpointStatistics(1, CheckpointStatsStatus.COMPLETED, 0,  200, 0, 0, 0, 0));
		checkpointInfo.put(vertex2,
				new TaskCheckpointStatistics(1, CheckpointStatsStatus.COMPLETED, 0,  201, 0, 0, 0, 0));
		Mockito.when(restClient.getJobVertexCheckPointStates(eq(jobID))).thenReturn(checkpointInfo);

		HighStateSizeDetector detector = new HighStateSizeDetector();
		detector.open(monitor);

		List<JobVertexID> expectedList = new LinkedList<>();
		expectedList.add(vertex2);
		assertEquals(expectedList, ((JobVertexHighStateSize) detector.detect()).getJobVertexIDs());

	}

}
