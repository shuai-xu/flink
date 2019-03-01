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
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexTmKilledDueToMemoryExceed;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for KilledDueToMemoryExceedDetector.
 */
public class KilledDueToMemoryExceedDetectorTest extends DetectorTestBase {
	@Test
	public void testDetectKilledDueToMemoryExceed() throws Exception {
		String tmId1 = "tmId1";
		String tmId2 = "tmId2";
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(1024).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(1024).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<String, List<ExecutionVertexID>> taskManagerTasks = new HashMap<>();
		taskManagerTasks.put(tmId1, Lists.newArrayList(new ExecutionVertexID(vertex1, 0), new ExecutionVertexID(vertex1, 1)));
		taskManagerTasks.put(tmId2, Lists.newArrayList(new ExecutionVertexID(vertex2, 0), new ExecutionVertexID(vertex2, 1)));
		Mockito.when(restClient.getAllTaskManagerTasks()).thenReturn(taskManagerTasks);

		Map<String, List<Exception>> tmExceptions = new HashMap<>();
		tmExceptions.put(tmId1, Lists.newArrayList(new Exception("Container Killed due to memory exceeds 2 times")));
		Mockito.when(restClient.getTaskManagerExceptions(Mockito.anyLong(), Mockito.anyLong())).thenReturn(tmExceptions);

		KilledDueToMemoryExceedDetector killedDueToMemoryExceedDetector = new KilledDueToMemoryExceedDetector();
		killedDueToMemoryExceedDetector.open(monitor);

		assertNull(killedDueToMemoryExceedDetector.detect());
		Symptom symptom = killedDueToMemoryExceedDetector.detect();
		assertNotNull(symptom);
		assertTrue(symptom instanceof JobVertexTmKilledDueToMemoryExceed);
		JobVertexTmKilledDueToMemoryExceed jobVertexTmKilledDueToMemoryExceed = (JobVertexTmKilledDueToMemoryExceed) symptom;
		assertEquals(1, jobVertexTmKilledDueToMemoryExceed.getUtilities().size());
		assertEquals(Double.valueOf(2.0), jobVertexTmKilledDueToMemoryExceed.getUtilities().get(vertex1));
	}
}
