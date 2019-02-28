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
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDirectOOM;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for direct memory adjuster.
 */
public class DirectMemoryAdjusterTest {
	@Test
	public void testDiagnose() {
		DirectMemoryAdjuster directMemoryAdjuster = new DirectMemoryAdjuster();
		Whitebox.setInternalState(directMemoryAdjuster, "stableTime", 10);

		JobStable jobUnStable = JobStable.UNSTABLE;
		JobStable jobStableShortTime = new JobStable(5L);
		JobStable jobStableLongTime = new JobStable(15L);
		JobVertexDirectOOM jobVertexDirectOOM = new JobVertexDirectOOM(new JobID(), Collections.emptyList());
		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(new JobID());

		List<Symptom> symptomList = new ArrayList<>();

		symptomList.add(jobVertexDirectOOM);
		assertTrue(directMemoryAdjuster.diagnose(symptomList));

		symptomList.clear();
		symptomList.add(jobStableLongTime);
		assertFalse(directMemoryAdjuster.diagnose(symptomList));

		symptomList.clear();
		symptomList.add(jobVertexLowMemory);
		assertFalse(directMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobUnStable);
		assertFalse(directMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobUnStable);
		symptomList.add(jobStableShortTime);
		assertFalse(directMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobStableShortTime);
		symptomList.add(jobStableLongTime);
		assertTrue(directMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexDirectOOM);
		assertTrue(directMemoryAdjuster.diagnose(symptomList));

		assertEquals(jobStableLongTime, Whitebox.getInternalState(directMemoryAdjuster, "jobStable"));
		assertEquals(jobVertexDirectOOM, Whitebox.getInternalState(directMemoryAdjuster, "jobVertexDirectOOM"));
		assertEquals(jobVertexLowMemory, Whitebox.getInternalState(directMemoryAdjuster, "jobVertexLowMemory"));
	}

	@Test
	public void testScaleUpVertexDirectMem() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		JobVertexDirectOOM jobVertexDirectOOM = new JobVertexDirectOOM(new JobID(), Lists.newArrayList(vertex1));

		DirectMemoryAdjuster directMemoryAdjuster = new DirectMemoryAdjuster();
		Whitebox.setInternalState(directMemoryAdjuster, "scaleUpRatio", 1.5);
		Whitebox.setInternalState(directMemoryAdjuster, "jobVertexDirectOOM", jobVertexDirectOOM);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setDirectMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setDirectMemoryInMB(100).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Integer> results = directMemoryAdjuster.scaleUpVertexDirectMem(jobConfig);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(Integer.valueOf(150), results.get(vertex1));
	}

	@Test
	public void testScaleDownVertexDirectMem() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(new JobID());
		jobVertexLowMemory.addVertex(vertex1, 0.0, 0.5, 0.0);

		DirectMemoryAdjuster directMemoryAdjuster = new DirectMemoryAdjuster();
		Whitebox.setInternalState(directMemoryAdjuster, "scaleDownRatio", 1.0);
		Whitebox.setInternalState(directMemoryAdjuster, "jobVertexLowMemory", jobVertexLowMemory);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setDirectMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setDirectMemoryInMB(100).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Integer> results = directMemoryAdjuster.scaleDownVertexDirectMem(jobConfig);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(Integer.valueOf(50), results.get(vertex1));
	}
}
