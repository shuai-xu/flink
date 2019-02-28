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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighNativeMemory;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.jobgraph.JobVertexID;

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
 * Tests for native memory adjuster.
 */
public class NativeMemoryAdjusterTest {
	@Test
	public void testDiagnose() {
		NativeMemoryAdjuster nativeMemoryAdjuster = new NativeMemoryAdjuster();
		Whitebox.setInternalState(nativeMemoryAdjuster, "stableTime", 10);

		JobStable jobUnStable = JobStable.UNSTABLE;
		JobStable jobStableShortTime = new JobStable(5L);
		JobStable jobStableLongTime = new JobStable(15L);
		JobVertexHighNativeMemory jobVertexHighNativeMemory = new JobVertexHighNativeMemory(
			new JobID(), Collections.emptyMap(), true, false);
		JobVertexHighNativeMemory jobVertexHighNativeMemoryCritical = new JobVertexHighNativeMemory(
			new JobID(), Collections.emptyMap(), true, true);
		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(new JobID());

		List<Symptom> symptomList = new ArrayList<>();

		symptomList.add(jobVertexHighNativeMemoryCritical);
		assertTrue(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.clear();
		symptomList.add(jobStableLongTime);
		assertFalse(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexHighNativeMemory);
		assertTrue(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobVertexHighNativeMemory);
		symptomList.add(jobVertexLowMemory);
		assertTrue(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.clear();
		symptomList.add(jobVertexHighNativeMemory);
		symptomList.add(jobVertexLowMemory);
		assertFalse(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobUnStable);
		assertFalse(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobUnStable);
		symptomList.add(jobStableShortTime);
		assertFalse(nativeMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobStableShortTime);
		symptomList.add(jobStableLongTime);
		assertTrue(nativeMemoryAdjuster.diagnose(symptomList));

		assertEquals(jobStableLongTime, Whitebox.getInternalState(nativeMemoryAdjuster, "jobStable"));
		assertEquals(jobVertexHighNativeMemory, Whitebox.getInternalState(nativeMemoryAdjuster, "jobVertexHighNativeMemory"));
		assertEquals(jobVertexLowMemory, Whitebox.getInternalState(nativeMemoryAdjuster, "jobVertexLowMemory"));
	}

	@Test
	public void testScaleUpVertexNativeMem() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();

		Map<JobVertexID, Double> currentMaxUtility = new HashMap<>();
		currentMaxUtility.put(vertex1, 1.5);
		currentMaxUtility.put(vertex2, 1.2);
		JobVertexHighNativeMemory jobVertexHighNativeMemory = new JobVertexHighNativeMemory(
			new JobID(), currentMaxUtility, false, false);

		Map<JobVertexID, Double> previousMaxUtility = new HashMap<>();
		previousMaxUtility.put(vertex2, 1.5);
		previousMaxUtility.put(vertex3, 1.5);

		NativeMemoryAdjuster nativeMemoryAdjuster = new NativeMemoryAdjuster();
		Whitebox.setInternalState(nativeMemoryAdjuster, "scaleUpRatio", 1.0);
		Whitebox.setInternalState(nativeMemoryAdjuster, "jobVertexHighNativeMemory", jobVertexHighNativeMemory);
		Whitebox.setInternalState(nativeMemoryAdjuster, "vertexToScaleUpMaxUtilities", previousMaxUtility);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig3 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(100).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		vertexConfigs.put(vertex3, vertexConfig3);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Integer> results = nativeMemoryAdjuster.scaleUpVertexNativeMemory(jobConfig);
		assertNotNull(results);
		assertEquals(3, results.size());
		assertEquals(Integer.valueOf(150), results.get(vertex1));
		assertEquals(Integer.valueOf(150), results.get(vertex2));
		assertEquals(Integer.valueOf(150), results.get(vertex3));
	}

	@Test
	public void testScaleDownVertexNativeMem() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(new JobID());
		jobVertexLowMemory.addVertex(vertex1, 0.1, 0.0, 0.5);
		jobVertexLowMemory.addVertex(vertex2, 0.1, 0.0, 1.5);

		NativeMemoryAdjuster nativeMemoryAdjuster = new NativeMemoryAdjuster();
		Whitebox.setInternalState(nativeMemoryAdjuster, "scaleDownRatio", 1.0);
		Whitebox.setInternalState(nativeMemoryAdjuster, "jobVertexLowMemory", jobVertexLowMemory);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(100).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Integer> results = nativeMemoryAdjuster.scaleDownVertexNativeMemory(jobConfig);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(Integer.valueOf(50), results.get(vertex1));
		assertEquals(Integer.valueOf(150), results.get(vertex2));
	}
}
