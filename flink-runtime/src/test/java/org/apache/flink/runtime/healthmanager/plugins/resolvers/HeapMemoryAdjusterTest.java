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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHeapOOM;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for heap memory adjuster.
 */
public class HeapMemoryAdjusterTest {
	@Test
	public void testDiagnose() {
		HeapMemoryAdjuster heapMemoryAdjuster = new HeapMemoryAdjuster();
		Whitebox.setInternalState(heapMemoryAdjuster, "stableTime", 10);

		JobStable jobUnStable = JobStable.UNSTABLE;
		JobStable jobStableShortTime = new JobStable(5L);
		JobStable jobStableLongTime = new JobStable(15L);
		JobVertexHeapOOM jobVertexHeapOOM = new JobVertexHeapOOM(new JobID(), Collections.emptyList());
		JobVertexFrequentFullGC jobVertexFrequentFullGC = new JobVertexFrequentFullGC(new JobID(),
			Collections.emptyList(), false);
		JobVertexLongTimeFullGC jobVertexLongTimeFullGC = new JobVertexLongTimeFullGC(new JobID(),
			Collections.emptyList(), false, false);
		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(new JobID());

		List<Symptom> symptomList = new ArrayList<>();

		symptomList.add(jobVertexHeapOOM);
		assertTrue(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.clear();
		symptomList.add(jobStableLongTime);
		assertFalse(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexFrequentFullGC);
		assertTrue(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobVertexFrequentFullGC);
		symptomList.add(jobVertexLongTimeFullGC);
		assertTrue(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobVertexLongTimeFullGC);
		symptomList.add(jobVertexLowMemory);
		assertTrue(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.clear();
		symptomList.add(jobVertexFrequentFullGC);
		symptomList.add(jobVertexLongTimeFullGC);
		symptomList.add(jobVertexLowMemory);
		assertFalse(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobUnStable);
		assertFalse(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobUnStable);
		symptomList.add(jobStableShortTime);
		assertFalse(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.remove(jobStableShortTime);
		symptomList.add(jobStableLongTime);
		assertTrue(heapMemoryAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexHeapOOM);
		assertTrue(heapMemoryAdjuster.diagnose(symptomList));

		assertEquals(jobStableLongTime, Whitebox.getInternalState(heapMemoryAdjuster, "jobStable"));
		assertEquals(jobVertexHeapOOM, Whitebox.getInternalState(heapMemoryAdjuster, "jobVertexHeapOOM"));
		assertEquals(jobVertexFrequentFullGC, Whitebox.getInternalState(heapMemoryAdjuster, "jobVertexFrequentFullGC"));
		assertEquals(jobVertexLongTimeFullGC, Whitebox.getInternalState(heapMemoryAdjuster, "jobVertexLongTimeFullGC"));
		assertEquals(jobVertexLowMemory, Whitebox.getInternalState(heapMemoryAdjuster, "jobVertexLowMemory"));
	}

	@Test
	public void testScaleUpVertexHeapMem() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();
		JobVertexID vertex4 = new JobVertexID();

		JobVertexHeapOOM jobVertexHeapOOM = new JobVertexHeapOOM(new JobID(), Lists.newArrayList(vertex1));
		JobVertexFrequentFullGC jobVertexFrequentFullGC = new JobVertexFrequentFullGC(
			new JobID(), Lists.newArrayList(vertex2), false);
		JobVertexLongTimeFullGC jobVertexLongTimeFullGC = new JobVertexLongTimeFullGC(
			new JobID(), Lists.newArrayList(vertex3), false, false);

		Set<JobVertexID> vertexToScaleUp = new HashSet<>();
		vertexToScaleUp.add(vertex3);
		vertexToScaleUp.add(vertex4);

		HeapMemoryAdjuster heapMemoryAdjuster = new HeapMemoryAdjuster();
		Whitebox.setInternalState(heapMemoryAdjuster, "scaleUpRatio", 1.5);
		Whitebox.setInternalState(heapMemoryAdjuster, "jobVertexHeapOOM", jobVertexHeapOOM);
		Whitebox.setInternalState(heapMemoryAdjuster, "jobVertexFrequentFullGC", jobVertexFrequentFullGC);
		Whitebox.setInternalState(heapMemoryAdjuster, "jobVertexLongTimeFullGC", jobVertexLongTimeFullGC);
		Whitebox.setInternalState(heapMemoryAdjuster, "vertexToScaleUp", vertexToScaleUp);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig3 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig4 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(100).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		vertexConfigs.put(vertex3, vertexConfig3);
		vertexConfigs.put(vertex4, vertexConfig4);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Integer> results = heapMemoryAdjuster.scaleUpVertexHeapMem(jobConfig);
		assertNotNull(results);
		assertEquals(4, results.size());
		assertEquals(Integer.valueOf(150), results.get(vertex1));
		assertEquals(Integer.valueOf(150), results.get(vertex2));
		assertEquals(Integer.valueOf(150), results.get(vertex3));
		assertEquals(Integer.valueOf(150), results.get(vertex4));
	}

	@Test
	public void testScaleDownVertexHeapMem() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		JobVertexLowMemory jobVertexLowMemory = new JobVertexLowMemory(new JobID());
		jobVertexLowMemory.addVertex(vertex1, 0.5, 0.0, 0.0);

		HeapMemoryAdjuster heapMemoryAdjuster = new HeapMemoryAdjuster();
		Whitebox.setInternalState(heapMemoryAdjuster, "scaleDownRatio", 1.0);
		Whitebox.setInternalState(heapMemoryAdjuster, "jobVertexLowMemory", jobVertexLowMemory);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(100).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(100).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Integer> results = heapMemoryAdjuster.scaleDownVertexHeapMem(jobConfig);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(Integer.valueOf(50), results.get(vertex1));
	}
}
