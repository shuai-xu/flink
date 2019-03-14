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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighCpu;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowCpu;
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
* Tests for cpu adjuster.
 */
public class CpuAdjusterTest {
	@Test
	public void testDiagnose() {
		CpuAdjuster cpuAdjuster = new CpuAdjuster();
		Whitebox.setInternalState(cpuAdjuster, "stableTime", 10);

		JobStable jobUnStable = JobStable.UNSTABLE;
		JobStable jobStableShortTime = new JobStable(5L);
		JobStable jobStableLongTime = new JobStable(15L);
		JobVertexHighCpu jobVertexHighCpu = new JobVertexHighCpu(new JobID(), Collections.emptyMap(), false);
		JobVertexLowCpu jobVertexLowCpu = new JobVertexLowCpu(new JobID(), Collections.emptyMap());
		JobVertexFrequentFullGC jobVertexFrequentFullGC = new JobVertexFrequentFullGC(new JobID(), Collections.emptyList(), false);
		JobVertexFrequentFullGC jobVertexFrequentFullGCSevere = new JobVertexFrequentFullGC(new JobID(), Collections.emptyList(), true);
		JobVertexLongTimeFullGC jobVertexLongTimeFullGC = new JobVertexLongTimeFullGC(new JobID(), Collections.emptyList(), false, false);
		JobVertexLongTimeFullGC jobVertexLongTimeFullGCSevere = new JobVertexLongTimeFullGC(new JobID(), Collections.emptyList(), true, false);

		List<Symptom> symptomList = new ArrayList<>();

		symptomList.add(jobVertexHighCpu);
		symptomList.add(jobVertexLowCpu);
		assertFalse(cpuAdjuster.diagnose(symptomList));

		symptomList.add(jobUnStable);
		assertFalse(cpuAdjuster.diagnose(symptomList));

		symptomList.remove(jobUnStable);
		symptomList.add(jobStableShortTime);
		assertFalse(cpuAdjuster.diagnose(symptomList));

		symptomList.clear();

		symptomList.add(jobStableLongTime);
		assertFalse(cpuAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexHighCpu);
		assertTrue(cpuAdjuster.diagnose(symptomList));

		symptomList.remove(jobVertexHighCpu);
		symptomList.add(jobVertexLowCpu);
		assertTrue(cpuAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexHighCpu);
		assertTrue(cpuAdjuster.diagnose(symptomList));

		symptomList.add(jobVertexFrequentFullGCSevere);
		assertFalse(cpuAdjuster.diagnose(symptomList));

		symptomList.remove(jobVertexFrequentFullGCSevere);
		symptomList.add(jobVertexLongTimeFullGCSevere);
		assertFalse(cpuAdjuster.diagnose(symptomList));

		symptomList.remove(jobVertexLongTimeFullGCSevere);
		symptomList.add(jobVertexFrequentFullGC);
		symptomList.add(jobVertexLongTimeFullGC);
		assertTrue(cpuAdjuster.diagnose(symptomList));

		assertEquals(jobStableLongTime, Whitebox.getInternalState(cpuAdjuster, "jobStable"));
		assertEquals(jobVertexHighCpu, Whitebox.getInternalState(cpuAdjuster, "jobVertexHighCpu"));
		assertEquals(jobVertexLowCpu, Whitebox.getInternalState(cpuAdjuster, "jobVertexLowCpu"));
		assertEquals(jobVertexFrequentFullGC, Whitebox.getInternalState(cpuAdjuster, "jobVertexFrequentFullGC"));
		assertEquals(jobVertexLongTimeFullGC, Whitebox.getInternalState(cpuAdjuster, "jobVertexLongTimeFullGC"));
	}

	@Test
	public void testScaleUpVertexCpu() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		Map<JobVertexID, Double> currentUtilities = new HashMap<>();
		currentUtilities.put(vertex1, 1.5);
		currentUtilities.put(vertex2, 1.5);
		JobVertexHighCpu jobVertexHighCpu = new JobVertexHighCpu(new JobID(), currentUtilities, false);

		Map<JobVertexID, Double> previousUtilities = new HashMap<>();
		previousUtilities.put(vertex2, 2.0);

		CpuAdjuster cpuAdjuster = new CpuAdjuster();
		Whitebox.setInternalState(cpuAdjuster, "jobVertexHighCpu", jobVertexHighCpu);
		Whitebox.setInternalState(cpuAdjuster, "vertexToScaleUpMaxUtility", previousUtilities);
		Whitebox.setInternalState(cpuAdjuster, "scaleUpRatio", 1.0);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Double> results = cpuAdjuster.scaleUpVertexCpu(jobConfig);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(Double.valueOf(1.5), results.get(vertex1));
		assertEquals(Double.valueOf(2.0), results.get(vertex2));
	}

	@Test
	public void testScaleDownVertexCpu() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		Map<JobVertexID, Double> currentUtilities = new HashMap<>();
		currentUtilities.put(vertex1, 0.5);
		JobVertexLowCpu jobVertexLowCpu = new JobVertexLowCpu(new JobID(), currentUtilities);

		CpuAdjuster cpuAdjuster = new CpuAdjuster();
		Whitebox.setInternalState(cpuAdjuster, "jobVertexLowCpu", jobVertexLowCpu);
		Whitebox.setInternalState(cpuAdjuster, "scaleDownRatio", 1.0);

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build());
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Map<JobVertexID, Double> results = cpuAdjuster.scaleDownVertexCpu(jobConfig);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(Double.valueOf(0.5), results.get(vertex1));
	}
}
