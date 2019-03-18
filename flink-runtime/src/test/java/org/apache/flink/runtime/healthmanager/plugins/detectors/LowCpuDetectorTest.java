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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.JobTMMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowCpu;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for LowCpuDetector.
 */
public class LowCpuDetectorTest extends DetectorTestBase {

	/**
	 * Test detect low cpu.
	 * Cpu remain low in 2 or more consecutive will be detected.
	 * vertex1 (tm1): Low, Low, Low, High
	 * vertex2 (tm2): Low, High, Low, Low
	 * detection: (), (1), (1), (2)
	 * @throws Exception
	 */
	@Test
	public void testDetectLowCpu() throws Exception {
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_DOWN_WAIT_TIME, 0);
		config.setDouble(LowCpuDetector.LOW_CPU_THRESHOLD, 0.6);

		String tmId1 = "tmId1";
		String tmId2 = "tmId2";
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId1))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex1, 0), new ExecutionVertexID(vertex1, 1)));
		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId2))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex2, 0), new ExecutionVertexID(vertex2, 1)));

		// set vertex config

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build()));
		vertexConfigs.put(vertex2, new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build()));
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		// set capacity

		Map<String, Tuple2<Long, Double>> capacities = new HashMap<>();
		capacities.put(tmId1, new Tuple2<>(0L, 1.0));
		capacities.put(tmId2, new Tuple2<>(0L, 1.0));

		JobTMMetricSubscription capacitySub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(capacitySub.getValue()).thenReturn(capacities);
		Mockito.when(metricProvider.subscribeAllTMMetric(
				Mockito.eq(jobID),
				Mockito.eq(MetricNames.TM_CPU_CAPACITY),
				Mockito.anyLong(),
				Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(capacitySub);

		// set usage

		Map<String, Tuple2<Long, Double>> usages1 = new HashMap<>();
		usages1.put(tmId1, new Tuple2<>(0L, 0.5));
		usages1.put(tmId2, new Tuple2<>(0L, 0.5));

		Map<String, Tuple2<Long, Double>> usages2 = new HashMap<>();
		usages2.put(tmId1, new Tuple2<>(0L, 0.5));
		usages2.put(tmId2, new Tuple2<>(0L, 0.8));

		Map<String, Tuple2<Long, Double>> usages3 = new HashMap<>();
		usages3.put(tmId1, new Tuple2<>(0L, 0.5));
		usages3.put(tmId2, new Tuple2<>(0L, 0.5));

		Map<String, Tuple2<Long, Double>> usages4 = new HashMap<>();
		usages4.put(tmId1, new Tuple2<>(0L, 0.8));
		usages4.put(tmId2, new Tuple2<>(0L, 0.5));

		JobTMMetricSubscription usageSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(usageSub.getValue()).thenReturn(usages1).thenReturn(usages2).thenReturn(usages3).thenReturn(usages4);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_CPU_USAGE),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(usageSub);

		// verify detections

		LowCpuDetector lowCpuDetector = new LowCpuDetector();
		lowCpuDetector.open(monitor);

		Symptom symptom1 = lowCpuDetector.detect();
		assertNull(symptom1);

		Symptom symptom2 = lowCpuDetector.detect();
		assertNotNull(symptom2);
		assertTrue(symptom2 instanceof JobVertexLowCpu);
		Set<JobVertexID> vertices2 = ((JobVertexLowCpu) symptom2).getUtilities().keySet();
		assertEquals(1, vertices2.size());
		assertTrue(vertices2.contains(vertex1));

		Symptom symptom3 = lowCpuDetector.detect();
		assertNotNull(symptom3);
		assertTrue(symptom3 instanceof JobVertexLowCpu);
		Set<JobVertexID> vertices3 = ((JobVertexLowCpu) symptom3).getUtilities().keySet();
		assertEquals(1, vertices3.size());
		assertTrue(vertices3.contains(vertex1));

		Symptom symptom4 = lowCpuDetector.detect();
		assertNotNull(symptom4);
		assertTrue(symptom4 instanceof JobVertexLowCpu);
		Set<JobVertexID> vertices4 = ((JobVertexLowCpu) symptom4).getUtilities().keySet();
		assertEquals(1, vertices4.size());
		assertTrue(vertices4.contains(vertex2));
	}

	/**
	 * Test detect when vertex config is updated.
	 * Cpu remain low in 2 or more consecutive will be detected.
	 * vertex1 (tm1): Low, Low
	 * vertex2 (tm2): Low, High (config updated)
	 * detection: (), (1)
	 * @throws Exception
	 */
	@Test
	public void testDetectConfigUpdated() throws Exception {
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_DOWN_WAIT_TIME, 0);
		config.setDouble(LowCpuDetector.LOW_CPU_THRESHOLD, 0.6);

		String tmId1 = "tmId1";
		String tmId2 = "tmId2";
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId1))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex1, 0), new ExecutionVertexID(vertex1, 1)));
		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId2))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex2, 0), new ExecutionVertexID(vertex2, 1)));

		// set vertex config

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs1 = new HashMap<>();
		vertexConfigs1.put(vertex1, new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build()));
		vertexConfigs1.put(vertex2, new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build()));

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs2 = new HashMap<>();
		vertexConfigs2.put(vertex1, new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(1.0).build()));
		vertexConfigs2.put(vertex2, new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setCpuCores(0.5).build()));

		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs1);

		// set capacity

		Map<String, Tuple2<Long, Double>> capacities = new HashMap<>();
		capacities.put(tmId1, new Tuple2<>(0L, 1.0));
		capacities.put(tmId2, new Tuple2<>(0L, 1.0));

		JobTMMetricSubscription capacitySub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(capacitySub.getValue()).thenReturn(capacities);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_CPU_CAPACITY),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(capacitySub);

		// set usage

		Map<String, Tuple2<Long, Double>> usages1 = new HashMap<>();
		usages1.put(tmId1, new Tuple2<>(0L, 0.5));
		usages1.put(tmId2, new Tuple2<>(0L, 0.5));

		Map<String, Tuple2<Long, Double>> usages2 = new HashMap<>();
		usages2.put(tmId1, new Tuple2<>(0L, 0.5));
		usages2.put(tmId2, new Tuple2<>(0L, 0.5));

		JobTMMetricSubscription usageSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(usageSub.getValue()).thenReturn(usages1).thenReturn(usages2);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_CPU_USAGE),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(usageSub);

		// verify detections

		LowCpuDetector lowCpuDetector = new LowCpuDetector();
		lowCpuDetector.open(monitor);

		Symptom symptom1 = lowCpuDetector.detect();
		assertNull(symptom1);

		// update config
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs2);

		Symptom symptom2 = lowCpuDetector.detect();
		assertNotNull(symptom2);
		assertTrue(symptom2 instanceof JobVertexLowCpu);
		Set<JobVertexID> vertices2 = ((JobVertexLowCpu) symptom2).getUtilities().keySet();
		assertEquals(1, vertices2.size());
		assertTrue(vertices2.contains(vertex1));
	}
}
