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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLowMemory;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for LowMemoryDetector.
 */
public class LowMemoryDetectorTest extends DetectorTestBase {
	static final int GB = 1024 * 1024 * 1024;
	@Test
	public void testDetectLowMemory() throws Exception {
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_DOWN_WAIT_TIME, 0);
		config.setDouble(LowMemoryDetector.LOW_MEM_THRESHOLD, 0.6);

		// vertex 1: heap memory low
		// vertex 2: direct memory low
		// vertex 3: native memory low
		// vertex 4: memory not low

		String tmId1 = "tmId1";
		String tmId2 = "tmId2";
		String tmId3 = "tmId3";
		String tmId4 = "tmId4";
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();
		JobVertexID vertex4 = new JobVertexID();

		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId1))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex1, 0), new ExecutionVertexID(vertex1, 1)));
		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId2))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex2, 0), new ExecutionVertexID(vertex2, 1)));
		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId3))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex3, 0), new ExecutionVertexID(vertex3, 1)));
		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq(tmId4))).thenReturn(
			Lists.newArrayList(new ExecutionVertexID(vertex4, 0), new ExecutionVertexID(vertex4, 1)));

		RestServerClient.VertexConfig vertexConfig1 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(512).build());
		RestServerClient.VertexConfig vertexConfig2 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setDirectMemoryInMB(512).build());
		RestServerClient.VertexConfig vertexConfig3 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setNativeMemoryInMB(512).build());
		RestServerClient.VertexConfig vertexConfig4 = new RestServerClient.VertexConfig(
			1, 1, ResourceSpec.newBuilder().setHeapMemoryInMB(512).build());

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, vertexConfig1);
		vertexConfigs.put(vertex2, vertexConfig2);
		vertexConfigs.put(vertex3, vertexConfig3);
		vertexConfigs.put(vertex4, vertexConfig4);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		// set capacity

		Map<String, Tuple2<Long, Double>> capacities = new HashMap<>();
		capacities.put(tmId1, new Tuple2<>(0L, 1.0 * GB));
		capacities.put(tmId2, new Tuple2<>(0L, 1.0 * GB));
		capacities.put(tmId3, new Tuple2<>(0L, 1.0 * GB));
		capacities.put(tmId4, new Tuple2<>(0L, 1.0 * GB));

		JobTMMetricSubscription capacitySub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_MEM_CAPACITY),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(capacitySub);
		Mockito.when(capacitySub.getValue()).thenReturn(capacities);

		// set total usage

		Map<String, Tuple2<Long, Double>> totalUsageLow = new HashMap<>();
		totalUsageLow.put(tmId1, new Tuple2<>(0L, 0.5 * GB));
		totalUsageLow.put(tmId2, new Tuple2<>(0L, 0.5 * GB));
		totalUsageLow.put(tmId3, new Tuple2<>(0L, 0.5 * GB));
		totalUsageLow.put(tmId4, new Tuple2<>(0L, 0.8 * GB));

		Map<String, Tuple2<Long, Double>> totalUsageHigh = new HashMap<>();
		totalUsageHigh.put(tmId1, new Tuple2<>(0L, 0.8 * GB));
		totalUsageHigh.put(tmId2, new Tuple2<>(0L, 0.8 * GB));
		totalUsageHigh.put(tmId3, new Tuple2<>(0L, 0.8 * GB));
		totalUsageHigh.put(tmId4, new Tuple2<>(0L, 0.8 * GB));

		JobTMMetricSubscription totalUsageSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_MEM_USAGE_TOTAL),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(totalUsageSub);
		Mockito.when(totalUsageSub.getValue())
			.thenReturn(totalUsageHigh)
			.thenReturn(totalUsageLow)
			.thenReturn(totalUsageLow)
			.thenReturn(totalUsageHigh);

		// set heap usage

		Map<String, Tuple2<Long, Double>> heapUsageLow = new HashMap<>();
		heapUsageLow.put(tmId1, new Tuple2<>(0L, 0.5 * GB));
		heapUsageLow.put(tmId2, new Tuple2<>(0L, 0.0 * GB));
		heapUsageLow.put(tmId3, new Tuple2<>(0L, 0.0 * GB));
		heapUsageLow.put(tmId4, new Tuple2<>(0L, 0.8 * GB));

		Map<String, Tuple2<Long, Double>> heapUsageHigh = new HashMap<>();
		heapUsageHigh.put(tmId1, new Tuple2<>(0L, 0.8 * GB));
		heapUsageHigh.put(tmId2, new Tuple2<>(0L, 0.0 * GB));
		heapUsageHigh.put(tmId3, new Tuple2<>(0L, 0.0 * GB));
		heapUsageHigh.put(tmId4, new Tuple2<>(0L, 0.8 * GB));

		JobTMMetricSubscription heapUsageSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_MEM_HEAP_USED),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(heapUsageSub);
		Mockito.when(heapUsageSub.getValue())
			.thenReturn(heapUsageHigh)
			.thenReturn(heapUsageLow)
			.thenReturn(heapUsageLow)
			.thenReturn(heapUsageHigh);

		// set non heap usage

		Map<String, Tuple2<Long, Double>> nonHeapUsageLow = new HashMap<>();
		nonHeapUsageLow.put(tmId1, new Tuple2<>(0L, 0.0 * GB));
		nonHeapUsageLow.put(tmId2, new Tuple2<>(0L, 0.5 * GB));
		nonHeapUsageLow.put(tmId3, new Tuple2<>(0L, 0.0 * GB));
		nonHeapUsageLow.put(tmId4, new Tuple2<>(0L, 0.0 * GB));

		Map<String, Tuple2<Long, Double>> nonHeapUsageHigh = new HashMap<>();
		nonHeapUsageHigh.put(tmId1, new Tuple2<>(0L, 0.0 * GB));
		nonHeapUsageHigh.put(tmId2, new Tuple2<>(0L, 0.8 * GB));
		nonHeapUsageHigh.put(tmId3, new Tuple2<>(0L, 0.0 * GB));
		nonHeapUsageHigh.put(tmId4, new Tuple2<>(0L, 0.0 * GB));

		JobTMMetricSubscription nonHeapUsageSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.eq(jobID),
			Mockito.eq(MetricNames.TM_MEM_NON_HEAP_USED),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.AVG)))
			.thenReturn(nonHeapUsageSub);
		Mockito.when(nonHeapUsageSub.getValue())
			.thenReturn(nonHeapUsageHigh)
			.thenReturn(nonHeapUsageLow)
			.thenReturn(nonHeapUsageLow)
			.thenReturn(nonHeapUsageHigh);

		// verify detections

		LowMemoryDetector lowMemoryDetector = new LowMemoryDetector();
		lowMemoryDetector.open(monitor);

		Symptom symptom1 = lowMemoryDetector.detect();
		assertNull(symptom1);

		Symptom symptom2 = lowMemoryDetector.detect();
		assertNull(symptom2);

		Symptom symptom3 = lowMemoryDetector.detect();
		assertNotNull(symptom3);
		assertTrue(symptom3 instanceof JobVertexLowMemory);
		JobVertexLowMemory jobVertexLowMemory = (JobVertexLowMemory) symptom3;
		assertEquals(3, jobVertexLowMemory.getHeapUtilities().size());
		assertEquals(Double.valueOf(0.5), jobVertexLowMemory.getHeapUtilities().get(vertex1));
		assertEquals(Double.valueOf(0.5), jobVertexLowMemory.getNonHeapUtilities().get(vertex2));
		assertEquals(Double.valueOf(0.5), jobVertexLowMemory.getNativeUtilities().get(vertex3));

		Symptom symptom4 = lowMemoryDetector.detect();
		assertNull(symptom4);
	}
}
