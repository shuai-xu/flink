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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.JobTMMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for LongTimeFullGCDetector.
 */
public class LongTimeFullGCDetectorTest extends DetectorTestBase {
	@Test
	public void testLongTimeFullGC() throws Exception {
		// initial job vertex config.
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		ExecutionVertexID executionVertexID1 = new ExecutionVertexID(vertex1, 0);

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
				10, 4, new ResourceSpec.Builder().setHeapMemoryInMB(20).build());
		vertexConfigs.put(vertex1, vertex1Config1);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Collections.emptyList());

		Mockito.when(restClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes));

		Mockito.when(restClient.getTaskManagerTasks(Mockito.eq("tmId")))
			.thenReturn(Arrays.asList(executionVertexID1));

		// mock subscribing JM GC time metric
		JobTMMetricSubscription gcTimeSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(gcTimeSub.getValue()).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
				InvocationOnMock invocationOnMock) throws Throwable {
				Map<String, Tuple2<Long, Double>> fullGCs = new HashMap<>();
				fullGCs.put("tmId", Tuple2.of(System.currentTimeMillis(), 18000.0));
				return fullGCs;
			}
		});

		// mock subscribing JM GC count metric
		JobTMMetricSubscription gcCountSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(gcCountSub.getValue()).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
				InvocationOnMock invocationOnMock) throws Throwable {
				Map<String, Tuple2<Long, Double>> fullGCs = new HashMap<>();
				fullGCs.put("tmId", Tuple2.of(System.currentTimeMillis(), 3.0));
				return fullGCs;
			}
		});

		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class), Mockito.eq(MetricNames.FULL_GC_TIME_METRIC),
			Mockito.anyLong(), Mockito.eq(TimelineAggType.RANGE)))
			.thenReturn(gcTimeSub);

		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class), Mockito.eq(MetricNames.FULL_GC_COUNT_METRIC),
			Mockito.anyLong(), Mockito.eq(TimelineAggType.RANGE)))
			.thenReturn(gcCountSub);

		LongTimeFullGCDetector detector = new LongTimeFullGCDetector();
		detector.open(monitor);
		JobVertexLongTimeFullGC symptom = (JobVertexLongTimeFullGC) detector.detect();

		List<JobVertexID> vertex = new LinkedList<>();
		vertex.add(vertex1);
		assertEquals(vertex, symptom.getJobVertexIDs());
	}
}
