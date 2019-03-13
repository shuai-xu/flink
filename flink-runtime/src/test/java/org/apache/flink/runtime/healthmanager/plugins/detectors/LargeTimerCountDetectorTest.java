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
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLargeTimerCount;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests to check whether there existing massive proctime/rowtime timer.
 */
public class LargeTimerCountDetectorTest extends DetectorTestBase {
	@Test
	public void testMassiveTimerDetecting1() throws Exception {
		// initial job vertex config.
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			10, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			10, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs.put(vertex1, vertex1Config1);
		vertexConfigs.put(vertex2, vertex2Config1);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.any(JobID.class),
			Mockito.any(JobVertexID.class),
			Mockito.eq(MetricNames.TASK_TIMER_COUNT),
			Mockito.any(MetricAggType.class),
			Mockito.anyLong(),
			Mockito.any(TimelineAggType.class))).then(new Answer<TaskMetricSubscription>() {
			@Override
			public TaskMetricSubscription answer(InvocationOnMock invocation) throws Throwable {
				JobVertexID vertexId = (JobVertexID) invocation.getArguments()[1];
				TaskMetricSubscription subscription = Mockito.mock(TaskMetricSubscription.class);
				if (vertex1.equals(vertexId)) {
					Mockito.when(subscription.getValue()).thenReturn(Tuple2.of(0L, 7000000.0));
				} else {
					Mockito.when(subscription.getValue()).thenReturn(Tuple2.of(0L, 5000000.0));
				}
				return subscription;
			}
		});

		LargeTimerCountDetector detector = new LargeTimerCountDetector();
		detector.open(monitor);
		JobVertexLargeTimerCount massiveTimer = (JobVertexLargeTimerCount) detector.detect();
		List<JobVertexID> vertex = new LinkedList<>();
		vertex.add(vertex1);
		assertEquals(vertex, massiveTimer.getJobVertexIDs());
	}

	@Test
	public void testMassiveTimerDetecting2() throws Exception {
		// initial job vertex config.
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			10, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			10, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs.put(vertex1, vertex1Config1);
		vertexConfigs.put(vertex2, vertex2Config1);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.any(JobID.class),
			Mockito.any(JobVertexID.class),
			Mockito.eq(MetricNames.TASK_TIMER_COUNT),
			Mockito.any(MetricAggType.class),
			Mockito.anyLong(),
			Mockito.any(TimelineAggType.class))).then(new Answer<TaskMetricSubscription>() {
			@Override
			public TaskMetricSubscription answer(InvocationOnMock invocation) throws Throwable {
				JobVertexID vertexId = (JobVertexID) invocation.getArguments()[1];
				TaskMetricSubscription subscription = Mockito.mock(TaskMetricSubscription.class);
				Mockito.when(subscription.getValue()).thenReturn(Tuple2.of(0L, 0.0));
				return subscription;
			}
		});

		LargeTimerCountDetector detector = new LargeTimerCountDetector();
		detector.open(monitor);
		assertNull(detector.detect());
	}
}
