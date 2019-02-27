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
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighDelay;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for HighDelayDetector.
 */
public class HighDelayDetectorTest extends DetectorTestBase {

	@Test
	public void testDetectLowDelay() throws Exception {
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
		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputs = new HashMap<>();
		inputs.put(vertex1, Collections.emptyList());
		inputs.put(vertex2, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		Mockito.when(jobConfig.getInputNodes()).thenReturn(inputs);

		Mockito.when(metricProvider.subscribeTaskMetric(
				Mockito.any(JobID.class),
				Mockito.any(JobVertexID.class),
				Mockito.anyString(),
				Mockito.any(MetricAggType.class),
				Mockito.anyLong(),
				Mockito.any(TimelineAggType.class))).then(new Answer<TaskMetricSubscription>() {
			@Override
			public TaskMetricSubscription answer(InvocationOnMock invocation) throws Throwable {
				JobVertexID vertexId = (JobVertexID) invocation.getArguments()[1];
				String metricName = (String) invocation.getArguments()[2];
				TimelineAggType timelineAggType = (TimelineAggType) invocation.getArguments()[5];
				if (vertex1.equals(vertexId)) {
					// task latency: 3 partition latency: 10 partition count: 10
					if (metricName.equals(MetricNames.SOURCE_DELAY)) {
						if (timelineAggType.equals(TimelineAggType.LATEST)) {
							TaskMetricSubscription subscription = Mockito.mock(TaskMetricSubscription.class);
							Mockito.when(subscription.getValue()).thenReturn(Tuple2.of(0L, 10 * 60 * 1000.0));
							return subscription;
						}
					}
				}
				return Mockito.mock(TaskMetricSubscription.class);
			}
		});

		HighDelayDetector detector = new HighDelayDetector();
		detector.open(monitor);
		assertNull(detector.detect());
	}

	@Test
	public void testDetectHighDelay() throws Exception {
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
		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputs = new HashMap<>();
		inputs.put(vertex1, Collections.emptyList());
		inputs.put(vertex2, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		Mockito.when(jobConfig.getInputNodes()).thenReturn(inputs);

		Mockito.when(metricProvider.subscribeTaskMetric(
				Mockito.any(JobID.class),
				Mockito.any(JobVertexID.class),
				Mockito.anyString(),
				Mockito.any(MetricAggType.class),
				Mockito.anyLong(),
				Mockito.any(TimelineAggType.class))).then(new Answer<TaskMetricSubscription>() {
			@Override
			public TaskMetricSubscription answer(InvocationOnMock invocation) throws Throwable {
				JobVertexID vertexId = (JobVertexID) invocation.getArguments()[1];
				String metricName = (String) invocation.getArguments()[2];
				TimelineAggType timelineAggType = (TimelineAggType) invocation.getArguments()[5];
				if (vertex1.equals(vertexId)) {
					// task latency: 3 partition latency: 10 partition count: 10
					if (metricName.equals(MetricNames.SOURCE_DELAY)) {
						if (timelineAggType.equals(TimelineAggType.LATEST)) {
							TaskMetricSubscription subscription = Mockito.mock(TaskMetricSubscription.class);
							Mockito.when(subscription.getValue()).thenReturn(Tuple2.of(0L, 1 + 10 * 60 * 1000.0));
							return subscription;
						}
					}
				}
				return Mockito.mock(TaskMetricSubscription.class);
			}
		});

		HighDelayDetector detector = new HighDelayDetector();
		detector.open(monitor);
		JobVertexHighDelay delaySyptom = (JobVertexHighDelay) detector.detect();
		List<JobVertexID> vertex = new LinkedList<>();
		vertex.add(vertex1);
		assertEquals(vertex, delaySyptom.getJobVertexIDs());
		assertEquals(Collections.emptyList(), delaySyptom.getSevereJobVertexIDs());

		// check severe delay.
		Mockito.when(metricProvider.subscribeTaskMetric(
				Mockito.any(JobID.class),
				Mockito.any(JobVertexID.class),
				Mockito.anyString(),
				Mockito.any(MetricAggType.class),
				Mockito.anyLong(),
				Mockito.any(TimelineAggType.class))).then(new Answer<TaskMetricSubscription>() {
			@Override
			public TaskMetricSubscription answer(InvocationOnMock invocation) throws Throwable {
				JobVertexID vertexId = (JobVertexID) invocation.getArguments()[1];
				String metricName = (String) invocation.getArguments()[2];
				TimelineAggType timelineAggType = (TimelineAggType) invocation.getArguments()[5];
				if (vertex1.equals(vertexId)) {
					// task latency: 3 partition latency: 10 partition count: 10
					if (metricName.equals(MetricNames.SOURCE_DELAY)) {
						if (timelineAggType.equals(TimelineAggType.LATEST)) {
							TaskMetricSubscription subscription = Mockito.mock(TaskMetricSubscription.class);
							Mockito.when(subscription.getValue()).thenReturn(Tuple2.of(0L, 1 + 60 * 60 * 1000.0));
							return subscription;
						}
					}
				}
				return Mockito.mock(TaskMetricSubscription.class);
			}
		});
		detector = new HighDelayDetector();
		detector.open(monitor);
		delaySyptom = (JobVertexHighDelay) detector.detect();
		assertEquals(vertex, delaySyptom.getJobVertexIDs());
		assertEquals(vertex, delaySyptom.getSevereJobVertexIDs());

	}
}
