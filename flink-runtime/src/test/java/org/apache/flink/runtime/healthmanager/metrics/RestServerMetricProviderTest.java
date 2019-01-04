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

package org.apache.flink.runtime.healthmanager.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertEquals;

/**
 * Tests for RestServer Metric Provider.
 */
public class RestServerMetricProviderTest {

	@Test
	public void testSubscribeTaskMetric() throws InterruptedException {
		Configuration config = new Configuration();
		config.setString("metric.provider.fetch.interval.ms", "200");
		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
				1, new ExecutorThreadFactory("health-manager"));
		RestServerClient client = Mockito.mock(RestServerClient.class);

		RestServerMetricProvider provider = new RestServerMetricProvider(
				config,
				client,
				executorService);
		provider.open();

		JobID jobID = new JobID();
		JobVertexID vertexID = new JobVertexID();

		Map<String, Map<Integer, Tuple2<Long, Double>>> values1 = new HashMap<>();
		Map<Integer, Tuple2<Long, Double>> subtaskValues1 = new HashMap<>();
		subtaskValues1.put(0, Tuple2.of(100L, 10.0));
		subtaskValues1.put(1, Tuple2.of(100L, 20.0));
		values1.put("test-1", subtaskValues1);

		Map<String, Map<Integer, Tuple2<Long, Double>>> values2 = new HashMap<>();
		Map<Integer, Tuple2<Long, Double>> subtaskValues2 = new HashMap<>();
		subtaskValues2.put(0, Tuple2.of(200L, 20.0));
		subtaskValues2.put(1, Tuple2.of(200L, 30.0));
		values2.put("test-1", subtaskValues2);

		Map<String, Map<Integer, Tuple2<Long, Double>>> values3 = new HashMap<>();
		Map<Integer, Tuple2<Long, Double>> subtaskValues3 = new HashMap<>();
		subtaskValues3.put(0, Tuple2.of(1000L, 20.0));
		subtaskValues3.put(1, Tuple2.of(1000L, 30.0));
		values3.put("test-1", subtaskValues3);
		Mockito.when(client.getTaskMetrics(
				Mockito.eq(jobID), Mockito.eq(vertexID), Mockito.eq(Collections.singleton("test-1"))))
			.thenReturn(values1).thenReturn(values2).thenReturn(values3).thenReturn(new HashMap<>());

		TaskMetricSubscription subscription = provider.subscribeTaskMetric(
				jobID, vertexID, "test-1", MetricAggType.MAX, 1000L, TimelineAggType.MAX);

		Thread.sleep(2000);

		assertEquals(Tuple2.of(0L, 30.0), subscription.getValue());
	}

	@Test
	public void testSubscribeJobTMMetric() throws InterruptedException {
		Configuration config = new Configuration();
		config.setString("metric.provider.fetch.interval.ms", "200");
		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
				1, new ExecutorThreadFactory("health-manager"));
		RestServerClient client = Mockito.mock(RestServerClient.class);

		RestServerMetricProvider provider = new RestServerMetricProvider(
				config,
				client,
				executorService);
		provider.open();

		JobID jobID = new JobID();

		Map<String, Map<String, Tuple2<Long, Double>>> values1 = new HashMap<>();
		Map<String, Tuple2<Long, Double>> subtaskValues1 = new HashMap<>();
		subtaskValues1.put("tm-1", Tuple2.of(100L, 10.0));
		subtaskValues1.put("tm-2", Tuple2.of(100L, 20.0));
		values1.put("test-1", subtaskValues1);

		Map<String, Map<String, Tuple2<Long, Double>>> values2 = new HashMap<>();
		Map<String, Tuple2<Long, Double>> subtaskValues2 = new HashMap<>();
		subtaskValues2.put("tm-1", Tuple2.of(200L, 20.0));
		subtaskValues2.put("tm-2", Tuple2.of(200L, 30.0));
		values2.put("test-1", subtaskValues2);

		Map<String, Map<String, Tuple2<Long, Double>>> values3 = new HashMap<>();
		Map<String, Tuple2<Long, Double>> subtaskValues3 = new HashMap<>();
		subtaskValues3.put("tm-1", Tuple2.of(1000L, 40.0));
		subtaskValues3.put("tm-2", Tuple2.of(1000L, 20.0));
		values3.put("test-1", subtaskValues3);

		Mockito.when(client.getTaskManagerMetrics(
				Mockito.eq(jobID), Mockito.eq(Collections.singleton("test-1"))))
				.thenReturn(values1).thenReturn(values2).thenReturn(values3).thenReturn(new HashMap<>());

		JobTMMetricSubscription subscription = provider.subscribeAllTMMetric(
				jobID, "test-1", 1000L, TimelineAggType.MAX);

		Thread.sleep(2000);

		assertEquals(
				new HashMap<String, Tuple2<Long, Double>>() {
					{
						put("tm-1", Tuple2.of(0L, 20.0));
						put("tm-2", Tuple2.of(0L, 30.0));
					}
				}, subscription.getValue());
	}

	@Test
	public void testSubscribeTMMetric() throws InterruptedException {
		Configuration config = new Configuration();
		config.setString("metric.provider.fetch.interval.ms", "200");
		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
				1, new ExecutorThreadFactory("health-manager"));
		RestServerClient client = Mockito.mock(RestServerClient.class);

		RestServerMetricProvider provider = new RestServerMetricProvider(
				config,
				client,
				executorService);
		provider.open();

		Map<String, Map<String, Tuple2<Long, Double>>> values1 = new HashMap<>();
		Map<String, Tuple2<Long, Double>> subtaskValues1 = new HashMap<>();
		subtaskValues1.put("tm-1", Tuple2.of(100L, 10.0));
		values1.put("test-1", subtaskValues1);

		Map<String, Map<String, Tuple2<Long, Double>>> values2 = new HashMap<>();
		Map<String, Tuple2<Long, Double>> subtaskValues2 = new HashMap<>();
		subtaskValues2.put("tm-1", Tuple2.of(200L, 20.0));
		values2.put("test-1", subtaskValues2);

		Map<String, Map<String, Tuple2<Long, Double>>> values3 = new HashMap<>();
		Map<String, Tuple2<Long, Double>> subtaskValues3 = new HashMap<>();
		subtaskValues3.put("tm-1", Tuple2.of(1000L, 40.0));
		values3.put("test-1", subtaskValues3);

		Mockito.when(client.getTaskManagerMetrics(
				Mockito.eq(Collections.singleton("tm-1")), Mockito.eq(Collections.singleton("test-1"))))
				.thenReturn(values1).thenReturn(values2).thenReturn(values3).thenReturn(new HashMap<>());

		TaskManagerMetricSubscription subscription = provider.subscribeTaskManagerMetric(
				"tm-1", "test-1", 1000L, TimelineAggType.MAX);

		Thread.sleep(2000);

		assertEquals(Tuple2.of(0L, 20.0), subscription.getValue());
	}

}
