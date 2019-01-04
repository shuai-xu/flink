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
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for Task Metrics Subscription.
 */
public class TaskMetricSubscriptionTest {

	@Test
	public void testSubscription() {
		TaskMetricSubscription subscription = new TaskMetricSubscription(
				new JobID(),
				new JobVertexID(),
				MetricAggType.MAX,
				"test",
				TimelineAggType.AVG,
				1000);
		Map<Integer, Tuple2<Long, Double>> data1 = new HashMap<>();
		data1.put(0, Tuple2.of(100L, 10.0));
		data1.put(1, Tuple2.of(100L, 10.0));
		data1.put(2, Tuple2.of(100L, 10.0));
		subscription.addValue(data1);
		assertEquals(null, subscription.getValue());

		Map<Integer, Tuple2<Long, Double>> data2 = new HashMap<>();
		data2.put(0, Tuple2.of(200L, 20.0));
		data2.put(1, Tuple2.of(200L, 30.0));
		data2.put(2, Tuple2.of(200L, 40.0));
		subscription.addValue(data2);
		assertEquals(null, subscription.getValue());

		Map<Integer, Tuple2<Long, Double>> data3 = new HashMap<>();
		data3.put(0, Tuple2.of(1000L, 20.0));
		data3.put(1, Tuple2.of(1000L, 30.0));
		data3.put(2, Tuple2.of(1000L, 40.0));
		subscription.addValue(data3);
		assertEquals(Tuple2.of(0L, 25.0), subscription.getValue());
	}
}
