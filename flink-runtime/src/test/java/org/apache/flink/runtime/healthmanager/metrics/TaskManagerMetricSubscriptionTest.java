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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for TaskManagerMetricSubscription.
 */
public class TaskManagerMetricSubscriptionTest {

	@Test
	public void testSubscription() {
		TaskManagerMetricSubscription subscription = new TaskManagerMetricSubscription(
				"tm-1",
				"test",
				TimelineAggType.AVG,
				1000);
		subscription.addValue(Tuple2.of(100L, 10.0));
		assertEquals(null, subscription.getValue());
		subscription.addValue(Tuple2.of(200L, 20.0));
		assertEquals(null, subscription.getValue());
		subscription.addValue(Tuple2.of(1000L, 20.0));
		assertEquals(Tuple2.of(0L, 15.0), subscription.getValue());
	}
}
