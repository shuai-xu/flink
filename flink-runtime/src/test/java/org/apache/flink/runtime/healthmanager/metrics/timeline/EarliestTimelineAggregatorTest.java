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

package org.apache.flink.runtime.healthmanager.metrics.timeline;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for LatestTimelineAggregator.
 */
public class EarliestTimelineAggregatorTest {

	@Test
	public void testAgg() {
		EarliestTimelineAggregator aggregator = new EarliestTimelineAggregator(1000);
		aggregator.addValue(Tuple2.of(0L, 10.0));
		aggregator.addValue(Tuple2.of(999L, 20.0));
		aggregator.addValue(Tuple2.of(1000L, 30.0));
		assertEquals(aggregator.getValue(), Tuple2.of(0L, 10.0));

		aggregator.addValue(Tuple2.of(2000L, 10.0));
		assertEquals(aggregator.getValue(), Tuple2.of(1000L, 30.0));

	}

	@Test
	public void testNull() {
		EarliestTimelineAggregator aggregator = new EarliestTimelineAggregator(1000);
		assertNull(aggregator.getValue());
	}
}
