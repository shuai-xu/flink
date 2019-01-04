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

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for different metrics value.
 */
public class MetricAggFunctionTest {

	List<Tuple2<Long, Double>> alignedData = new LinkedList<>();
	List<Tuple2<Long, Double>> unAlignedData = new LinkedList<>();
	List<Tuple2<Long, Double>> nullData = new LinkedList<>();

	@Before
	public void setup() {
		alignedData.add(Tuple2.of(100L, 10.0));
		alignedData.add(Tuple2.of(100L, 20.0));
		alignedData.add(Tuple2.of(100L, 30.0));
		alignedData.add(Tuple2.of(100L, 40.0));

		unAlignedData.add(Tuple2.of(100L, 10.0));
		unAlignedData.add(Tuple2.of(200L, 20.0));
		unAlignedData.add(Tuple2.of(100L, 30.0));
		unAlignedData.add(Tuple2.of(100L, 40.0));

		nullData.add(Tuple2.of(100L, 10.0));
		nullData.add(null);
		nullData.add(Tuple2.of(100L, 30.0));
		nullData.add(Tuple2.of(100L, 40.0));
	}

	@Test
	public void testSum() {
		assertEquals(
			Tuple2.of(100L, 100.0),
			MetricAggFunction.getMetricAggFunction(MetricAggType.SUM).getValue(alignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.SUM).getValue(unAlignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.SUM).getValue(nullData));
	}

	@Test
	public void testAvg() {
		assertEquals(
				Tuple2.of(100L, 25.0),
				MetricAggFunction.getMetricAggFunction(MetricAggType.AVG).getValue(alignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.AVG).getValue(unAlignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.AVG).getValue(nullData));
	}

	@Test
	public void testMax() {
		assertEquals(
				Tuple2.of(100L, 40.0),
				MetricAggFunction.getMetricAggFunction(MetricAggType.MAX).getValue(alignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.MAX).getValue(unAlignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.MAX).getValue(nullData));
	}

	@Test
	public void testMin() {
		assertEquals(
				Tuple2.of(100L, 10.0),
				MetricAggFunction.getMetricAggFunction(MetricAggType.MIN).getValue(alignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.MIN).getValue(unAlignedData));

		assertEquals(
				null,
				MetricAggFunction.getMetricAggFunction(MetricAggType.MIN).getValue(nullData));
	}
}
