/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch.calculator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.resource.MockNodeTestBase;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for NodeFinalParallelismSetter.
 */
public class NodeFinalParallelismSetterTest extends MockNodeTestBase {

	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
		sEnv.setParallelism(21);
	}

	@Test
	public void testSource() {
		/**
		 *   0, Source   1, Source  2, Values  4, Source   5, Source
		 *            \      /      /            /           /
		 *             3, Union
		 */
		createNodeList(6);
		BatchExecTableSourceScan scan0 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		when(((BatchExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		updateNode(1, mock(BatchExecTableSourceScan.class));
		updateNode(2, mock(BatchExecValues.class));
		updateNode(3, mock(BatchExecUnion.class));
		updateNode(4, mock(BatchExecBoundedStreamScan.class));
		when(((BatchExecBoundedStreamScan) nodeList.get(4)).getSourceTransformation(any()).getParallelism()).thenReturn(7);
		updateNode(5, mock(BatchExecBoundedStreamScan.class));
		connect(3, 0, 1, 2, 4, 5);
		NodeFinalParallelismSetter.calculate(tEnv, nodeList.get(3));
		assertEquals(5, nodeList.get(0).getResource().getParallelism());
		assertEquals(-1, nodeList.get(1).getResource().getParallelism());
		assertEquals(1, nodeList.get(2).getResource().getParallelism());
		assertEquals(7, nodeList.get(4).getResource().getParallelism());
		assertEquals(21, nodeList.get(5).getResource().getParallelism());
	}

	@Test
	public void testExchange() {
		/**
		 *   0, Source    1, Source
		 *        |         |
		 *   2, Exchange  3, Exchange
		 *        |         |
		 *   4, Calc      5, Calc
		 *         \         /
		 *          6, Union
		 */
		createNodeList(7);
		BatchExecTableSourceScan scan0 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		when(((BatchExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		BatchExecExchange execExchange4 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange4.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateNode(2, execExchange4);
		BatchExecExchange execExchange3 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange3.getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		updateNode(3, execExchange3);
		connect(2, 0);
		connect(4, 2);
		connect(3, 1);
		connect(5, 3);
		connect(6, 4, 5);
		NodeFinalParallelismSetter.calculate(tEnv, nodeList.get(6));
		assertEquals(5, nodeList.get(0).getResource().getParallelism());
		assertEquals(1, nodeList.get(5).getResource().getParallelism());
		assertEquals(1, nodeList.get(3).getResource().getParallelism());
		assertEquals(-1, nodeList.get(1).getResource().getParallelism());
		assertEquals(-1, nodeList.get(2).getResource().getParallelism());
		assertEquals(-1, nodeList.get(4).getResource().getParallelism());
		assertEquals(-1, nodeList.get(6).getResource().getParallelism());
	}
}
