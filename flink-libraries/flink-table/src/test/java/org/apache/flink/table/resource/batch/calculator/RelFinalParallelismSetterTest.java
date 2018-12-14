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
import org.apache.flink.table.resource.MockRelTestBase;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for RelFinalParallelismSetter.
 */
public class RelFinalParallelismSetterTest extends MockRelTestBase {

	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
	}

	@Test
	public void testSource() {
		/**
		 *   0, Source   1, Source  2, Values  4, Source   5, Source
		 *            \      /      /            /           /
		 *             3, Union
		 */
		createRelList(6);
		BatchExecTableSourceScan scan0 = mock(BatchExecTableSourceScan.class);
		updateRel(0, scan0);
		when(((BatchExecTableSourceScan) relList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		updateRel(1, mock(BatchExecTableSourceScan.class));
		updateRel(2, mock(BatchExecValues.class));
		updateRel(3, mock(BatchExecUnion.class));
		updateRel(4, mock(BatchExecBoundedStreamScan.class));
		when(((BatchExecBoundedStreamScan) relList.get(4)).getSourceTransformation(any()).getParallelism()).thenReturn(7);
		updateRel(5, mock(BatchExecBoundedStreamScan.class));
		connect(3, 0, 1, 2, 4, 5);
		RelFinalParallelismSetter.calculate(tEnv, relList.get(3));
		verify(relList.get(0)).setResultPartitionCount(5);
		verify(relList.get(2)).setResultPartitionCount(1);
		verify(relList.get(1), never()).setResultPartitionCount(anyInt());
		verify(relList.get(4)).setResultPartitionCount(7);
		verify(relList.get(5)).setResultPartitionCount(StreamExecutionEnvironment.getDefaultLocalParallelism());
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
		createRelList(7);
		BatchExecTableSourceScan scan0 = mock(BatchExecTableSourceScan.class);
		updateRel(0, scan0);
		when(((BatchExecTableSourceScan) relList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		BatchExecExchange execExchange4 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange4.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateRel(2, execExchange4);
		BatchExecExchange execExchange3 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange3.getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		updateRel(3, execExchange3);
		connect(2, 0);
		connect(4, 2);
		connect(3, 1);
		connect(5, 3);
		connect(6, 4, 5);
		RelFinalParallelismSetter.calculate(tEnv, relList.get(6));
		verify(relList.get(0)).setResultPartitionCount(5);
		verify(relList.get(5)).setResultPartitionCount(1);
		verify(relList.get(3)).setResultPartitionCount(1);
		verify(relList.get(1), never()).setResultPartitionCount(anyInt());
		verify(relList.get(2), never()).setResultPartitionCount(anyInt());
		verify(relList.get(4), never()).setResultPartitionCount(anyInt());
		verify(relList.get(6), never()).setResultPartitionCount(anyInt());
	}
}
