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
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.resource.MockNodeTestBase;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for BatchResultPartitionCalculator.
 */
public class DefaultResultPartitionCalculatorTest extends MockNodeTestBase {
	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;
	private RelMetadataQuery mq = mock(RelMetadataQuery.class);

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
	}

	@Test
	public void testUnion() {
		/**
		 *   0, Source   1, Source
		 *          \      /
		 *           2, Union
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 10);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		createNodeList(3);
		updateNode(0, mock(BatchExecTableSourceScan.class));
		updateNode(1, mock(BatchExecTableSourceScan.class));
		updateNode(2, mock(BatchExecUnion.class));
		connect(2, 0, 1);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(2));
		assertEquals(10, nodeList.get(0).getResource().getParallelism());
		assertEquals(10, nodeList.get(1).getResource().getParallelism());
		assertEquals(50, nodeList.get(2).getResource().getParallelism());
		assertEquals(10, nodeList.get(0).getResource().getParallelism());
	}

	@Test
	public void testSourceInferModeIsNone() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		createNodeList(2);
		updateNode(0, mock(BatchExecTableSourceScan.class));
		updateNode(1, mock(BatchExecExchange.class, RETURNS_DEEP_STUBS));
		when(((BatchExecExchange) nodeList.get(1)).getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(50, nodeList.get(0).getResource().getParallelism());
		assertEquals(50, nodeList.get(1).getResource().getParallelism());
	}

	@Test
	public void testSourceLocked() {
		/**
		 *   0, Source   2, Source    3, Source
		 *       |      /             /
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		createNodeList(4);
		updateNode(0, mock(BatchExecTableSourceScan.class));
		updateNode(2, mock(BatchExecBoundedStreamScan.class));
		updateNode(3, mock(BatchExecBoundedStreamScan.class));
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0, 2, 3);
		when(((BatchExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(30);
		when(((BatchExecBoundedStreamScan) nodeList.get(2)).getSourceTransformation(any()).getParallelism()).thenReturn(10);
		when(((BatchExecBoundedStreamScan) nodeList.get(3)).getSourceTransformation(any()).getParallelism()).thenReturn(-1);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(30, nodeList.get(0).getResource().getParallelism());
		assertEquals(50, nodeList.get(1).getResource().getParallelism());
		assertEquals(10, nodeList.get(2).getResource().getParallelism());
		assertEquals(StreamExecutionEnvironment.getDefaultLocalParallelism(), nodeList.get(3).getResource().getParallelism());
	}

	@Test
	public void testSourceInferMinParallelism() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		tEnv.getConfig().getConf().setLong(TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION, 100);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_MB_PER_PARTITION, 1000);
		createNodeList(2);
		BatchExecTableSourceScan scan = mock(BatchExecTableSourceScan.class, RETURNS_DEEP_STUBS);
		when(mq.getRowCount(scan)).thenReturn(50d);
		when(mq.getAverageRowSize(scan)).thenReturn(1.6 * ExecResourceUtil.SIZE_IN_MB);
		updateNode(0, scan);
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(1, nodeList.get(0).getResource().getParallelism());
		assertEquals(50, nodeList.get(1).getResource().getParallelism());
	}

	@Test
	public void testSourceInferMaxParallelism() throws Exception {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		tEnv.getConfig().getConf().setLong(TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION, 100);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_MB_PER_PARTITION, 1000);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX, 100);
		createNodeList(2);

		BatchExecTableSourceScan scan = PowerMockito.mock(BatchExecTableSourceScan.class);
		when(mq.getRowCount(scan)).thenReturn(50000d);
		when(mq.getAverageRowSize(scan)).thenReturn(1d * ExecResourceUtil.SIZE_IN_MB);
		updateNode(0, scan);
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(100, nodeList.get(0).getResource().getParallelism());
		assertEquals(50, nodeList.get(1).getResource().getParallelism());
	}

	@Test
	public void testSourceInferParallelism() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		tEnv.getConfig().getConf().setLong(TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION, 100);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_MB_PER_PARTITION, 1000);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX, 100);
		createNodeList(2);
		BatchExecTableSourceScan scan = mock(BatchExecTableSourceScan.class, RETURNS_DEEP_STUBS);
		when(mq.getRowCount(scan)).thenReturn(5000d);
		when(mq.getAverageRowSize(scan)).thenReturn(12d * ExecResourceUtil.SIZE_IN_MB);
		updateNode(0, scan);
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(60, nodeList.get(0).getResource().getParallelism());
		assertEquals(50, nodeList.get(1).getResource().getParallelism());
	}

	@Test
	public void testExchange() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		createNodeList(2);
		updateNode(0, mock(BatchExecTableSourceScan.class));
		updateNode(1, mock(BatchExecExchange.class, RETURNS_DEEP_STUBS));
		when(((BatchExecExchange) nodeList.get(1)).getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		connect(1, 0);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(50, nodeList.get(0).getResource().getParallelism());
		assertEquals(1, nodeList.get(1).getResource().getParallelism());
	}

	@Test
	public void testValues() {
		/**
		 *   0, Values
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getConf().setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 50);
		createNodeList(2);
		updateNode(0, mock(BatchExecValues.class));
		updateNode(1, mock(BatchExecExchange.class, RETURNS_DEEP_STUBS));
		when(((BatchExecExchange) nodeList.get(1)).getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		connect(1, 0);
		BatchResultPartitionCalculator.calculate(tEnv, mq, nodeList.get(1));
		assertEquals(1, nodeList.get(0).getResource().getParallelism());
		assertEquals(1, nodeList.get(1).getResource().getParallelism());
	}
}
