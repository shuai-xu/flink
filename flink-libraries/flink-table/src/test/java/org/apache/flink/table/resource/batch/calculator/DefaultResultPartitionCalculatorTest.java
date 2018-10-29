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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.resource.MockRelTestBase;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for BatchResultPartitionCalculator.
 */
public class DefaultResultPartitionCalculatorTest extends MockRelTestBase {
	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;
	private BatchResultPartitionCalculator resultPartitionCalculator;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
		resultPartitionCalculator = new BatchResultPartitionCalculator(tEnv);
	}

	@Test
	public void testUnion() {
		/**
		 *   0, Source   1, Source
		 *          \      /
		 *           2, Union
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_SOURCE_PARALLELISM(), 10);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		createRelList(3);
		updateRel(0, mock(BatchExecScan.class));
		updateRel(1, mock(BatchExecScan.class));
		updateRel(2, mock(BatchExecUnion.class));
		connect(2, 0, 1);
		resultPartitionCalculator.calculate(relList.get(2));
		verify(relList.get(0)).setResultPartitionCount(10);
		verify(relList.get(1)).setResultPartitionCount(10);
		verify(relList.get(2)).setResultPartitionCount(50);
	}

	@Test
	public void testSourceInferModeIsNone() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		createRelList(2);
		updateRel(0, mock(BatchExecScan.class));
		updateRel(1, mock(BatchExecExchange.class, RETURNS_DEEP_STUBS));
		when(((BatchExecExchange) relList.get(1)).getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(50);
		verify(relList.get(1)).setResultPartitionCount(50);
	}

	@Test
	public void testSourceLocked() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		createRelList(2);
		updateRel(0, mock(BatchExecScan.class));
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateRel(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		when(((BatchExecScan) relList.get(0)).getTableSourceResultPartitionNum(tEnv)).thenReturn(new Tuple2<>(true, 30));
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(30);
		verify(relList.get(1)).setResultPartitionCount(50);
	}

	@Test
	public void testSourceInferMinParallelism() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION(), 100);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION(), 1000);
		createRelList(2);
		RelMetadataQuery mq = mock(RelMetadataQuery.class);
		BatchExecScan scan = mock(BatchExecScan.class, RETURNS_DEEP_STUBS);
		when(scan.getCluster().getMetadataQuery()).thenReturn(mq);
		when(mq.getRowCount(scan)).thenReturn(50d);
		when(mq.getAverageRowSize(scan)).thenReturn(1.6 * ExecResourceUtil.SIZE_IN_MB);
		updateRel(0, scan);
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateRel(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(1);
		verify(relList.get(1)).setResultPartitionCount(50);
	}

	@Test
	public void testSourceInferMaxParallelism() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION(), 100);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION(), 1000);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM(), 100);
		createRelList(2);
		RelMetadataQuery mq = mock(RelMetadataQuery.class);
		BatchExecScan scan = mock(BatchExecScan.class, RETURNS_DEEP_STUBS);
		when(scan.getCluster().getMetadataQuery()).thenReturn(mq);
		when(mq.getRowCount(scan)).thenReturn(50000d);
		when(mq.getAverageRowSize(scan)).thenReturn(1d * ExecResourceUtil.SIZE_IN_MB);
		updateRel(0, scan);
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateRel(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(100);
		verify(relList.get(1)).setResultPartitionCount(50);
	}

	@Test
	public void testSourceInferParallelism() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION(), 100);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION(), 1000);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM(), 100);
		createRelList(2);
		RelMetadataQuery mq = mock(RelMetadataQuery.class);
		BatchExecScan scan = mock(BatchExecScan.class, RETURNS_DEEP_STUBS);
		when(scan.getCluster().getMetadataQuery()).thenReturn(mq);
		when(mq.getRowCount(scan)).thenReturn(5000d);
		when(mq.getAverageRowSize(scan)).thenReturn(12d * ExecResourceUtil.SIZE_IN_MB);
		updateRel(0, scan);
		BatchExecExchange execExchange = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateRel(1, execExchange);
		when(execExchange.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		connect(1, 0);
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(60);
		verify(relList.get(1)).setResultPartitionCount(50);
	}

	@Test
	public void testExchange() {
		/**
		 *   0, Source
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		createRelList(2);
		updateRel(0, mock(BatchExecScan.class));
		updateRel(1, mock(BatchExecExchange.class, RETURNS_DEEP_STUBS));
		when(((BatchExecExchange) relList.get(1)).getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		connect(1, 0);
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(50);
		verify(relList.get(1)).setResultPartitionCount(1);
	}

	@Test
	public void testValues() {
		/**
		 *   0, Values
		 *       |
		 *    1, Exchange
		 */
		tEnv.getConfig().getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.NONE.toString());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		createRelList(2);
		updateRel(0, mock(BatchExecValues.class));
		updateRel(1, mock(BatchExecExchange.class, RETURNS_DEEP_STUBS));
		when(((BatchExecExchange) relList.get(1)).getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		connect(1, 0);
		resultPartitionCalculator.calculate(relList.get(1));
		verify(relList.get(0)).setResultPartitionCount(1);
		verify(relList.get(1)).setResultPartitionCount(1);
	}
}
