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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for ParallelismCalculatorOnStatistics.
 */
public class ParallelismCalculatorOnStatisticsTest {

	private TableConfig tableConfig;
	private RelMetadataQuery mq;
	private BatchExecScan scanParallelism30 = mock(BatchExecScan.class);
	private BatchExecScan scanParallelism1 = mock(BatchExecScan.class);
	private BatchExecScan scanParallelism42 = mock(BatchExecScan.class);
	private BatchExecScan scanParallelismMax = mock(BatchExecScan.class);

	@Before
	public void setUp() {
		tableConfig = new TableConfig();
		mq = mock(RelMetadataQuery.class);
		tableConfig.getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION(), 100);
		tableConfig.getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION(), 100);
		tableConfig.getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM(), 50);
		tableConfig.getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM(), 100);
		tableConfig.getParameters().setInteger(ExecResourceUtil.SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_PARALLELISM, 5);
		tableConfig.getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.ALL.toString());

		when(mq.getRowCount(scanParallelism30)).thenReturn(3000d);
		when(mq.getAverageRowSize(scanParallelism30)).thenReturn(4d);
		when(mq.getRowCount(scanParallelism1)).thenReturn(30d);
		when(mq.getAverageRowSize(scanParallelism1)).thenReturn(4d);
		when(mq.getRowCount(scanParallelism42)).thenReturn(3000d);
		when(mq.getAverageRowSize(scanParallelism42)).thenReturn(1.4d * ExecResourceUtil.SIZE_IN_MB);
		when(mq.getRowCount(scanParallelismMax)).thenReturn(30000d);
		when(mq.getAverageRowSize(scanParallelismMax)).thenReturn(1.4d * ExecResourceUtil.SIZE_IN_MB);
	}

	@Test
	public void testOnlySource() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism30)));
		new ParallelismCalculatorOnStatistics(mq, tableConfig).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(30, false);

		ShuffleStage shuffleStage1 = mock(ShuffleStage.class);
		when(shuffleStage1.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism1)));
		new ParallelismCalculatorOnStatistics(mq, tableConfig).calculate(shuffleStage1);
		verify(shuffleStage1).setResultParallelism(1, false);

		ShuffleStage shuffleStage2 = mock(ShuffleStage.class);
		when(shuffleStage2.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism42)));
		new ParallelismCalculatorOnStatistics(mq, tableConfig).calculate(shuffleStage2);
		verify(shuffleStage2).setResultParallelism(42, false);

		ShuffleStage shuffleStage3 = mock(ShuffleStage.class);
		when(shuffleStage3.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelismMax)));
		new ParallelismCalculatorOnStatistics(mq, tableConfig).calculate(shuffleStage3);
		verify(shuffleStage3).setResultParallelism(100, false);
	}

	@Test
	public void testStatics() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		RowBatchExecRel singleRel = mockSingleWithInputStatics(4000);
		RowBatchExecRel biRel = mockBiWithInputStatics(2000d, 1500d);
		when(shuffleStage0.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism30, singleRel, biRel)));
		new ParallelismCalculatorOnStatistics(mq, tableConfig).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(30, false);
		verify(shuffleStage0).setResultParallelism(40, false);
		verify(shuffleStage0).setResultParallelism(20, false);
	}

	@Test
	public void testShuffleStageFinal() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.isParallelismFinal()).thenReturn(true);
		RowBatchExecRel singleRel = mockSingleWithInputStatics(4000);
		RowBatchExecRel biRel = mockBiWithInputStatics(2000d, 1500d);
		when(shuffleStage0.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism30, singleRel, biRel)));
		new ParallelismCalculatorOnStatistics(mq, tableConfig).calculate(shuffleStage0);
		verify(shuffleStage0, never()).setResultParallelism(anyInt(), anyBoolean());
	}

	private Set<RowBatchExecRel> getRelSet(List<RowBatchExecRel> rowBatchExecRelList) {
		Set<RowBatchExecRel> relSet = new HashSet<>();
		relSet.addAll(rowBatchExecRelList);
		return relSet;
	}

	private RowBatchExecRel mockSingleWithInputStatics(double inputRowCount) {
		BatchExecCalc rel = mock(BatchExecCalc.class);
		RelNode input = mock(RelNode.class);
		when(rel.getInput()).thenReturn(input);
		when(mq.getRowCount(input)).thenReturn(inputRowCount);
		return rel;
	}

	private RowBatchExecRel mockBiWithInputStatics(double leftInputRowCount, double rightInputRowCount) {
		BatchExecSortMergeJoin rel = mock(BatchExecSortMergeJoin.class);
		RelNode leftInput = mock(RelNode.class);
		RelNode rightInput = mock(RelNode.class);
		when(rel.getLeft()).thenReturn(leftInput);
		when(rel.getRight()).thenReturn(rightInput);
		when(mq.getRowCount(leftInput)).thenReturn(leftInputRowCount);
		when(mq.getRowCount(rightInput)).thenReturn(rightInputRowCount);
		return rel;
	}
}
