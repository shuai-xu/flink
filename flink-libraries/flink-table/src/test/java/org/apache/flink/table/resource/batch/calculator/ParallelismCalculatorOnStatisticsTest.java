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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;

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

	private Configuration tableConf;
	private RelMetadataQuery mq;
	private BatchExecScan scanParallelism30 = mock(BatchExecScan.class);
	private BatchExecScan scanParallelism1 = mock(BatchExecScan.class);
	private BatchExecScan scanParallelism42 = mock(BatchExecScan.class);
	private BatchExecScan scanParallelismMax = mock(BatchExecScan.class);
	private int envParallelism = 5;

	@Before
	public void setUp() {
		tableConf = new Configuration();
		mq = mock(RelMetadataQuery.class);
		tableConf.setLong(TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION, 100);
		tableConf.setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_MB_PER_PARTITION, 100);
		tableConf.setInteger(TableConfigOptions.SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MAX, 50);
		tableConf.setInteger(TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX, 100);
		tableConf.setInteger(ExecResourceUtil.SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_PARALLELISM, 5);
		tableConf.setString(TableConfigOptions.SQL_RESOURCE_INFER_MODE, ExecResourceUtil.InferMode.ALL.toString());

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
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(scanParallelism30)));
		new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(30, false);

		ShuffleStage shuffleStage1 = mock(ShuffleStage.class);
		when(shuffleStage1.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(scanParallelism1)));
		new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism).calculate(shuffleStage1);
		verify(shuffleStage1).setResultParallelism(1, false);

		ShuffleStage shuffleStage2 = mock(ShuffleStage.class);
		when(shuffleStage2.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(scanParallelism42)));
		new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism).calculate(shuffleStage2);
		verify(shuffleStage2).setResultParallelism(42, false);

		ShuffleStage shuffleStage3 = mock(ShuffleStage.class);
		when(shuffleStage3.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(scanParallelismMax)));
		new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism).calculate(shuffleStage3);
		verify(shuffleStage3).setResultParallelism(100, false);
	}

	@Test
	public void testStatics() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		ExecNode<?, ?> singleNode = mockSingleWithInputStatics(4000);
		ExecNode<?, ?> biNode = mockBiWithInputStatics(2000d, 1500d);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(scanParallelism30, singleNode, biNode)));
		new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(30, false);
		verify(shuffleStage0).setResultParallelism(40, false);
		verify(shuffleStage0).setResultParallelism(20, false);
	}

	@Test
	public void testShuffleStageFinal() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.isParallelismFinal()).thenReturn(true);
		ExecNode<?, ?> singleNode = mockSingleWithInputStatics(4000);
		ExecNode<?, ?> biNode = mockBiWithInputStatics(2000d, 1500d);
		when(shuffleStage0.getExecNodeSet()).thenReturn(getNodeSet(Arrays.asList(scanParallelism30, singleNode, biNode)));
		new ParallelismCalculatorOnStatistics(mq, tableConf, envParallelism).calculate(shuffleStage0);
		verify(shuffleStage0, never()).setResultParallelism(anyInt(), anyBoolean());
	}

	private Set<ExecNode<?, ?>> getNodeSet(List<ExecNode<?, ?>> nodeList) {
		Set<ExecNode<?, ?>> nodeSet = new HashSet<>();
		nodeSet.addAll(nodeList);
		return nodeSet;
	}

	private ExecNode<?, ?> mockSingleWithInputStatics(double inputRowCount) {
		BatchExecCalc node = mock(BatchExecCalc.class);
		BatchExecCalc input = mock(BatchExecCalc.class);
		when(input.getFlinkPhysicalRel()).thenReturn(input);
		when(node.getInputNodes()).thenReturn(Arrays.asList(input));
		when(mq.getRowCount(input)).thenReturn(inputRowCount);
		return node;
	}

	private ExecNode<?, ?> mockBiWithInputStatics(double leftInputRowCount, double rightInputRowCount) {
		BatchExecSortMergeJoin node = mock(BatchExecSortMergeJoin.class);
		BatchExecCalc leftInput = mock(BatchExecCalc.class);
		when(leftInput.getFlinkPhysicalRel()).thenReturn(leftInput);
		BatchExecCalc rightInput = mock(BatchExecCalc.class);
		when(rightInput.getFlinkPhysicalRel()).thenReturn(rightInput);
		when(node.getInputNodes()).thenReturn(Arrays.asList(leftInput, rightInput));
		when(mq.getRowCount(leftInput)).thenReturn(leftInputRowCount);
		when(mq.getRowCount(rightInput)).thenReturn(rightInputRowCount);
		return node;
	}
}
