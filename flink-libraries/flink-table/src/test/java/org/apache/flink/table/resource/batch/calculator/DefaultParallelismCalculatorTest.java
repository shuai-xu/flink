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
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for BatchParallelismCalculator.
 */
public class DefaultParallelismCalculatorTest {

	private TableConfig tableConfig;
	private RelMetadataQuery mq;
	private BatchExecScan scanParallelism30 = mock(BatchExecScan.class);

	@Before
	public void setUp() {
		tableConfig = new TableConfig();
		mq = mock(RelMetadataQuery.class);
		tableConfig.getParameters().setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(), ExecResourceUtil.InferMode.ONLY_SOURCE.toString());
		tableConfig.getParameters().setInteger(TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION(), 100);
		tableConfig.getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 50);
		when(mq.getRowCount(scanParallelism30)).thenReturn(3000d);
		when(mq.getAverageRowSize(scanParallelism30)).thenReturn(4d);
	}

	@Test
	public void testOnlySource() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		when(shuffleStage0.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism30)));
		new BatchParallelismCalculator(mq, tableConfig).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(30, false);
	}

	@Test
	public void testSourceAndCalc() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(shuffleStage0.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(scanParallelism30, calc)));
		new BatchParallelismCalculator(mq, tableConfig).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(30, false);
	}

	@Test
	public void testNoSource() {
		ShuffleStage shuffleStage0 = mock(ShuffleStage.class);
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(shuffleStage0.getBatchExecRelSet()).thenReturn(getRelSet(Arrays.asList(calc)));
		new BatchParallelismCalculator(mq, tableConfig).calculate(shuffleStage0);
		verify(shuffleStage0).setResultParallelism(50, false);
	}

	private Set<RowBatchExecRel> getRelSet(List<RowBatchExecRel> rowBatchExecRelList) {
		Set<RowBatchExecRel> relSet = new HashSet<>();
		relSet.addAll(rowBatchExecRelList);
		return relSet;
	}
}
