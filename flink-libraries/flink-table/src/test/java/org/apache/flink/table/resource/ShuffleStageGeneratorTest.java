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

package org.apache.flink.table.resource;

import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.resource.batch.ShuffleStageGenerator;

import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link ShuffleStageGenerator}.
 */
public class ShuffleStageGeneratorTest extends MockRelTestBase {

	@Test
	public void testGenerateShuffleStags() {
		/**
		 *
		 *    0, Source     1, Source
		 *             \     /
		 *             2, Union
		 *             /     \
		 *        3, Calc   4, Calc
		 *           |        |
		 *    5, Exchange    6, Exchange
		 *            \      /
		 *              7, Join
		 *               |
		 *              8, Calc
		 */
		createRelList(9);
		updateRel(2, mock(BatchExecUnion.class));
		updateRel(5, mock(BatchExecExchange.class));
		updateRel(6, mock(BatchExecExchange.class));
		connect(2, 0, 1);
		connect(3, 2);
		connect(4, 2);
		connect(5, 3);
		connect(6, 4);
		connect(7, 5, 6);
		connect(8, 7);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(8));

		assertSameShuffleStage(relShuffleStageMap, 7, 8);
		assertSameShuffleStage(relShuffleStageMap, 0, 1, 3, 4);
	}

	@Test
	public void testMultiOutput() {
		/**
		 *
		 *    0, Source     2, Source  4, Source   6, Source
		 *       |            |         |             |
		 *    1, Calc       3, Calc    5, Calc     7, Exchange
		 *            \     /      \   /       \    /
		 *            8, Join     9, Join     10, Join
		 *             \          /       \    /
		 *              \   12, Exchange  \   /
		 *               \      /         \  /
		 *                 11, Join      13, Union
		 *                         \      |
		 *                15, Exchange   14, Calc
		 *                           \   /
		 *                           16, Join
		 */
		createRelList(17);
		updateRel(7, mock(BatchExecExchange.class));
		updateRel(12, mock(BatchExecExchange.class));
		updateRel(13, mock(BatchExecUnion.class));
		updateRel(15, mock(BatchExecExchange.class));
		connect(1, 0);
		connect(3, 2);
		connect(5, 4);
		connect(7, 6);
		connect(8, 1, 3);
		connect(9, 3, 5);
		connect(10, 5, 7);
		connect(12, 9);
		connect(11, 8, 12);
		connect(13, 9, 10);
		connect(14, 13);
		connect(15, 11);
		connect(16, 15, 14);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(16));

		assertSameShuffleStage(relShuffleStageMap, 0, 1, 8, 3, 2, 9, 5, 4, 10, 11, 14, 16);
		assertSameShuffleStage(relShuffleStageMap, 6);
	}

	@Test
	public void testWithFinalParallelism() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc  6, Source
		 *             \     /     /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createRelList(7);
		RowBatchExecRel scan0 = mock(BatchExecScan.class);
		when(scan0.resultPartitionCount()).thenReturn(10);
		RowBatchExecRel scan1 = mock(BatchExecScan.class);
		when(scan1.resultPartitionCount()).thenReturn(11);
		updateRel(0, scan0);
		updateRel(2, scan1);
		updateRel(4, mock(BatchExecUnion.class));
		updateRel(5, mock(BatchExecCalc.class));
		updateRel(6, mock(BatchExecScan.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3, 6);
		connect(5, 4);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(5));

		assertSameShuffleStage(relShuffleStageMap, 0, 1);
		assertSameShuffleStage(relShuffleStageMap, 2, 3, 6, 5);
	}

	@Test
	public void testWithFinalParallelism1() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc
		 *             \     /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createRelList(7);
		RowBatchExecRel scan0 = mock(BatchExecScan.class);
		when(scan0.resultPartitionCount()).thenReturn(10);
		RowBatchExecRel scan1 = mock(BatchExecScan.class);
		when(scan1.resultPartitionCount()).thenReturn(11);
		updateRel(0, scan0);
		updateRel(2, scan1);
		updateRel(4, mock(BatchExecUnion.class));
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(calc.resultPartitionCount()).thenReturn(12);
		updateRel(5, calc);
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3);
		connect(5, 4);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(5));

		assertSameShuffleStage(relShuffleStageMap, 0, 1);
		assertSameShuffleStage(relShuffleStageMap, 2, 3);
		assertSameShuffleStage(relShuffleStageMap, 5);
	}

	@Test
	public void testWithFinalParallelism2() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *       |         3, Exchange
		 *       |            |
		 *    1, Calc      4, Calc
		 *             \     /
		 *               5, Union
		 *                 |
		 *               6, Calc
		 */
		createRelList(7);
		RowBatchExecRel scan0 = mock(BatchExecScan.class);
		when(scan0.resultPartitionCount()).thenReturn(10);
		RowBatchExecRel scan1 = mock(BatchExecScan.class);
		when(scan1.resultPartitionCount()).thenReturn(11);
		updateRel(0, scan0);
		updateRel(2, scan1);
		updateRel(3, mock(BatchExecExchange.class));
		BatchExecCalc calc = mock(BatchExecCalc.class);
		when(calc.resultPartitionCount()).thenReturn(1);
		updateRel(4, calc);
		updateRel(5, mock(BatchExecUnion.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 3);
		connect(5, 1, 4);
		connect(6, 5);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(6));

		assertSameShuffleStage(relShuffleStageMap, 0, 1, 6);
		assertSameShuffleStage(relShuffleStageMap, 2);
		assertSameShuffleStage(relShuffleStageMap, 4);
	}

	@Test
	public void testWithFinalParallelism3() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc  6, Source   7,Source
		 *             \     /     /             /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createRelList(8);
		RowBatchExecRel scan0 = mock(BatchExecScan.class);
		when(scan0.resultPartitionCount()).thenReturn(11);
		RowBatchExecRel scan1 = mock(BatchExecScan.class);
		when(scan1.resultPartitionCount()).thenReturn(5);
		updateRel(0, scan0);
		updateRel(2, scan1);
		BatchExecUnion union4 = mock(BatchExecUnion.class);
		when(union4.resultPartitionCount()).thenReturn(5);
		updateRel(4, union4);
		updateRel(5, mock(BatchExecCalc.class));
		updateRel(6, mock(BatchExecScan.class));
		updateRel(7, mock(BatchExecScan.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3, 6, 7);
		connect(5, 4);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(5));

		assertSameShuffleStage(relShuffleStageMap, 0, 1);
		assertSameShuffleStage(relShuffleStageMap, 2, 3, 6, 5, 7);
	}

	@Test
	public void testWithFinalParallelism4() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc
		 *             \     /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createRelList(6);
		RowBatchExecRel scan0 = mock(BatchExecScan.class);
		when(scan0.resultPartitionCount()).thenReturn(11);
		RowBatchExecRel scan1 = mock(BatchExecScan.class);
		when(scan1.resultPartitionCount()).thenReturn(5);
		updateRel(0, scan0);
		updateRel(2, scan1);
		BatchExecUnion union4 = mock(BatchExecUnion.class);
		when(union4.resultPartitionCount()).thenReturn(3);
		updateRel(4, union4);
		updateRel(5, mock(BatchExecCalc.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3);
		connect(5, 4);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(5));

		assertSameShuffleStage(relShuffleStageMap, 0, 1);
		assertSameShuffleStage(relShuffleStageMap, 2, 3);
		assertSameShuffleStage(relShuffleStageMap, 5);
	}

	private void assertSameShuffleStage(Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap, int ... relIndexes) {
		Set<RowBatchExecRel> relSet = new HashSet<>();
		for (int index : relIndexes) {
			relSet.add(relList.get(index));
		}
		for (int index : relIndexes) {
			assertNotNull("shuffleStage should not be null. rel index: " + index, relShuffleStageMap.get(relList.get(index)));
			assertEquals("rel index: " + index, relSet, relShuffleStageMap.get(relList.get(index)).getBatchExecRelSet());
		}
	}
}
