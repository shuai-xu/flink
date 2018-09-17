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

package org.apache.flink.table.plan.resource;

import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;

import org.apache.calcite.rel.RelNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link ShuffleStageGenerator}.
 */
public class ShuffleStageGeneratorTest {

	private List<RowBatchExecRel> relList;

	@Test
	public void testGenerateShuffleStags() {
		/**
		 *
		 *    0, Source     1, Source
		 *             \     /
		 *             2, Union
		 *               /  \
		 *              /   3, Reuse
		 *             /     \
		 *        4, Calc   5, Calc
		 *           |        |
		 *    6, Exchange    7, Exchange
		 *            \      /
		 *              8, Join
		 *               |
		 *              9, Reuse
		 *               |
		 *              10, Calc
		 */
		createRelList(11);
		updateRel(2, mock(BatchExecUnion.class));
		updateRel(3, mock(BatchExecReused.class));
		updateRel(6, mock(BatchExecExchange.class));
		updateRel(7, mock(BatchExecExchange.class));
		updateRel(9, mock(BatchExecReused.class));
		connect(2, 0, 1);
		connect(3, 2);
		connect(4, 2);
		connect(5, 3);
		connect(6, 4);
		connect(7, 5);
		connect(8, 6, 7);
		connect(9, 8);
		connect(10, 9);

		Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap = ShuffleStageGenerator.generate(relList.get(10));
		//	assertEquals(5, new HashSet<>(relShuffleStageMap.values()).size());

		assertSameShuffleStage(relShuffleStageMap, 8, 10);
		assertSameShuffleStage(relShuffleStageMap, 0, 1, 4, 5);
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

	private void updateRel(int index, RowBatchExecRel rel) {
		relList.set(index, rel);
		when(rel.toString()).thenReturn("id: " + index);
	}

	private void createRelList(int num) {
		relList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			RowBatchExecRel rel = mock(RowBatchExecRel.class);
			when(rel.getInputs()).thenReturn(new ArrayList<>());
			when(rel.toString()).thenReturn("id: " + i);
			relList.add(rel);
		}
	}

	private void connect(int relIndex, int... inputRelIndexes) {
		List<RelNode> inputs = new ArrayList<>(inputRelIndexes.length);
		for (int inputIndex : inputRelIndexes) {
			inputs.add(relList.get(inputIndex));
		}
		when(relList.get(relIndex).getInputs()).thenReturn(inputs);
		if (relList.get(relIndex) instanceof BatchExecReused && inputRelIndexes.length == 1) {
			when(((BatchExecReused) relList.get(relIndex)).getInput()).thenReturn(relList.get(inputRelIndexes[0]));
		}
	}
}
