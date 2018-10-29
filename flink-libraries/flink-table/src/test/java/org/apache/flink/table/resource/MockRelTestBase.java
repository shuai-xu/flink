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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base for test with mock rel list.
 */
public class MockRelTestBase {

	protected List<RowBatchExecRel> relList;

	protected void updateRel(int index, RowBatchExecRel rel) {
		relList.set(index, rel);
		when(rel.toString()).thenReturn("id: " + index);
		if (rel instanceof BatchExecScan) {
			when(((BatchExecScan) rel).getTableSourceResultPartitionNum(any())).thenReturn(new Tuple2<>(false, -1));
		}
	}

	protected void createRelList(int num) {
		relList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			RowBatchExecRel rel = mock(BatchExecCalc.class);
			when(rel.getInputs()).thenReturn(new ArrayList<>());
			when(rel.toString()).thenReturn("id: " + i);
			relList.add(rel);
		}
	}

	protected void connect(int relIndex, int... inputRelIndexes) {
		List<RelNode> inputs = new ArrayList<>(inputRelIndexes.length);
		for (int inputIndex : inputRelIndexes) {
			inputs.add(relList.get(inputIndex));
		}
		when(relList.get(relIndex).getInputs()).thenReturn(inputs);
		if (inputRelIndexes.length == 1 && relList.get(relIndex) instanceof SingleRel) {
			when(((SingleRel) relList.get(relIndex)).getInput()).thenReturn(relList.get(inputRelIndexes[0]));
		} else if (inputRelIndexes.length == 2 && relList.get(relIndex) instanceof BiRel) {
			when(((BiRel) relList.get(relIndex)).getLeft()).thenReturn(relList.get(inputRelIndexes[0]));
			when(((BiRel) relList.get(relIndex)).getRight()).thenReturn(relList.get(inputRelIndexes[1]));
		}
	}
}
