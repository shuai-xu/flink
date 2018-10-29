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
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.HashSet;
import java.util.Set;

/**
 * Set final parallelism if needed at the beginning time.
 */
public class RelFinalParallelismSetter {

	private final BatchTableEnvironment tEnv;
	private Set<RowBatchExecRel> calculatedRelSet = new HashSet<>();

	private RelFinalParallelismSetter(BatchTableEnvironment tEnv) {
		this.tEnv = tEnv;
	}

	public static void calculate(BatchTableEnvironment tEnv, RowBatchExecRel rowBatchExecRel) {
		new RelFinalParallelismSetter(tEnv).calculate(rowBatchExecRel);
	}

	private void calculate(RowBatchExecRel rowBatchExecRel) {
		if (!calculatedRelSet.add(rowBatchExecRel)) {
			return;
		}
		if (rowBatchExecRel instanceof BatchExecScan) {
			calculateSource((BatchExecScan) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof SingleRel) {
			calculateSingle((SingleRel & RowBatchExecRel) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecValues) {
			calculateValues((BatchExecValues) rowBatchExecRel);
		} else {
			calculateInputs(rowBatchExecRel);
		}
	}

	private void calculateSource(BatchExecScan scanBatchExec) {
		Tuple2<Boolean, Integer> result = scanBatchExec.getTableSourceResultPartitionNum(tEnv);
		// we expect sourceParallelism > 0 always, only for the mocked test case.
		if (result.f0 && result.f1 > 0) {
			// if parallelism locked, use set parallelism directly.
			scanBatchExec.setResultPartitionCount(result.f1);
		}
	}

	private <T extends SingleRel & RowBatchExecRel> void calculateSingle(T singleRel) {
		calculateInputs(singleRel);
		RelNode inputRel = singleRel.getInput();
		if (inputRel instanceof BatchExecExchange) {
			if (((BatchExecExchange) inputRel).getDistribution().getType() == RelDistribution.Type.SINGLETON) {
				singleRel.setResultPartitionCount(1);
				((BatchExecExchange) inputRel).setResultPartitionCount(1);
			}
		}
	}

	private void calculateValues(BatchExecValues valuesBatchExec) {
		valuesBatchExec.setResultPartitionCount(1);
	}

	private void calculateInputs(RelNode relNode) {
		relNode.getInputs().forEach(i -> calculate((RowBatchExecRel) i));
	}
}
