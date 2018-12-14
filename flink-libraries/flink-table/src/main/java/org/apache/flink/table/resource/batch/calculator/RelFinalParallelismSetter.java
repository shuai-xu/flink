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
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;

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
	private Set<BatchExecRel<?>> calculatedRelSet = new HashSet<>();

	private RelFinalParallelismSetter(BatchTableEnvironment tEnv) {
		this.tEnv = tEnv;
	}

	public static void calculate(BatchTableEnvironment tEnv, BatchExecRel<?> rowBatchExecRel) {
		new RelFinalParallelismSetter(tEnv).calculate(rowBatchExecRel);
	}

	private void calculate(BatchExecRel<?> batchExecRel) {
		if (!calculatedRelSet.add(batchExecRel)) {
			return;
		}
		if (batchExecRel instanceof BatchExecTableSourceScan) {
			calculateTableSource((BatchExecTableSourceScan) batchExecRel);
		} else if (batchExecRel instanceof BatchExecBoundedStreamScan) {
			calculateBoundedStreamScan((BatchExecBoundedStreamScan) batchExecRel);
		} else if (batchExecRel instanceof SingleRel) {
			calculateSingle((SingleRel & BatchExecRel<?>) batchExecRel);
		} else if (batchExecRel instanceof BatchExecValues) {
			calculateValues((BatchExecValues) batchExecRel);
		} else {
			calculateInputs(batchExecRel);
		}
	}

	private void calculateTableSource(BatchExecTableSourceScan tableSourceScan) {
		if (tableSourceScan.canLimitPushedDown()) {
			tableSourceScan.setResultPartitionCount(1);
		} else {
			StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.streamEnv());
			if (transformation.getMaxParallelism() > 0) {
				tableSourceScan.setResultPartitionCount(transformation.getMaxParallelism());
			}
		}
	}

	private void calculateBoundedStreamScan(BatchExecBoundedStreamScan boundedStreamScan) {
		StreamTransformation transformation = boundedStreamScan.getSourceTransformation(tEnv.streamEnv());
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = StreamExecutionEnvironment.getDefaultLocalParallelism();
		}
		boundedStreamScan.setResultPartitionCount(parallelism);
	}

	private <T extends SingleRel & BatchExecRel<?>> void calculateSingle(T singleRel) {
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
		relNode.getInputs().forEach(i -> calculate((BatchExecRel<?>) i));
	}
}
