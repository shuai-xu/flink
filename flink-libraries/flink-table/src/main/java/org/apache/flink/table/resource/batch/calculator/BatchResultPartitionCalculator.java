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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.resource.ResourceCalculator;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculating resultPartitionCount with visiting every [[BatchExecRel]].
 */
public class BatchResultPartitionCalculator extends ResourceCalculator<ExecNode<?, ?>> {
	private static final Logger LOG = LoggerFactory.getLogger(BatchResultPartitionCalculator.class);
	private final RelMetadataQuery mq;
	private final int envParallelism;

	private BatchResultPartitionCalculator(BatchTableEnvironment tEnv, RelMetadataQuery mq) {
		super(tEnv);
		this.mq = mq;
		this.envParallelism = tEnv.streamEnv().getParallelism();
	}

	public static void calculate(BatchTableEnvironment tEnv, RelMetadataQuery mq, ExecNode<?, ?> rootExecRel) {
		new BatchResultPartitionCalculator(tEnv, mq).calculate(rootExecRel);
	}

	public void calculate(ExecNode<?, ?> node) {
		if (node.getResource().getParallelism() > 0) {
			return;
		}
		if (node instanceof BatchExecBoundedStreamScan) {
			calculateBoundedStreamScan((BatchExecBoundedStreamScan) node);
		} else if (node instanceof BatchExecTableSourceScan) {
			calculateTableSourceScan((BatchExecTableSourceScan) node);
		} else if (node instanceof BatchExecUnion) {
			calculateUnion((BatchExecUnion) node);
		} else if (node instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) node);
		} else if (node instanceof BatchExecJoinBase) {
			calculateJoin((BatchExecJoinBase) node);
		} else if (node instanceof SingleRel) {
			calculateSingle((SingleRel & BatchExecRel<?>) node);
		} else if (node instanceof BatchExecValues) {
			calculateValues((BatchExecValues) node);
		} else {
			throw new TableException("could not reach here. " + node.getClass());
		}
	}

	private void calculateBoundedStreamScan(BatchExecBoundedStreamScan boundedStreamScan) {
		StreamTransformation transformation = boundedStreamScan.getSourceTransformation(tEnv.streamEnv());
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = StreamExecutionEnvironment.getDefaultLocalParallelism();
		}
		boundedStreamScan.getResource().setParallelism(parallelism);
	}

	private void calculateTableSourceScan(BatchExecTableSourceScan tableSourceScan) {
		if (tableSourceScan.canLimitPushedDown()) {
			tableSourceScan.getResource().setParallelism(1);
		} else {
			StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.streamEnv());
			if (transformation.getMaxParallelism() > 0) {
				tableSourceScan.getResource().setParallelism(transformation.getMaxParallelism());
				return;
			}
			boolean infer = !ExecResourceUtil.getInferMode(tableConf).equals(ExecResourceUtil.InferMode.NONE);
			LOG.info("infer source partitions num: " + infer);
			if (infer) {
				double rowCount = mq.getRowCount(tableSourceScan);
				double io = rowCount * mq.getAverageRowSize(tableSourceScan);
				LOG.info("source row count is : " + rowCount);
				LOG.info("source data size is : " + io);
				long rowsPerPartition = ExecResourceUtil.getRelCountPerPartition(tableConf);
				long sizePerPartition = ExecResourceUtil.getSourceSizePerPartition(tableConf);
				int maxNum = ExecResourceUtil.getSourceMaxParallelism(tableConf);
				tableSourceScan.getResource().setParallelism(Math.min(maxNum,
						Math.max(
								(int) Math.max(
										io / sizePerPartition / ExecResourceUtil.SIZE_IN_MB,
										rowCount / rowsPerPartition),
								1)));
			} else {
				tableSourceScan.getResource().setParallelism(ExecResourceUtil
						.getSourceParallelism(tableConf, envParallelism));
			}

		}
	}

	private void calculateUnion(BatchExecUnion unionBatchExec) {
		calculateInputs(unionBatchExec);
		unionBatchExec.getResource().setParallelism(ExecResourceUtil.
				getOperatorDefaultParallelism(tableConf, envParallelism));
	}

	private void calculateExchange(BatchExecExchange exchangeBatchExec) {
		calculateInputs(exchangeBatchExec);
		if (exchangeBatchExec.getDistribution().getType() == RelDistribution.Type.SINGLETON) {
			exchangeBatchExec.getResource().setParallelism(1);
		} else {
			exchangeBatchExec.getResource().setParallelism(ExecResourceUtil.getOperatorDefaultParallelism(tableConf, envParallelism));
		}
	}

	private void calculateJoin(BatchExecJoinBase joinBatchExec) {
		calculateInputs(joinBatchExec);
		int rightResultPartitionCount =
				((BatchExecRel<?>) ((BiRel) joinBatchExec).getRight()).getResource().getParallelism();
		int leftResultPartitionCount =
				((BatchExecRel<?>) ((BiRel) joinBatchExec).getLeft()).getResource().getParallelism();

		if (((BiRel) joinBatchExec).getRight() instanceof BatchExecExchange &&
				((BatchExecExchange) ((BiRel) joinBatchExec).getRight()).getDistribution().getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
			joinBatchExec.getResource().setParallelism(leftResultPartitionCount);
		} else {
			joinBatchExec.getResource().setParallelism(rightResultPartitionCount);
		}
	}

	private <T extends SingleRel & BatchExecRel<?>> void calculateSingle(T singleRel) {
		calculateInputs(singleRel);
		RelNode inputRel = singleRel.getInput();
		(singleRel).getResource().setParallelism(((BatchExecRel<?>) inputRel).getResource().getParallelism());
	}

	private void calculateValues(BatchExecValues valuesBatchExec) {
		valuesBatchExec.getResource().setParallelism(1);
	}
}
