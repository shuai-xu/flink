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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
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
public class BatchResultPartitionCalculator extends ResourceCalculator<BatchExecRel<?>> {
	private static final Logger LOG = LoggerFactory.getLogger(BatchResultPartitionCalculator.class);

	public BatchResultPartitionCalculator(TableEnvironment tEnv) {
		super(tEnv);
	}

	public void calculate(BatchExecRel<?> batchExecRel) {
		if (batchExecRel.resultPartitionCount() > 0) {
			return;
		}
		if (batchExecRel instanceof BatchExecScan) {
			calculateSource((BatchExecScan) batchExecRel);
		} else if (batchExecRel instanceof BatchExecUnion) {
			calculateUnion((BatchExecUnion) batchExecRel);
		} else if (batchExecRel instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) batchExecRel);
		} else if (batchExecRel instanceof BatchExecJoinBase) {
			calculateJoin((BatchExecJoinBase) batchExecRel);
		} else if (batchExecRel instanceof SingleRel) {
			calculateSingle((SingleRel & BatchExecRel<?>) batchExecRel);
		} else if (batchExecRel instanceof BatchExecValues) {
			calculateValues((BatchExecValues) batchExecRel);
		} else {
			throw new TableException("could not reach here. " + batchExecRel.getClass());
		}
	}

	private void calculateSource(BatchExecScan scanBatchExec) {
		Tuple2<Boolean, Integer> result = scanBatchExec.getTableSourceResultPartitionNum(tEnv);
		// we expect sourceParallelism > 0 always, only for the mocked test case.
		if (result.f0 && result.f1 > 0) {
			// if parallelism locked, use set parallelism directly.
			scanBatchExec.setResultPartitionCount(result.f1);
			return;
		}
		boolean infer = !ExecResourceUtil.getInferMode(tConfig).equals(ExecResourceUtil.InferMode.NONE);
		LOG.info("infer source partitions num: " + infer);
		if (infer) {
			RelMetadataQuery mq = scanBatchExec.getCluster().getMetadataQuery();
			double rowCount = mq.getRowCount(scanBatchExec);
			double io = rowCount * mq.getAverageRowSize(scanBatchExec);
			LOG.info("source row count is : " + rowCount);
			LOG.info("source data size is : " + io);
			long rowsPerPartition = ExecResourceUtil.getRelCountPerPartition(tConfig);
			long sizePerPartition = ExecResourceUtil.getSourceSizePerPartition(tConfig);
			int maxNum = ExecResourceUtil.getSourceMaxParallelism(tConfig);
			scanBatchExec.setResultPartitionCount(Math.min(maxNum,
					Math.max(
							(int) Math.max(
									io / sizePerPartition / ExecResourceUtil.SIZE_IN_MB,
									rowCount / rowsPerPartition),
							1)));
		} else {
			scanBatchExec.setResultPartitionCount(ExecResourceUtil
					.getSourceParallelism(tConfig));
		}
	}

	private void calculateUnion(BatchExecUnion unionBatchExec) {
		calculateInputs(unionBatchExec);
		unionBatchExec.setResultPartitionCount(ExecResourceUtil.
				getOperatorDefaultParallelism(tConfig));
	}

	private void calculateExchange(BatchExecExchange exchangeBatchExec) {
		calculateInputs(exchangeBatchExec);
		if (exchangeBatchExec.getDistribution().getType() == RelDistribution.Type.SINGLETON) {
			exchangeBatchExec.setResultPartitionCount(1);
		} else {
			exchangeBatchExec.setResultPartitionCount(ExecResourceUtil.getOperatorDefaultParallelism(tConfig));
		}
	}

	private void calculateJoin(BatchExecJoinBase joinBatchExec) {
		calculateInputs(joinBatchExec);
		int rightResultPartitionCount =
				((BatchExecRel<?>) ((BiRel) joinBatchExec).getRight()).resultPartitionCount();
		int leftResultPartitionCount =
				((BatchExecRel<?>) ((BiRel) joinBatchExec).getLeft()).resultPartitionCount();

		if (((BiRel) joinBatchExec).getRight() instanceof BatchExecExchange &&
				((BatchExecExchange) ((BiRel) joinBatchExec).getRight()).getDistribution().getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
			joinBatchExec.setResultPartitionCount(leftResultPartitionCount);
		} else {
			joinBatchExec.setResultPartitionCount(rightResultPartitionCount);
		}
	}

	private <T extends SingleRel & BatchExecRel<?>> void calculateSingle(T singleRel) {
		calculateInputs(singleRel);
		RelNode inputRel = singleRel.getInput();
		(singleRel).setResultPartitionCount(((BatchExecRel<?>) inputRel).resultPartitionCount());
	}

	private void calculateValues(BatchExecValues valuesBatchExec) {
		valuesBatchExec.setResultPartitionCount(1);
	}
}
