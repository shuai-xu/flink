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

package org.apache.flink.table.plan.resource.calculator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.util.BatchExecResourceUtil;

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
public class DefaultResultPartitionCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultResultPartitionCalculator.class);

	private final TableConfig tConfig;
	private final BatchTableEnvironment tEnv;

	private DefaultResultPartitionCalculator(BatchTableEnvironment tEnv) {
		this.tEnv = tEnv;
		this.tConfig = tEnv.getConfig();
	}

	public static void calculate(BatchTableEnvironment tEnv, RowBatchExecRel rowBatchExecRel) {
		new DefaultResultPartitionCalculator(tEnv).calculate(rowBatchExecRel);
	}

	private void calculate(RowBatchExecRel rowBatchExecRel) {
		if (rowBatchExecRel.resultPartitionCount() > 0) {
			return;
		}
		if (rowBatchExecRel instanceof BatchExecScan) {
			calculateSource((BatchExecScan) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecUnion) {
			calculateUnion((BatchExecUnion) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecJoinBase) {
			calculateJoin((BatchExecJoinBase) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof SingleRel) {
			calculateSingle((SingleRel & RowBatchExecRel) rowBatchExecRel);
		} else if (rowBatchExecRel instanceof BatchExecValues) {
			calculateValues((BatchExecValues) rowBatchExecRel);
		} else {
			throw new TableException("could not reach here. " + rowBatchExecRel.getClass());
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
		boolean infer = !BatchExecResourceUtil.getInferMode(tConfig).equals(BatchExecResourceUtil.InferMode.NONE);
		LOG.info("infer source partitions num: " + infer);
		if (infer) {
			RelMetadataQuery mq = scanBatchExec.getCluster().getMetadataQuery();
			double rowCount = mq.getRowCount(scanBatchExec);
			double io = rowCount * mq.getAverageRowSize(scanBatchExec);
			LOG.info("source row count is : " + rowCount);
			LOG.info("source data size is : " + io);
			long rowsPerPartition = BatchExecResourceUtil.getRelCountPerPartition(tConfig);
			long sizePerPartition = BatchExecResourceUtil.getSourceSizePerPartition(tConfig);
			int maxNum = BatchExecResourceUtil.getSourceMaxParallelism(tConfig);
			scanBatchExec.setResultPartitionCount(Math.min(maxNum,
					Math.max(
							(int) Math.max(
									io / sizePerPartition / BatchExecResourceUtil.SIZE_IN_MB,
									rowCount / rowsPerPartition),
							1)));
		} else {
			scanBatchExec.setResultPartitionCount(BatchExecResourceUtil
					.getSourceParallelism(tConfig));
		}
	}

	private void calculateUnion(BatchExecUnion unionBatchExec) {
		calculateInputs(unionBatchExec);
		unionBatchExec.setResultPartitionCount(BatchExecResourceUtil.
				getOperatorDefaultParallelism(tConfig));
	}

	private void calculateExchange(BatchExecExchange exchangeBatchExec) {
		calculateInputs(exchangeBatchExec);
		if (exchangeBatchExec.getDistribution().getType() == RelDistribution.Type.SINGLETON) {
			exchangeBatchExec.setResultPartitionCount(1);
		} else {
			exchangeBatchExec.setResultPartitionCount(BatchExecResourceUtil.getOperatorDefaultParallelism(tConfig));
		}
	}

	private void calculateJoin(BatchExecJoinBase joinBatchExec) {
		calculateInputs(joinBatchExec);
		int rightResultPartitionCount =
				((RowBatchExecRel) ((BiRel) joinBatchExec).getRight()).resultPartitionCount();
		int leftResultPartitionCount =
				((RowBatchExecRel) ((BiRel) joinBatchExec).getLeft()).resultPartitionCount();

		if (((BiRel) joinBatchExec).getRight() instanceof BatchExecExchange &&
				((BatchExecExchange) ((BiRel) joinBatchExec).getRight()).getDistribution().getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
			joinBatchExec.setResultPartitionCount(leftResultPartitionCount);
		} else {
			joinBatchExec.setResultPartitionCount(rightResultPartitionCount);
		}
	}

	private <T extends SingleRel & RowBatchExecRel> void calculateSingle(T singleRel) {
		calculateInputs(singleRel);
		RelNode inputRel = singleRel.getInput();
		(singleRel).setResultPartitionCount(((RowBatchExecRel) inputRel).resultPartitionCount());
	}

	private void calculateValues(BatchExecValues valuesBatchExec) {
		valuesBatchExec.setResultPartitionCount(1);
	}

	private void calculateInputs(RelNode relNode) {
		relNode.getInputs().forEach(i -> calculate((RowBatchExecRel) i));
	}

}
