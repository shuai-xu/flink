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

package org.apache.flink.table.resource.batch.autoconf;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecNestedLoopJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecOverAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRank;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel$;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTemporalTableJoin;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.resource.RelResource;
import org.apache.flink.table.resource.batch.ShuffleStage;
import org.apache.flink.table.util.ExecResourceUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.Map;

import static org.apache.flink.table.runtime.sort.BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM;
import static org.apache.flink.table.util.ExecResourceUtil.SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB;

/**
 * Managed memory calculator on statistics for relNode.
 */
public class RelManagedCalculatorOnStatistics extends BatchExecNodeVisitor {

	private final Map<BatchExecRel<?>, RelResource> relResMap;
	private final Configuration tableConf;
	private final Map<BatchExecRel<?>, ShuffleStage> relShuffleStageMap;
	private final RelMetadataQuery mq;

	public RelManagedCalculatorOnStatistics(
			Configuration tableConf,
			Map<BatchExecRel<?>, ShuffleStage> relShuffleStageMap,
			RelMetadataQuery mq,
			Map<BatchExecRel<?>, RelResource> relResMap) {
		this.tableConf = tableConf;
		this.relShuffleStageMap = relShuffleStageMap;
		this.mq = mq;
		this.relResMap = relResMap;
	}

	private int getResultPartitionCount(BatchExecRel<?> batchExecRel) {
		return relShuffleStageMap.get(batchExecRel).getResultParallelism();
	}

	private void calculateNoManagedMem(BatchExecRel<?> batchExecRel) {
		visitChildren(batchExecRel);
		relResMap.get(batchExecRel).setManagedMem(0, 0, 0);
	}

	@Override
	public void visit(BatchExecBoundedStreamScan boundedStreamScan) {
		calculateNoManagedMem(boundedStreamScan);
	}

	@Override
	public void visit(BatchExecTableSourceScan scanTableSource) {
		calculateNoManagedMem(scanTableSource);
	}

	@Override
	public void visit(BatchExecValues values) {
		calculateNoManagedMem(values);
	}

	@Override
	public void visit(BatchExecCalc calc) {
		calculateNoManagedMem(calc);
	}

	@Override
	public void visit(BatchExecCorrelate correlate) {
		calculateNoManagedMem(correlate);
	}

	@Override
	public void visit(BatchExecExchange exchange) {
		visitChildren(exchange);
	}

	@Override
	public void visit(BatchExecExpand expand) {
		calculateNoManagedMem(expand);
	}

	private void calculateHashAgg(BatchExecRel<?> hashAgg) {
		visitChildren(hashAgg);
		double memoryInBytes = BatchExecRel$.MODULE$.getBatchExecMemCost(hashAgg);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tableConf, (int) (memoryInBytes / ExecResourceUtil.SIZE_IN_MB / getResultPartitionCount(hashAgg)));
		relResMap.get(hashAgg).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
	}

	@Override
	public void visit(BatchExecHashAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashJoinBase hashJoin) {
		visitChildren(hashJoin);
		int shuffleCount = hashJoin.shuffleBuildCount(mq);
		int memCostInMb = (int) (BatchExecRel$.MODULE$.getBatchExecMemCost(hashJoin) /
				shuffleCount / ExecResourceUtil.SIZE_IN_MB);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tableConf, memCostInMb / getResultPartitionCount(hashJoin));
		relResMap.get(hashJoin).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
	}

	@Override
	public void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		visitChildren(sortMergeJoin);
		int externalBufferMemoryMb = ExecResourceUtil.getExternalBufferManagedMemory(
				tableConf) * sortMergeJoin.getExternalBufferNum();
		double memoryInBytes = BatchExecRel$.MODULE$.getBatchExecMemCost(sortMergeJoin);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tableConf,
				(int) (memoryInBytes / ExecResourceUtil.SIZE_IN_MB / getResultPartitionCount(sortMergeJoin)));
		int reservedMemory = managedMem.f0 + externalBufferMemoryMb;
		int preferMemory = managedMem.f1 + externalBufferMemoryMb;
		int configMinMemory = ExecResourceUtil.getOperatorMinManagedMem(tableConf);
		int minSortMemory = (int) (SORTER_MIN_NUM_SORT_MEM * 2 / ExecResourceUtil.SIZE_IN_MB) + 1;
		Preconditions.checkArgument(configMinMemory >= externalBufferMemoryMb + minSortMemory,
				SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB +
						" should >= externalBufferMemoryMb(" +
						externalBufferMemoryMb +
						"), minSortMemory(" +
						minSortMemory + ").");
		relResMap.get(sortMergeJoin).setManagedMem(reservedMemory, preferMemory, managedMem.f2);
	}

	@Override
	public void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			visitChildren(nestedLoopJoin);
			int shuffleCount = nestedLoopJoin.shuffleBuildCount(mq);
			double memCostInMb = BatchExecRel$.MODULE$.getBatchExecMemCost(nestedLoopJoin) /
					shuffleCount / ExecResourceUtil.SIZE_IN_MB;
			Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
					tableConf, (int) (memCostInMb / getResultPartitionCount(nestedLoopJoin)));
			relResMap.get(nestedLoopJoin).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
		}
	}

	@Override
	public void visit(BatchExecLocalHashAggregate localHashAggregate) {
		calculateHashAgg(localHashAggregate);
	}

	@Override
	public void visit(BatchExecSortAggregate sortAggregate) {
		calculateNoManagedMem(sortAggregate);
	}

	@Override
	public void visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		calculateHashAgg(localHashAggregate);
	}

	@Override
	public void visit(BatchExecLocalSortAggregate localSortAggregate) {
		calculateNoManagedMem(localSortAggregate);
	}

	@Override
	public void visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		calculateNoManagedMem(localSortAggregate);
	}

	@Override
	public void visit(BatchExecSortWindowAggregate sortAggregate) {
		calculateNoManagedMem(sortAggregate);
	}

	@Override
	public void visit(BatchExecOverAggregate overWindowAgg) {
		boolean[] needBufferList = overWindowAgg.needBufferDataToNeedResetAcc()._1;
		boolean needBuffer = false;
		for (boolean b : needBufferList) {
			if (b) {
				needBuffer = true;
				break;
			}
		}
		if (!needBuffer) {
			calculateNoManagedMem(overWindowAgg);
		} else {
			visitChildren(overWindowAgg);
			int externalBufferMemory = ExecResourceUtil.getExternalBufferManagedMemory(tableConf);
			relResMap.get(overWindowAgg).setManagedMem(externalBufferMemory, externalBufferMemory, externalBufferMemory, true);
		}
	}

	@Override
	public void visit(BatchExecLimit limit) {
		calculateNoManagedMem(limit);
	}

	@Override
	public void visit(BatchExecSort sort) {
		visitChildren(sort);
		double memoryInBytes = BatchExecRel$.MODULE$.getBatchExecMemCost(sort);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tableConf, (int) (memoryInBytes / ExecResourceUtil.SIZE_IN_MB / getResultPartitionCount(sort)));
		relResMap.get(sort).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
	}

	@Override
	public void visit(BatchExecSortLimit sortLimit) {
		calculateNoManagedMem(sortLimit);
	}

	@Override
	public void visit(BatchExecRank rank) {
		calculateNoManagedMem(rank);
	}

	@Override
	public void visit(BatchExecUnion union) {
		visitChildren(union);
	}

	@Override
	public void visit(BatchExecTemporalTableJoin joinTable) {
		calculateNoManagedMem(joinTable);
	}

	@Override
	public void visit(BatchExecSink<?> sink) {
		throw new TableException("could not reach sink here.");
	}

	private void visitChildren(BatchExecRel<?> rowBatchExec) {
		for (RelNode batchExecRel: rowBatchExec.getInputs()) {
			((BatchExecRel<?>) batchExecRel).accept(this);
		}
	}
}
