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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.batch.BatchExecRelVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinTable;
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
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
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
public class RelManagedCalculatorOnStatistics implements BatchExecRelVisitor<Void> {

	private final Map<RowBatchExecRel, RelResource> relResMap;
	private final TableConfig tConfig;
	private final Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap;
	private final RelMetadataQuery mq;

	public RelManagedCalculatorOnStatistics(
			TableConfig tConfig,
			Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap,
			RelMetadataQuery mq,
			Map<RowBatchExecRel, RelResource> relResMap) {
		this.tConfig = tConfig;
		this.relShuffleStageMap = relShuffleStageMap;
		this.mq = mq;
		this.relResMap = relResMap;
	}

	private int getResultPartitionCount(RowBatchExecRel batchExecRel) {
		return relShuffleStageMap.get(batchExecRel).getResultParallelism();
	}

	private void calculateNoManagedMem(RowBatchExecRel batchExecRel) {
		visitChildren(batchExecRel);
		relResMap.get(batchExecRel).setManagedMem(0, 0, 0);
	}

	@Override
	public Void visit(BatchExecBoundedStreamScan boundedStreamScan) {
		calculateNoManagedMem(boundedStreamScan);
		return null;
	}

	@Override
	public Void visit(BatchExecTableSourceScan scanTableSource) {
		calculateNoManagedMem(scanTableSource);
		return null;
	}

	@Override
	public Void visit(BatchExecValues values) {
		calculateNoManagedMem(values);
		return null;
	}

	@Override
	public Void visit(BatchExecCalc calc) {
		calculateNoManagedMem(calc);
		return null;
	}

	@Override
	public Void visit(BatchExecCorrelate correlate) {
		calculateNoManagedMem(correlate);
		return null;
	}

	@Override
	public Void visit(BatchExecExchange exchange) {
		visitChildren(exchange);
		return null;
	}

	@Override
	public Void visit(BatchExecReused reused) {
		visitChildren(reused);
		return null;
	}

	@Override
	public Void visit(BatchExecExpand expand) {
		calculateNoManagedMem(expand);
		return null;
	}

	private void calculateHashAgg(RowBatchExecRel hashAgg) {
		visitChildren(hashAgg);
		double memoryInBytes = BatchExecRel$.MODULE$.getBatchExecMemCost(hashAgg);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tConfig, (int) (memoryInBytes / ExecResourceUtil.SIZE_IN_MB / getResultPartitionCount(hashAgg)));
		relResMap.get(hashAgg).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
	}

	@Override
	public Void visit(BatchExecHashAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecHashJoinBase hashJoin) {
		visitChildren(hashJoin);
		int shuffleCount = hashJoin.shuffleBuildCount(mq);
		int memCostInMb = (int) (BatchExecRel$.MODULE$.getBatchExecMemCost(hashJoin) /
				shuffleCount / ExecResourceUtil.SIZE_IN_MB);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tConfig, memCostInMb / getResultPartitionCount(hashJoin));
		relResMap.get(hashJoin).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
		return null;
	}

	@Override
	public Void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		visitChildren(sortMergeJoin);
		int externalBufferMemoryMb = ExecResourceUtil.getExternalBufferManagedMemory(
				tConfig) * sortMergeJoin.getExternalBufferNum();
		double memoryInBytes = BatchExecRel$.MODULE$.getBatchExecMemCost(sortMergeJoin);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tConfig,
				(int) (memoryInBytes / ExecResourceUtil.SIZE_IN_MB / getResultPartitionCount(sortMergeJoin)));
		int reservedMemory = managedMem.f0 + externalBufferMemoryMb;
		int preferMemory = managedMem.f1 + externalBufferMemoryMb;
		int configMinMemory = ExecResourceUtil.getOperatorMinManagedMem(tConfig);
		int minSortMemory = (int) (SORTER_MIN_NUM_SORT_MEM * 2 / ExecResourceUtil.SIZE_IN_MB) + 1;
		Preconditions.checkArgument(configMinMemory >= externalBufferMemoryMb + minSortMemory,
				SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB +
						" should >= externalBufferMemoryMb(" +
						externalBufferMemoryMb +
						"), minSortMemory(" +
						minSortMemory + ").");
		relResMap.get(sortMergeJoin).setManagedMem(reservedMemory, preferMemory, managedMem.f2);
		return null;
	}

	@Override
	public Void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			visitChildren(nestedLoopJoin);
			int shuffleCount = nestedLoopJoin.shuffleBuildCount(mq);
			double memCostInMb = BatchExecRel$.MODULE$.getBatchExecMemCost(nestedLoopJoin) /
					shuffleCount / ExecResourceUtil.SIZE_IN_MB;
			Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
					tConfig, (int) (memCostInMb / getResultPartitionCount(nestedLoopJoin)));
			relResMap.get(nestedLoopJoin).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
		}
		return null;
	}

	@Override
	public Void visit(BatchExecLocalHashAggregate localHashAggregate) {
		calculateHashAgg(localHashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecSortAggregate sortAggregate) {
		calculateNoManagedMem(sortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		calculateHashAgg(localHashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalSortAggregate localSortAggregate) {
		calculateNoManagedMem(localSortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		calculateNoManagedMem(localSortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecSortWindowAggregate sortAggregate) {
		calculateNoManagedMem(sortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecOverAggregate overWindowAgg) {
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
			int externalBufferMemory = ExecResourceUtil.getExternalBufferManagedMemory(tConfig);
			relResMap.get(overWindowAgg).setManagedMem(externalBufferMemory, externalBufferMemory, externalBufferMemory, true);
		}
		return null;
	}

	@Override
	public Void visit(BatchExecLimit limit) {
		calculateNoManagedMem(limit);
		return null;
	}

	@Override
	public Void visit(BatchExecSort sort) {
		visitChildren(sort);
		double memoryInBytes = BatchExecRel$.MODULE$.getBatchExecMemCost(sort);
		Tuple3<Integer, Integer, Integer> managedMem = ExecResourceUtil.reviseAndGetInferManagedMem(
				tConfig, (int) (memoryInBytes / ExecResourceUtil.SIZE_IN_MB / getResultPartitionCount(sort)));
		relResMap.get(sort).setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
		return null;
	}

	@Override
	public Void visit(BatchExecSortLimit sortLimit) {
		calculateNoManagedMem(sortLimit);
		return null;
	}

	@Override
	public Void visit(BatchExecRank rank) {
		calculateNoManagedMem(rank);
		return null;
	}

	@Override
	public Void visit(BatchExecUnion union) {
		visitChildren(union);
		return null;
	}

	@Override
	public Void visit(BatchExecJoinTable joinTable) {
		calculateNoManagedMem(joinTable);
		return null;
	}

	@Override
	public Void visit(BatchExecRel<?> batchExec) {
		throw new TableException("could not reach here. " + batchExec.getClass());
	}

	private void visitChildren(RowBatchExecRel rowBatchExec) {
		for (RelNode batchExecRel: rowBatchExec.getInputs()) {
			((RowBatchExecRel) batchExecRel).accept(this);
		}
	}
}
