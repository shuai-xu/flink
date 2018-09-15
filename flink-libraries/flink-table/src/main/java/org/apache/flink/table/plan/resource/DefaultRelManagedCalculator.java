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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.BatchExecRelVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregateBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregateBase;
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
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSegmentTop;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.util.BatchExecResourceUtil;

import org.apache.calcite.rel.RelNode;

import java.util.Map;

/**
 * Default managed memory calculator for relNode.
 */
public class DefaultRelManagedCalculator implements BatchExecRelVisitor<Void> {

	private final Map<RowBatchExecRel, RelResource> relResMap;
	private final TableConfig tConfig;

	public DefaultRelManagedCalculator(TableConfig tConfig, Map<RowBatchExecRel, RelResource> relResMap) {
		this.relResMap = relResMap;
		this.tConfig = tConfig;
	}

	private void calculateNoManagedMem(RowBatchExecRel batchExecRel) {
		visitChildren(batchExecRel);
		relResMap.get(batchExecRel).setManagedMem(0, 0);
	}

	@Override
	public Void visit(BatchExecBoundedDataStreamScan boundedStreamScan) {
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

	private void calculateHashAgg(BatchExecHashAggregateBase hashAgg) {
		if (hashAgg.getGrouping().length == 0) {
			calculateNoManagedMem(hashAgg);
			return;
		}
		visitChildren(hashAgg);
		int	reservedMem = BatchExecResourceUtil.getHashAggManagedMemory(tConfig);
		int	preferMem = BatchExecResourceUtil.getHashAggManagedPreferredMemory(tConfig);
		relResMap.get(hashAgg).setManagedMem(reservedMem, preferMem);
	}

	private void calculateHashWindowAgg(BatchExecHashWindowAggregateBase hashWindowAgg) {
		visitChildren(hashWindowAgg);
		int reservedMem = BatchExecResourceUtil.getHashAggManagedMemory(tConfig);
		int preferMem = BatchExecResourceUtil.getHashAggManagedPreferredMemory(tConfig);
		relResMap.get(hashWindowAgg).setManagedMem(reservedMem, preferMem);
	}

	@Override
	public Void visit(BatchExecHashAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateHashWindowAgg(hashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecHashJoinBase hashJoin) {
		visitChildren(hashJoin);
		int reservedMem = BatchExecResourceUtil.getHashJoinTableManagedMemory(tConfig);
		int preferMem = BatchExecResourceUtil.getHashJoinTableManagedPreferredMemory(tConfig);
		relResMap.get(hashJoin).setManagedMem(reservedMem, preferMem);
		return null;
	}

	@Override
	public Void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		visitChildren(sortMergeJoin);
		int externalBufferMemoryMb = BatchExecResourceUtil.getExternalBufferManagedMemory(
				tConfig) * sortMergeJoin.getExternalBufferNum();
		int sortMemory = BatchExecResourceUtil.getSortBufferManagedMemory(tConfig);
		int reservedMemory = sortMemory * 2 + externalBufferMemoryMb;
		int preferSortMemory = BatchExecResourceUtil.getSortBufferManagedPreferredMemory(
				tConfig);
		int preferMemory = preferSortMemory * 2 + externalBufferMemoryMb;
		relResMap.get(sortMergeJoin).setManagedMem(reservedMemory, preferMemory);
		return null;
	}

	@Override
	public Void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			visitChildren(nestedLoopJoin);
			int externalBufferMemoryMb = BatchExecResourceUtil.getExternalBufferManagedMemory(tConfig);
			relResMap.get(nestedLoopJoin).setManagedMem(externalBufferMemoryMb, externalBufferMemoryMb);
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
		calculateHashWindowAgg(localHashAggregate);
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
			int externalBufferMemory = BatchExecResourceUtil.getExternalBufferManagedMemory(tConfig);
			relResMap.get(overWindowAgg).setManagedMem(externalBufferMemory, externalBufferMemory);
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
		int reservedMemory = BatchExecResourceUtil.getSortBufferManagedMemory(tConfig);
		int preferMemory = BatchExecResourceUtil.getSortBufferManagedPreferredMemory(tConfig);
		relResMap.get(sort).setManagedMem(reservedMemory, preferMemory);
		return null;
	}

	@Override
	public Void visit(BatchExecSortLimit sortLimit) {
		calculateNoManagedMem(sortLimit);
		return null;
	}

	@Override
	public Void visit(BatchExecSegmentTop segmentTop) {
		calculateNoManagedMem(segmentTop);
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
