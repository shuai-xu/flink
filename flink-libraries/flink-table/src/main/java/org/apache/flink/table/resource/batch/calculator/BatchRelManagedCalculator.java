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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregateBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregateBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecNestedLoopJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecOverAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRank;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
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
import org.apache.flink.table.util.BatchExecRelVisitor;
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelNode;

import java.util.Map;

/**
 * Default managed memory calculator for relNode.
 */
public class BatchRelManagedCalculator implements BatchExecRelVisitor<Void> {

	private final Map<BatchExecRel<?>, RelResource> relResMap;
	private final Configuration tableConf;

	public BatchRelManagedCalculator(Configuration tableConf, Map<BatchExecRel<?>, RelResource> relResMap) {
		this.relResMap = relResMap;
		this.tableConf = tableConf;
	}

	private void calculateNoManagedMem(BatchExecRel<?> batchExecRel) {
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
		int	reservedMem = ExecResourceUtil.getHashAggManagedMemory(tableConf);
		int	preferMem = ExecResourceUtil.getHashAggManagedPreferredMemory(tableConf);
		relResMap.get(hashAgg).setManagedMem(reservedMem, preferMem, preferMem);
	}

	private void calculateHashWindowAgg(BatchExecHashWindowAggregateBase hashWindowAgg) {
		visitChildren(hashWindowAgg);
		int reservedMem = ExecResourceUtil.getHashAggManagedMemory(tableConf);
		int preferMem = ExecResourceUtil.getHashAggManagedPreferredMemory(tableConf);
		relResMap.get(hashWindowAgg).setManagedMem(reservedMem, preferMem, preferMem);
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
		int reservedMem = ExecResourceUtil.getHashJoinTableManagedMemory(tableConf);
		int preferMem = ExecResourceUtil.getHashJoinTableManagedPreferredMemory(tableConf);
		relResMap.get(hashJoin).setManagedMem(reservedMem, preferMem, preferMem);
		return null;
	}

	@Override
	public Void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		visitChildren(sortMergeJoin);
		int externalBufferMemoryMb = ExecResourceUtil.getExternalBufferManagedMemory(
				tableConf) * sortMergeJoin.getExternalBufferNum();
		int sortMemory = ExecResourceUtil.getSortBufferManagedMemory(tableConf);
		int reservedMemory = sortMemory * 2 + externalBufferMemoryMb;
		int preferSortMemory = ExecResourceUtil.getSortBufferManagedPreferredMemory(
				tableConf);
		int preferMemory = preferSortMemory * 2 + externalBufferMemoryMb;
		relResMap.get(sortMergeJoin).setManagedMem(reservedMemory, preferMemory, preferMemory);
		return null;
	}

	@Override
	public Void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			visitChildren(nestedLoopJoin);
			int externalBufferMemoryMb = ExecResourceUtil.getExternalBufferManagedMemory(tableConf);
			relResMap.get(nestedLoopJoin).setManagedMem(externalBufferMemoryMb, externalBufferMemoryMb, externalBufferMemoryMb);
		}
		return null;
	}

	@Override
	public Void visit(BatchExecSink<?> sink) {
		throw new TableException("could not reach sink here.");
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
			int externalBufferMemory = ExecResourceUtil.getExternalBufferManagedMemory(tableConf);
			relResMap.get(overWindowAgg).setManagedMem(externalBufferMemory, externalBufferMemory, externalBufferMemory);
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
		int reservedMemory = ExecResourceUtil.getSortBufferManagedMemory(tableConf);
		int preferMemory = ExecResourceUtil.getSortBufferManagedPreferredMemory(tableConf);
		int maxMemory = ExecResourceUtil.getSortBufferManagedMaxMemory(tableConf);
		relResMap.get(sort).setManagedMem(reservedMemory, preferMemory, maxMemory);
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
	public Void visit(BatchExecTemporalTableJoin joinTable) {
		calculateNoManagedMem(joinTable);
		return null;
	}

	@Override
	public Void visit(BatchExecRel<?> batchExec) {
		calculateNoManagedMem(batchExec);
		return null;
	}

	private void visitChildren(BatchExecRel<?> batchExec) {
		for (RelNode batchExecRel: batchExec.getInputs()) {
			((BatchExecRel<?>) batchExecRel).accept(this);
		}
	}
}
