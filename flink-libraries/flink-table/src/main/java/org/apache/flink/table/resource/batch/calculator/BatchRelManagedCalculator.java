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
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
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
import org.apache.flink.table.util.ExecResourceUtil;

import org.apache.calcite.rel.RelNode;

import java.util.Map;

/**
 * Default managed memory calculator for relNode.
 */
public class BatchRelManagedCalculator extends BatchExecNodeVisitor {

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
	public void visit(BatchExecHashAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateHashWindowAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashJoinBase hashJoin) {
		visitChildren(hashJoin);
		int reservedMem = ExecResourceUtil.getHashJoinTableManagedMemory(tableConf);
		int preferMem = ExecResourceUtil.getHashJoinTableManagedPreferredMemory(tableConf);
		relResMap.get(hashJoin).setManagedMem(reservedMem, preferMem, preferMem);
	}

	@Override
	public void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		visitChildren(sortMergeJoin);
		int externalBufferMemoryMb = ExecResourceUtil.getExternalBufferManagedMemory(
				tableConf) * sortMergeJoin.getExternalBufferNum();
		int sortMemory = ExecResourceUtil.getSortBufferManagedMemory(tableConf);
		int reservedMemory = sortMemory * 2 + externalBufferMemoryMb;
		int preferSortMemory = ExecResourceUtil.getSortBufferManagedPreferredMemory(
				tableConf);
		int preferMemory = preferSortMemory * 2 + externalBufferMemoryMb;
		relResMap.get(sortMergeJoin).setManagedMem(reservedMemory, preferMemory, preferMemory);
	}

	@Override
	public void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			visitChildren(nestedLoopJoin);
			int externalBufferMemoryMb = ExecResourceUtil.getExternalBufferManagedMemory(tableConf);
			relResMap.get(nestedLoopJoin).setManagedMem(externalBufferMemoryMb, externalBufferMemoryMb, externalBufferMemoryMb);
		}
	}

	@Override
	public void visit(BatchExecSink<?> sink) {
		throw new TableException("could not reach sink here.");
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
		calculateHashWindowAgg(localHashAggregate);
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
			relResMap.get(overWindowAgg).setManagedMem(externalBufferMemory, externalBufferMemory, externalBufferMemory);
		}
	}

	@Override
	public void visit(BatchExecLimit limit) {
		calculateNoManagedMem(limit);
	}

	@Override
	public void visit(BatchExecSort sort) {
		visitChildren(sort);
		int reservedMemory = ExecResourceUtil.getSortBufferManagedMemory(tableConf);
		int preferMemory = ExecResourceUtil.getSortBufferManagedPreferredMemory(tableConf);
		int maxMemory = ExecResourceUtil.getSortBufferManagedMaxMemory(tableConf);
		relResMap.get(sort).setManagedMem(reservedMemory, preferMemory, maxMemory);
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

	private void visitChildren(BatchExecRel<?> batchExec) {
		for (RelNode batchExecRel: batchExec.getInputs()) {
			((BatchExecRel<?>) batchExecRel).accept(this);
		}
	}
}
