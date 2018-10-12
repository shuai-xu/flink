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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.BatchExecRelVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCoGroupTableValuedAggregate;
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
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecReused;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
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
 * Cpu and heap memory calculator for relNode.
 */
public class RelCpuHeapMemCalculator implements BatchExecRelVisitor<Void> {

	private final Map<RowBatchExecRel, RelResource> relResMap;
	private final TableConfig tConfig;
	private final BatchTableEnvironment tableEnv;

	public RelCpuHeapMemCalculator(TableConfig tConfig, BatchTableEnvironment tableEnv,
		Map<RowBatchExecRel, RelResource> relResMap) {
		this.relResMap = relResMap;
		this.tConfig = tConfig;
		this.tableEnv = tableEnv;
	}

	private void calculateSource(BatchExecScan scanBatchExec) {
		// user may have set resource for source transformation.
		RelResource relResource = new RelResource();
		ResourceSpec sourceRes = scanBatchExec.getTableSourceResource(this.tableEnv);
		if (sourceRes == ResourceSpec.DEFAULT) {
			int heap = BatchExecResourceUtil.getSourceMem(tConfig);
			sourceRes = BatchExecResourceUtil.getResourceSpec(tConfig, heap);
		}
		ResourceSpec conversionRes = ResourceSpec.DEFAULT;
		if (scanBatchExec.needInternalConversion()) {
			conversionRes = BatchExecResourceUtil.getDefaultResourceSpec(tConfig);
		}
		ResourceSpec totalRes = sourceRes.merge(conversionRes);
		relResource.setCpu(totalRes.getCpuCores());
		relResource.setHeapMem(totalRes.getHeapMemory());
		relResMap.put(scanBatchExec, relResource);
		scanBatchExec.setResForSourceAndConversion(sourceRes, conversionRes);
	}

	private void calculateDefaultRel(RowBatchExecRel rel) {
		visitChildren(rel);
		double cpu = BatchExecResourceUtil.getCpu(tConfig);
		int heap = BatchExecResourceUtil.getDefaultHeapMem(tConfig);
		RelResource relResource = new RelResource();
		relResource.setCpu(cpu);
		relResource.setHeapMem(heap);
		relResMap.put(rel, relResource);
	}

	@Override
	public Void visit(BatchExecBoundedDataStreamScan boundedStreamScan) {
		calculateSource(boundedStreamScan);
		return null;
	}

	@Override
	public Void visit(BatchExecTableSourceScan scanTableSource) {
		calculateSource(scanTableSource);
		return null;
	}

	@Override
	public Void visit(BatchExecValues values) {
		calculateDefaultRel(values);
		return null;
	}

	@Override
	public Void visit(BatchExecCalc calc) {
		calculateDefaultRel(calc);
		return null;
	}

	@Override
	public Void visit(BatchExecCorrelate correlate) {
		calculateDefaultRel(correlate);
		return null;
	}

	@Override
	public Void visit(BatchExecExchange exchange) {
		calculateDefaultRel(exchange);
		return null;
	}

	@Override
	public Void visit(BatchExecReused reused) {
		visitChildren(reused);
		return null;
	}

	@Override
	public Void visit(BatchExecExpand expand) {
		calculateDefaultRel(expand);
		return null;
	}

	@Override
	public Void visit(BatchExecHashAggregate hashAggregate) {
		calculateDefaultRel(hashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateDefaultRel(hashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecHashJoinBase hashJoin) {
		calculateDefaultRel(hashJoin);
		return null;
	}

	@Override
	public Void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		calculateDefaultRel(sortMergeJoin);
		return null;
	}

	@Override
	public Void visit(BatchExecCoGroupTableValuedAggregate coAgg) {
		calculateDefaultRel(coAgg);
		return null;
	}

	@Override
	public Void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		calculateDefaultRel(nestedLoopJoin);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalHashAggregate localHashAggregate) {
		calculateDefaultRel(localHashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecSortAggregate sortAggregate) {
		calculateDefaultRel(sortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		calculateDefaultRel(localHashAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalSortAggregate localSortAggregate) {
		calculateDefaultRel(localSortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		calculateDefaultRel(localSortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecSortWindowAggregate sortAggregate) {
		calculateDefaultRel(sortAggregate);
		return null;
	}

	@Override
	public Void visit(BatchExecOverAggregate overWindowAgg) {
		calculateDefaultRel(overWindowAgg);
		return null;
	}

	@Override
	public Void visit(BatchExecLimit limit) {
		calculateDefaultRel(limit);
		return null;
	}

	@Override
	public Void visit(BatchExecSort sort) {
		calculateDefaultRel(sort);
		return null;
	}

	@Override
	public Void visit(BatchExecSortLimit sortLimit) {
		calculateDefaultRel(sortLimit);
		return null;
	}

	@Override
	public Void visit(BatchExecRank rank) {
		calculateDefaultRel(rank);
		return null;
	}

	@Override
	public Void visit(BatchExecUnion union) {
		visitChildren(union);
		return null;
	}

	@Override
	public Void visit(BatchExecJoinTable joinTable) {
		calculateDefaultRel(joinTable);
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
