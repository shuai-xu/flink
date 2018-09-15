/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan;

import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedDataStreamScan;
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

import org.apache.calcite.rel.RelNode;

/**
 * Default implementation of {@link BatchExecRelVisitor},
 * which visits each node but does nothing while it's there.
 *
 * @param <R> Return type from each {@code visitXxx} method.
 */
public class BatchExecRelVisitorImpl<R> implements BatchExecRelVisitor<R> {

	@Override
	public R visit(BatchExecBoundedDataStreamScan boundedStreamScan) {
		return visitInputs(boundedStreamScan);
	}

	@Override
	public R visit(BatchExecTableSourceScan scanTableSource) {
		return visitInputs(scanTableSource);
	}

	@Override
	public R visit(BatchExecValues values) {
		return visitInputs(values);
	}

	@Override
	public R visit(BatchExecCalc calc) {
		return visitInputs(calc);
	}

	@Override
	public R visit(BatchExecCorrelate correlate) {
		return visitInputs(correlate);
	}

	@Override
	public R visit(BatchExecExchange exchange) {
		return visitInputs(exchange);
	}

	@Override
	public R visit(BatchExecReused reused) {
		return visitInputs(reused);
	}

	@Override
	public R visit(BatchExecExpand expand) {
		return visitInputs(expand);
	}

	@Override
	public R visit(BatchExecHashAggregate hashAggregate) {
		return visitInputs(hashAggregate);
	}

	@Override
	public R visit(BatchExecHashWindowAggregate hashAggregate) {
		return visitInputs(hashAggregate);
	}

	@Override
	public R visit(BatchExecHashJoinBase hashJoin) {
		return visitInputs(hashJoin);
	}

	@Override
	public R visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		return visitInputs(sortMergeJoin);
	}

	@Override
	public R visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		return visitInputs(nestedLoopJoin);
	}

	@Override
	public R visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		return visitInputs(localHashAggregate);
	}

	@Override
	public R visit(BatchExecLocalSortAggregate localSortAggregate) {
		return visitInputs(localSortAggregate);
	}

	@Override
	public R visit(BatchExecLocalHashAggregate localHashAggregate) {
		return visitInputs(localHashAggregate);
	}

	@Override
	public R visit(BatchExecOverAggregate overWindowAgg) {
		return visitInputs(overWindowAgg);
	}

	@Override
	public R visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		return visitInputs(localSortAggregate);
	}

	@Override
	public R visit(BatchExecSortAggregate sortAggregate) {
		return visitInputs(sortAggregate);
	}

	@Override
	public R visit(BatchExecSortWindowAggregate sortAggregate) {
		return visitInputs(sortAggregate);
	}

	@Override
	public R visit(BatchExecLimit limit) {
		return visitInputs(limit);
	}

	@Override
	public R visit(BatchExecSort sort) {
		return visitInputs(sort);
	}

	@Override
	public R visit(BatchExecSortLimit sortLimit) {
		return visitInputs(sortLimit);
	}

	@Override
	public R visit(BatchExecSegmentTop segmentTop) {
		return visitInputs(segmentTop);
	}

	@Override
	public R visit(BatchExecRank rank) {
		return visitInputs(rank);
	}

	@Override
	public R visit(BatchExecUnion union) {
		return visitInputs(union);
	}

	@Override
	public R visit(BatchExecJoinTable joinTable) {
		return visitInputs(joinTable);
	}

	@Override
	public R visit(BatchExecRel<?> batchExec) {
		return visitInputs(batchExec);
	}

	protected R visitInputs(BatchExecRel<?> batchExecRel) {
		R r = null;
		for (RelNode input : batchExecRel.getInputs()) {
			r = ((BatchExecRel<?>) input).accept(this);
		}
		return r;
	}
}
