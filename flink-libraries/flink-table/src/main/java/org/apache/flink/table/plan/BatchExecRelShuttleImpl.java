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

import org.apache.flink.table.api.TableException;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Basic implementation of {@link BatchExecRelVisitor} that calls
 * {@link BatchExecRel#accept(BatchExecRelVisitor)} on each child, and
 * {@link RelNode#copy(org.apache.calcite.plan.RelTraitSet, java.util.List)} if
 * any children change.
 */
public class BatchExecRelShuttleImpl implements BatchExecRelVisitor<BatchExecRel<?>> {

	@Override
	public BatchExecRel<?> visit(BatchExecBoundedDataStreamScan boundedStreamScan) {
		return visitInputs(boundedStreamScan);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecTableSourceScan scanTableSource) {
		return visitInputs(scanTableSource);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecValues values) {
		return visitInputs(values);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecCalc calc) {
		return visitInputs(calc);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecCorrelate correlate) {
		return visitInputs(correlate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecExchange exchange) {
		return visitInputs(exchange);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecReused reused) {
		return visitInputs(reused);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecExpand expand) {
		return visitInputs(expand);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecHashJoinBase hashJoin) {
		return visitInputs(hashJoin);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		return visitInputs(sortMergeJoin);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecHashAggregate hashAggregate) {
		return visitInputs(hashAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecHashWindowAggregate hashAggregate) {
		return visitInputs(hashAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		return visitInputs(nestedLoopJoin);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecLocalHashAggregate localHashAggregate) {
		return visitInputs(localHashAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		return visitInputs(localHashAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecLocalSortAggregate localSortAggregate) {
		return visitInputs(localSortAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecSortAggregate sortAggregate){
			return visitInputs(sortAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecSortWindowAggregate sortAggregate) {
		return visitInputs(sortAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		return visitInputs(localSortAggregate);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecOverAggregate overWindowAgg) {
		return visitInputs(overWindowAgg);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecLimit limit) {
		return visitInputs(limit);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecSort sort) {
		return visitInputs(sort);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecSortLimit sortLimit) {
		return visitInputs(sortLimit);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecSegmentTop segmentTop) {
		return visitInputs(segmentTop);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecUnion union) {
		return visitInputs(union);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecJoinTable joinTable) {
		return visitInputs(joinTable);
	}

	@Override
	public BatchExecRel<?> visit(BatchExecRel<?> other) {
		throw new TableException("Unknown BatchExecRel: " + other.getClass().getCanonicalName());
	}

	protected BatchExecRel<?> visitInputs(BatchExecRel<?> batchExecRel) {
		boolean[] update = {false};
		List<BatchExecRel<?>> clonedInputs = visitInputs(batchExecRel.getInputs(), update);
		if (update[0]) {
			return (BatchExecRel<?>) batchExecRel.copy(batchExecRel.getTraitSet(), new ArrayList<>(clonedInputs));
		} else {
			return batchExecRel;
		}
	}

	protected List<BatchExecRel<?>> visitInputs(List<? extends RelNode> inputs, boolean[] update) {
		com.google.common.collect.ImmutableList.Builder<BatchExecRel<?>> clonedInputs =
				com.google.common.collect.ImmutableList.builder();
		for (RelNode input : inputs) {
			BatchExecRel<?> clonedInput = ((BatchExecRel<?>) input).accept(this);
			if ((clonedInput != input) && (update != null)) {
				update[0] = true;
			}
			clonedInputs.add(clonedInput);
		}
		return clonedInputs.build();
	}

}
