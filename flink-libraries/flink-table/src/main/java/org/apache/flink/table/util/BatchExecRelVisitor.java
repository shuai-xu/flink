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

package org.apache.flink.table.util;

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

/**
 * Visitor pattern for traversing a tree of {@link BatchExecRel} objects.
 *
 * <p>TODO rename to BatchExecNodeVisitor and remove return type
 *
 * @param <R> Return type
 */
public interface BatchExecRelVisitor<R> {
	//~ Methods ----------------------------------------------------------------

	R visit(BatchExecBoundedStreamScan boundedStreamScan);

	R visit(BatchExecTableSourceScan scanTableSource);

	R visit(BatchExecValues values);

	R visit(BatchExecCalc calc);

	R visit(BatchExecCorrelate correlate);

	R visit(BatchExecExchange exchange);

	R visit(BatchExecExpand expand);

	R visit(BatchExecHashAggregate hashAggregate);

	R visit(BatchExecHashWindowAggregate hashAggregate);

	R visit(BatchExecHashJoinBase hashJoin);

	R visit(BatchExecSortMergeJoinBase sortMergeJoin);

	R visit(BatchExecNestedLoopJoinBase nestedLoopJoin);

	R visit(BatchExecLocalHashAggregate localHashAggregate);

	R visit(BatchExecSortAggregate sortAggregate);

	R visit(BatchExecLocalHashWindowAggregate localHashAggregate);

	R visit(BatchExecLocalSortAggregate localSortAggregate);

	R visit(BatchExecLocalSortWindowAggregate localSortAggregate);

	R visit(BatchExecSortWindowAggregate sortAggregate);

	R visit(BatchExecOverAggregate overWindowAgg);

	R visit(BatchExecLimit limit);

	R visit(BatchExecSort sort);

	R visit(BatchExecSortLimit sortLimit);

	R visit(BatchExecRank rank);

	R visit(BatchExecUnion union);

	R visit(BatchExecTemporalTableJoin joinTable);

	R visit(BatchExecSink<?> sink);

	// TODO change `BatchExecRel` to `BatchExecNode`
	R visit(BatchExecRel<?> batchExec);
}
