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
package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, SingleRel}
import org.apache.calcite.sql.SqlRankFunction
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.util.RankLimit

class FlinkLogicalRank(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  child: RelNode,
  val rankFunction: SqlRankFunction,
  val partitionKey: ImmutableBitSet,
  val sortCollation: RelCollation,
  val rankLimit: RankLimit,
  rowType: RelDataType,
  val hasRowNumber: Boolean)
  extends SingleRel(cluster, traitSet, child)
    with FlinkLogicalRel {

  override def deriveRowType(): RelDataType = rowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalRank(
      cluster,
      traitSet,
      inputs.get(0),
      rankFunction,
      partitionKey,
      sortCollation,
      rankLimit,
      rowType,
      hasRowNumber)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    // compare to over window: one input at least one output(do not introduce retract amplification)
    // rank functions generally filters most of the input, output few records
    val inputRowCnt = mq.getRowCount(getInput)
    inputRowCnt * 0.01
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // by default, assume cost is proportional to number of rows
    val rowCount: Double = estimateRowCount(mq)
    val count = (rowType.getFieldCount - 1) * 1.0 / child.getRowType.getFieldCount
    planner.getCostFactory.makeCost(rowCount, rowCount * count, 0)
  }
}
