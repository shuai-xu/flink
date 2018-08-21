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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelFieldCollation, RelNode, SingleRel}
import org.apache.calcite.util.{ImmutableBitSet, Litmus}
import org.apache.flink.table.util.FlinkRelOptUtil

import scala.collection.JavaConversions._

/**
  * Top operator which can attach <i>with ties</i> attribution. Now this operator can only
  * support filtering of top 1 records, you can specify <i>with ties</i> to output records
  * with equal values of these columns together.
  *
  * <p>Now we involve the Segment function to this operator also, may split it out if
  * segment operator need to be an absolute operator, the Segment operator detect the
  * group change and mark the change through appending a segment-column, cause we merge this
  * function, so there is no need to make the segment-column anymore.
  *
  * @param groupKeys      group columns to partition the input records by.
  * @param fieldCollation field collation account to the aggregation column.
  * @param withTies columns with same value to output, will output 1 value if it is empty.
  */
abstract class SegmentTop(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    val groupKeys: ImmutableBitSet,
    val fieldCollation: RelFieldCollation,
    val withTies: ImmutableBitSet)
  extends SingleRel(cluster, traits, input) {

  def getGroupKeys: ImmutableBitSet = groupKeys

  def getFieldCollation: RelFieldCollation = fieldCollation

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    val fieldCnt = input.getRowType.getFieldCount
    for (g <- groupKeys) {
      if (g < 0 || g >= fieldCnt) {
        litmus.fail(s"Non exist group key: $g.")
      }
    }
    for (t <- withTies) {
      if (t < 0 || t >= fieldCnt) {
        litmus.fail(s"Non exist with ties column: $t.")
      }
    }
    if (fieldCollation.getFieldIndex < 0 || fieldCollation.getFieldIndex >= fieldCnt) {
      litmus.fail(s"Field collation index does not exist: $fieldCollation.")
    }
    litmus.succeed()
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this.getInput)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val groupNDV = mq.getDistinctRowCount(input, groupKeys, null)
    val numInputs = mq.getRowCount(input)
    if (groupNDV == null || numInputs == null) {
      return numInputs
    }
    val avgNumPerGroup = Math.max(numInputs / groupNDV
      * FlinkRelOptUtil.getTableConfig(this).selectivityOfSegmentTop, 1D)
    groupNDV * avgNumPerGroup
  }
}
