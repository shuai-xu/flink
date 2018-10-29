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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel._
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.plan.cost.BatchExecCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.runtime.operator.sort.LimitOperator

class BatchExecLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    limitOffset: RexNode,
    limit: RexNode,
    val isGlobal: Boolean,
    description: String)
  extends Sort(
    cluster,
    traitSet,
    inp,
    traitSet.getTrait(RelCollationTraitDef.INSTANCE),
    limitOffset,
    limit)
  with RowBatchExecRel {

  val limitStart: Long = if (offset != null) RexLiteral.intValue(offset) else 0L
  val limitEnd: Long = if (limit != null) {
    RexLiteral.intValue(limit) + limitStart
  } else {
    Long.MaxValue
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    super.supplement(new BatchExecLimit(
      cluster,
      traitSet,
      newInput,
      offset,
      fetch,
      isGlobal,
      description))
  }


  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("offset", offsetToString)
      .item("limit", limitToString)
      .item("global", isGlobal)
      .itemIf("reuse_id", getReuseId, isReused)
  }

  private def offsetToString: String = {
    s"$limitStart"
  }

  private def limitToString: String = {
    if (limit != null) {
      s"${RexLiteral.intValue(limit)}"
    } else {
      "unlimited"
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(this)
    val cpuCost = COMPARE_CPU_COST * rowCount
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, 0)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[BaseRow] = {
    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)
    val inputType = input.getOutputType
    val operator = new LimitOperator(isGlobal, limitStart, limitEnd)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      inputType,
      resultPartitionCount)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = {
    s"${if (isGlobal) "Global" else "Local"}Limit(offset: $offsetToString, limit: $limitToString)"
  }

  override def toString: String = getOperatorName
}
