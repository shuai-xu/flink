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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.JoinUtil.{joinConditionToString, joinSelectionToString, joinTypeToString}
import org.apache.flink.util.Preconditions.checkState

/**
  * RelNode for a stream join with [[org.apache.flink.table.api.functions.TemporalTableFunction]].
  */
class StreamExecTemporalTableJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinInfo: JoinInfo,
    leftSchema: BaseRowSchema,
    rightSchema: BaseRowSchema,
    schema: BaseRowSchema,
    joinType: FlinkJoinRelType,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with StreamExecRel {

  override def deriveRowType(): RelDataType = {
    schema.relDataType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString(schema.relDataType, joinCondition, getExpressionString))
      .item("join", joinSelectionToString(schema.relDataType))
      .item("joinType", joinTypeToString(joinType))
  }

  override def producesRetractions: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    checkState(inputs.size() == 2)
    new StreamExecTemporalTableJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinInfo,
      leftSchema,
      rightSchema,
      schema,
      joinType,
      ruleDescription)
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment)
  : StreamTransformation[BaseRow] = {
    throw new NotImplementedError()
  }
}
