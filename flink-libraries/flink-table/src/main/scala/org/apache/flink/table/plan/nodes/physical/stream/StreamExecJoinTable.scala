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

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonJoinTable
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.sources.{DimensionTableSource, IndexKey}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexNode, RexProgram}
import org.apache.calcite.util.mapping.IntPair

import java.util

/**
 * Flink RelNode which matches along with stream joins a dimension table
 */
class StreamExecJoinTable(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputSchema: BaseRowSchema,
    input: RelNode,
    tableSource: DimensionTableSource[_],
    tableSourceSchema: BaseRowSchema,
    calcProgram: Option[RexProgram],
    period: Option[RexNode],
    keyPairs: util.List[IntPair],
    constantKeys: util.Map[Int, Tuple2[InternalType, Object]],
    joinCondition: Option[RexNode],
    checkedIndex: IndexKey,
    schema: BaseRowSchema,
    joinInfo: JoinInfo,
    joinType: JoinRelType,
    ruleDescription: String)
  extends CommonJoinTable(
    cluster,
    traitSet,
    inputSchema,
    input,
    tableSource,
    tableSourceSchema,
    calcProgram,
    period,
    keyPairs,
    constantKeys,
    joinCondition,
    checkedIndex,
    schema,
    joinInfo,
    joinType,
    ruleDescription)
  with RowStreamExecRel {

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    translateToPlanInternal(
      getInput.asInstanceOf[RowStreamExecRel].translateToPlan(tableEnv),
      tableEnv.execEnv,
      tableEnv.getConfig)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecJoinTable(
      cluster,
      traitSet,
      inputSchema,
      inputs.get(0),
      tableSource,
      tableSourceSchema,
      calcProgram,
      period,
      keyPairs,
      constantKeys,
      joinCondition,
      checkedIndex,
      schema,
      joinInfo,
      joinType,
      ruleDescription)
  }
}
