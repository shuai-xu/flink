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

package org.apache.flink.table.plan.rules.physical.stream

import java.util

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTemporalTableJoin
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTemporalTableJoin
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.runtime.join.WindowJoinUtil

class StreamExecTemporalTableJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalTemporalTableJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecTemporalTableJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalTemporalTableJoin = call.rel(0)
    val joinInfo = join.analyzeCondition

    val (windowBounds, remainingPreds) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      joinInfo.getRemaining(join.getCluster.getRexBuilder),
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      TableConfig.DEFAULT)

    windowBounds.isEmpty && join.getJoinType == JoinRelType.INNER
  }

  override def convert(rel: RelNode): RelNode = {
    val temporalJoin = rel.asInstanceOf[FlinkLogicalTemporalTableJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAMEXEC)
    val joinInfo = temporalJoin.analyzeCondition

    def toHashTraitByColumns(columns: util.Collection[_ <: Number], inputTraitSets: RelTraitSet) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSets.
      replace(FlinkConventions.STREAMEXEC).
      replace(distribution)
    }
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, temporalJoin.getLeft.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, temporalJoin.getRight.getTraitSet))

    val convLeft: RelNode = RelOptRule.convert(temporalJoin.getInput(0), leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(temporalJoin.getInput(1), rightRequiredTrait)

    val leftRowSchema = new BaseRowSchema(convLeft.getRowType)
    val rightRowSchema = new BaseRowSchema(convRight.getRowType)

    new StreamExecTemporalTableJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      temporalJoin.getCondition,
      joinInfo,
      leftRowSchema,
      rightRowSchema,
      new BaseRowSchema(rel.getRowType),
      FlinkJoinRelType.toFlinkJoinRelType(temporalJoin.getJoinType),
      description)
  }
}

object StreamExecTemporalTableJoinRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalTableJoinRule
}
