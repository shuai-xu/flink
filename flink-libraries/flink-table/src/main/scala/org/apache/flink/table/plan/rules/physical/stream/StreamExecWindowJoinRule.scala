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
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecWindowJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.runtime.join.WindowJoinUtil

import scala.collection.JavaConverters._

class StreamExecWindowJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecWindowJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val joinInfo = join.analyzeCondition

    val (windowBounds, remainingPreds) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      joinInfo.getRemaining(join.getCluster.getRexBuilder),
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      TableConfig.DEFAULT)

    // remaining predicate must not access time attributes
    val remainingPredsAccessTime = remainingPreds.isDefined &&
      WindowJoinUtil.accessesTimeAttribute(remainingPreds.get, join.getRowType)

    if (windowBounds.isDefined) {
      if (windowBounds.get.isEventTime) {
        // we cannot handle event-time window joins yet
        false
      } else {
        // Check that no event-time attributes are in the input.
        // The proc-time join implementation does ensure that record timestamp are correctly set.
        // It is always the timestamp of the later arriving record.
        // We rely on projection pushdown to remove unused attributes before the join.
        val rowTimeAttrInOutput = join.getRowType.getFieldList.asScala
          .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

        !remainingPredsAccessTime && !rowTimeAttrInOutput
      }
    } else {
      // the given join does not have valid window bounds. We cannot translate it.
      false
    }

  }

  override def convert(rel: RelNode): RelNode = {

    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.STREAMEXEC)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), FlinkConventions.STREAMEXEC)
    val joinInfo = join.analyzeCondition

    def toHashTraitByColumns(columns: util.Collection[_ <: Number], inputTraitSet: RelTraitSet) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns, requireStrict = true)
      }
      inputTraitSet.
        replace(FlinkConventions.STREAMEXEC).
        replace(distribution)
    }
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, join.getLeft.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, join.getRight.getTraitSet))

    val newLeft = RelOptRule.convert(convLeft, leftRequiredTrait)
    val newRight = RelOptRule.convert(convRight, rightRequiredTrait)
    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAMEXEC)

    val leftRowType = convLeft.getRowType
    val rightRowType = convRight.getRowType

    val (windowBounds, remainCondition) =
      WindowJoinUtil.extractWindowBoundsFromPredicate(
        joinInfo.getRemaining(join.getCluster.getRexBuilder),
        leftRowType.getFieldCount,
        join.getRowType,
        join.getCluster.getRexBuilder,
        TableConfig.DEFAULT)

    new StreamExecWindowJoin(
      rel.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.getJoinType,
      new BaseRowSchema(leftRowType),
      new BaseRowSchema(rightRowType),
      new BaseRowSchema(rel.getRowType),
      windowBounds.get.isEventTime,
      windowBounds.get.leftLowerBound,
      windowBounds.get.leftUpperBound,
      remainCondition,
      description)
  }
}

object StreamExecWindowJoinRule {
  val INSTANCE: RelOptRule = new StreamExecWindowJoinRule
}
