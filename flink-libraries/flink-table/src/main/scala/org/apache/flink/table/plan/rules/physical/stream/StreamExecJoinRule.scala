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
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecJoin
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalDimensionTableSourceScan, FlinkLogicalJoin}
import org.apache.flink.table.runtime.join.WindowJoinUtil

import scala.collection.JavaConversions._

class StreamExecJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val right = join.getRight

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

    val rowTimeAttrInOutput = join.getRowType.getFieldList
      .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    // joins require an equality condition
    // or a conjunctive predicate with at least one equality condition
    // and disable outer joins with non-equality predicates(see FLINK-5520)
    // And do not accept a FlinkLogicalDimensionTableSourceScan as right input
    !windowBounds.isDefined && !remainingPredsAccessTime && !rowTimeAttrInOutput &&
      !right.isInstanceOf[FlinkLogicalDimensionTableSourceScan]
  }

  override def convert(rel: RelNode): RelNode = {
    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    lazy val (joinInfo, filterNulls) = {
      val filterNulls = new util.ArrayList[java.lang.Boolean]
      val joinInfo = JoinInfo.of(join.getLeft, join.getRight, join.getCondition, filterNulls)
      (joinInfo, filterNulls.map(_.booleanValue()).toArray)
    }
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
      toHashTraitByColumns(joinInfo.leftKeys, join.getLeft.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, join.getRight.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAMEXEC)

    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), rightRequiredTrait)

    new StreamExecJoin(
      rel.getCluster,
      providedTraitSet,
      convLeft,
      convRight,
      rel.getRowType,
      join.getCondition,
      join.getRowType,
      joinInfo,
      filterNulls,
      joinInfo.pairs.toList,
      FlinkJoinRelType.toFlinkJoinRelType(join.getJoinType),
      null,
      description)
  }
}

object StreamExecJoinRule {
  val INSTANCE: RelOptRule = new StreamExecJoinRule
}
