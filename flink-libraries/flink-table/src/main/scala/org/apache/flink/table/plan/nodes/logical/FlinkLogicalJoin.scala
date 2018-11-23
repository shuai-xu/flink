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

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConverters._

class FlinkLogicalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends FlinkLogicalJoinBase(
    cluster,
    traitSet,
    left,
    right,
    condition,
    joinType)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {

    new FlinkLogicalJoin(cluster, traitSet, left, right, conditionExpr, joinType)
  }
}

private class FlinkLogicalJoinConverter
  extends ConverterRule(
    classOf[LogicalJoin],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalJoinConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalJoin = call.rel(0).asInstanceOf[LogicalJoin]
    val joinInfo = join.analyzeCondition

    hasEqualityPredicates(joinInfo) || isSingleRowJoin(join)
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[LogicalJoin]
    val newLeft = RelOptRule.convert(join.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(join.getRight, FlinkConventions.LOGICAL)
    FlinkLogicalJoin.create(newLeft, newRight, join.getCondition, join.getJoinType)
  }

  def hasEqualityPredicates(joinInfo: JoinInfo): Boolean = {
    // joins require an equi-condition or a conjunctive predicate with at least one equi-condition
    !joinInfo.pairs().isEmpty
  }

  def isSingleRowJoin(join: LogicalJoin): Boolean = {
    join.getJoinType match {
      case JoinRelType.INNER if isSingleRow(join.getRight) || isSingleRow(join.getLeft) => true
      case JoinRelType.LEFT if isSingleRow(join.getRight) => true
      case JoinRelType.RIGHT if isSingleRow(join.getLeft) => true
      case _ => false
    }
  }

  /**
    * Recursively checks if a [[RelNode]] returns at most a single row.
    * Input must be a global aggregation possibly followed by projections or filters.
    */
  private def isSingleRow(node: RelNode): Boolean = {
    node match {
      case ss: RelSubset => isSingleRow(ss.getOriginal)
      case lp: Project => isSingleRow(lp.getInput)
      case lf: Filter => isSingleRow(lf.getInput)
      case lc: Calc => isSingleRow(lc.getInput)
      case la: Aggregate => la.getGroupSet.isEmpty
      case _ => false
    }
  }
}

/**
  * Support all joins.
  */
private class FlinkLogicalJoinBatchExecConverter extends FlinkLogicalJoinConverter {
  override def matches(call: RelOptRuleCall): Boolean = true
}

object FlinkLogicalJoin {
  val CONVERTER: ConverterRule = new FlinkLogicalJoinBatchExecConverter()

  def create(
      left: RelNode,
      right: RelNode,
      conditionExpr: RexNode,
      joinType: JoinRelType): FlinkLogicalJoin = {
    val cluster = left.getCluster
    val traitSet = cluster.traitSetOf(Convention.NONE)
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalJoin to
    // calculate the distribution trait
    val join = new FlinkLogicalJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(join)
      .replace(FlinkConventions.LOGICAL).simplify()
    join.copy(newTraitSet, join.getInputs).asInstanceOf[FlinkLogicalJoin]
  }
}
