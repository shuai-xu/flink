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
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.RexNode

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

/**
  * Support all joins.
  */
private class FlinkLogicalJoinConverter
  extends ConverterRule(
    classOf[LogicalJoin],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalJoinConverter") {

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[LogicalJoin]
    val newLeft = RelOptRule.convert(join.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(join.getRight, FlinkConventions.LOGICAL)
    FlinkLogicalJoin.create(newLeft, newRight, join.getCondition, join.getJoinType)
  }
}

object FlinkLogicalJoin {
  val CONVERTER: ConverterRule = new FlinkLogicalJoinConverter

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
