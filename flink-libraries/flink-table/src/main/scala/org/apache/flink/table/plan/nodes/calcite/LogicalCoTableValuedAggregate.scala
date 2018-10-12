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

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.functions.utils.CoTableValuedAggSqlFunction

/**
  * Logical node of co-table valued aggregate function call.
  */
class LogicalCoTableValuedAggregate(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    lRexCall: RexCall,
    rRexCall: RexCall,
    groupKey1: Seq[RexNode],
    groupKey2: Seq[RexNode])
  extends CoTableValuedAggregate(
    cluster,
    traits,
    leftNode,
    rightNode,
    lRexCall,
    rRexCall,
    lRexCall.getOperator.asInstanceOf[CoTableValuedAggSqlFunction].externalResultType,
    groupKey1,
    groupKey2) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalCoTableValuedAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      lRexCall,
      rRexCall,
      groupKey1,
      groupKey2)
  }
}

object LogicalCoTableValuedAggregate {
  def create(
    leftNode: RelNode,
    rightNode: RelNode,
    lRexCall: RexCall,
    rRexCall: RexCall,
    groupKey1: Seq[RexNode],
    groupKey2: Seq[RexNode]): LogicalCoTableValuedAggregate = {

    val traitSet = leftNode.getCluster.traitSetOf(Convention.NONE)
    new LogicalCoTableValuedAggregate(
      leftNode.getCluster,
      traitSet,
      leftNode,
      rightNode,
      lRexCall,
      rRexCall,
      groupKey1,
      groupKey2)
  }
}


