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
import org.apache.flink.table.plan.nodes.common.CommonTableValuedAggregate

/**
  * Logical node of table valued aggregate function call.
  */
class LogicalTableValuedAggregate(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputNode: RelNode,
    val call: RexCall,
    val groupKey: Seq[RexNode],
    val groupKeyName: Seq[String])
  extends CommonTableValuedAggregate(cluster, traits, inputNode, call, groupKey, groupKeyName) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalTableValuedAggregate(cluster, traitSet, inputs.get(0), call, groupKey, groupKeyName)
  }
}

object LogicalTableValuedAggregate {
  def create(
    inputNode: RelNode,
    call: RexCall,
    groupKey: Seq[RexNode],
    groupKeyName: Seq[String]) : LogicalTableValuedAggregate = {
    val traitSet = inputNode.getCluster.traitSetOf(Convention.NONE)
    new LogicalTableValuedAggregate(
      inputNode.getCluster, traitSet, inputNode, call, groupKey, groupKeyName)
  }
}
