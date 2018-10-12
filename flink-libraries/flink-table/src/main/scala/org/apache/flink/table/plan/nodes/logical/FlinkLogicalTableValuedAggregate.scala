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

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.LogicalTableValuedAggregate
import org.apache.flink.table.plan.nodes.common.CommonTableValuedAggregate
import org.apache.flink.table.util.Logging

/**
  * Flink logical node of table valued aggregate function call
  */
class FlinkLogicalTableValuedAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val call: RexCall,
    val groupKey: Seq[RexNode],
    val groupKeyName: Seq[String])
  extends CommonTableValuedAggregate(cluster, traitSet, input, call, groupKey, groupKeyName)
    with FlinkLogicalRel
    with Logging {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalTableValuedAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      call,
      groupKey,
      groupKeyName)
  }
}

private class FlinkLogicalTableValuedAggregateConverter
  extends ConverterRule(
    classOf[LogicalTableValuedAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableValuedAggregateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val call = rel.asInstanceOf[LogicalTableValuedAggregate]
    val traitSet = FlinkRelMetadataQuery.traitSet(rel).replace(FlinkConventions.LOGICAL).simplify()
    val newInput = RelOptRule.convert(call.getInput, FlinkConventions.LOGICAL)
    new FlinkLogicalTableValuedAggregate(
      rel.getCluster, traitSet, newInput, call.call, call.groupKey, call.groupKeyName)
  }
}

object FlinkLogicalTableValuedAggregate {
  val CONVERTER: ConverterRule = new FlinkLogicalTableValuedAggregateConverter()
}

