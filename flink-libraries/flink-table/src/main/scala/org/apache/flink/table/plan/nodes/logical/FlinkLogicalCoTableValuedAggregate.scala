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

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.functions.utils.CoTableValuedAggSqlFunction
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.{CoTableValuedAggregate, LogicalCoTableValuedAggregate}

class FlinkLogicalCoTableValuedAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    lRexCall: RexCall,
    rRexCall: RexCall,
    groupKey1: Seq[RexNode],
    groupKey2: Seq[RexNode])
  extends CoTableValuedAggregate(
    cluster,
    traitSet,
    left,
    right,
    lRexCall,
    rRexCall,
    lRexCall.getOperator.asInstanceOf[CoTableValuedAggSqlFunction].externalResultType,
    groupKey1,
    groupKey2)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalCoTableValuedAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      lRexCall,
      rRexCall,
      groupKey1,
      groupKey2
    )
  }

  override def getLeft: RelNode = left
}

private class FlinkLogicalCoGroupTableValuedAggregateConverter
  extends ConverterRule(
    classOf[LogicalCoTableValuedAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalCoGroupTableValuedAggregateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val connect = rel.asInstanceOf[LogicalCoTableValuedAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newLeft = RelOptRule.convert(connect.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(connect.getRight, FlinkConventions.LOGICAL)
    new FlinkLogicalCoTableValuedAggregate(
      connect.getCluster,
      traitSet,
      newLeft,
      newRight,
      connect.lRexCall,
      connect.rRexCall,
      connect.groupKey1,
      connect.groupKey2
    )
  }
}

object FlinkLogicalCoTableValuedAggregate {
  val CONVERTER: ConverterRule = new FlinkLogicalCoGroupTableValuedAggregateConverter
}
