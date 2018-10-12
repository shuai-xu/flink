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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexInputRef
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCoTableValuedAggregate
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCoGroupTableValuedAggregate

import scala.collection.JavaConversions._

/**
  * Rule of converting batch physical node of sort implement of co-table-valued aggregate function.
  */
class BatchExecCoGroupTableValuedAggregateRule
  extends RelOptRule(
    operand(
      classOf[FlinkLogicalCoTableValuedAggregate],
      operand(classOf[RelNode], any)), "BatchExecCoGroupTableValuedAggregateRule")
    with BatchExecSortAggRuleBase{

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[FlinkLogicalCoTableValuedAggregate]
    val offset = agg.getLeft.getRowType.getFieldCount
    val groupSet1 = agg.groupKey1.map(_.asInstanceOf[RexInputRef].getIndex).toArray
    val groupSet2 = agg.groupKey2.map(_.asInstanceOf[RexInputRef].getIndex - offset).toArray

    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCHEXEC)

    val requiredTraitSet1 = getRequireTraitSet(groupSet1, agg.leftNode)
    val requiredTraitSet2 = getRequireTraitSet(groupSet2, agg.rightNode)
    val newLeft = RelOptRule.convert(agg.leftNode, requiredTraitSet1)
    val newRight = RelOptRule.convert(agg.rightNode, requiredTraitSet2)

    val newAgg = new BatchExecCoGroupTableValuedAggregate(
      agg.getCluster,
      call.builder(),
      aggProvidedTraitSet,
      newLeft,
      newRight,
      agg.lRexCall,
      agg.rRexCall,
      agg.groupKey1,
      agg.groupKey2
    )
    call.transformTo(newAgg)
  }

  protected def getRequireTraitSet(groupSet: Array[Int], input: RelNode): RelTraitSet = {
    // for co table aggregate, requireStrict is true
    val requiredDistribution = if (groupSet.nonEmpty) {
      val distributionFields = groupSet.map(Integer.valueOf).toList
      FlinkRelDistribution.hash(distributionFields, true)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    input.getTraitSet.replace(FlinkConventions.BATCHEXEC).replace(requiredDistribution)
  }
}

object BatchExecCoGroupTableValuedAggregateRule {
  val INSTANCE = new BatchExecCoGroupTableValuedAggregateRule
}

