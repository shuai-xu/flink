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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel._
import org.apache.flink.table.api.{OperatorType, TableConfig}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.plan.util.AggregateUtil
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.util.FlinkRelOptUtil

import scala.collection.JavaConversions._

class BatchExecSortAggRule
    extends RelOptRule(
      operand(
        classOf[FlinkLogicalAggregate],
        operand(classOf[RelNode], any)), "BatchExecSortAggRule") with BatchExecSortAggRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[TableConfig])
    tableConfig.enabledGivenOpType(OperatorType.SortAgg)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[FlinkLogicalAggregate]
    val input = call.rels(1)

    if (agg.indicator) {
      throw new UnsupportedOperationException("Not support group sets aggregate now.")
    }

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = FlinkRelOptUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, input.getRowType)
    val groupSet = agg.getGroupSet.toArray
    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggregates)
    val aggNames: Seq[String] = agg.getNamedAggCalls.map(_.right)
    val cluster = agg.getCluster
    // TODO aggregate include projection now, so do not provide new trait will be safe
    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCHEXEC)

    generateSortAggPhysicalNode(
      call,
      cluster,
      aggNames,
      groupSet,
      auxGroupSet,
      aggregates,
      aggBufferTypes.map(_.map(DataTypes.internal)),
      aggCallToAggFunction,
      aggProvidedTraitSet,
      agg.getRowType)
  }
}

object BatchExecSortAggRule {
  val INSTANCE = new BatchExecSortAggRule
}
