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

import java.util

import org.apache.calcite.plan.{RelOptRuleCall, RelOptRule}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelCollation, RelFieldCollation, RelCollations, RelNode}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.utils.TableValuedAggSqlFunction
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableValuedAggregate
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecSortAggregate}
import org.apache.flink.table.runtime.aggregate.RelFieldCollations
import org.apache.flink.table.types.DataTypes

import scala.collection.JavaConversions._

/**
 * Rule of converting batch physical node of
 * sort implement of table-valued aggregate function.
 */
class BatchExecSortTableValuedAggRule
  extends RelOptRule(
    operand(
      classOf[FlinkLogicalTableValuedAggregate],
      operand(classOf[RelNode], any)), "BatchExecSortTableValuedAggRule")
  with BatchExecSortAggRuleBase{

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[FlinkLogicalTableValuedAggregate]
    val input = call.rels(1)

    val offset = agg.call.getOperands.size()
    val groupSet: Array[Int] = agg.groupKey.zipWithIndex.map(t => offset + t._2).toArray

    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCHEXEC)
    val cluster = agg.getCluster
    val aggNames = Seq("udtvagg")
    val auxGroupSet: Array[Int] = Array()

    val sqlFunction: TableValuedAggSqlFunction =
      agg.call.getOperator.asInstanceOf[TableValuedAggSqlFunction]
    val accTypeExternal = sqlFunction.externalAccType

    val aggBufferTypes = Array(Array(DataTypes.internal(accTypeExternal)))

    val aggregates :Array[UserDefinedFunction]= Array(sqlFunction.getFunction)

    val groupList = new util.ArrayList[Integer]()
    agg.groupKey.zipWithIndex.map(t => offset + t._2).foreach(groupList.add(_))
    val inputTypes =
      agg.call.getOperands.toArray
        .map(n => FlinkTypeFactory.toInternalType(n.asInstanceOf[RexNode].getType))
    val inputIndex = inputTypes.zipWithIndex.map(t => t._2)
    val argList: util.ArrayList[Integer] = new util.ArrayList()
    inputIndex.map(argList.add(_))
    val aggCall = AggregateCall.create(
      sqlFunction, false, false, argList, -1,
      call.builder().getTypeFactory.asInstanceOf[FlinkTypeFactory]
        .createTypeFromInternalType(DataTypes.internal(sqlFunction.externalResultType), true),
      sqlFunction.getFunction.getClass.getName)
    val aggCallToAggFunction = Seq(aggCall).zip(aggregates)

    val outputType = agg.getRowType

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

object BatchExecSortTableValuedAggRule {
  val INSTANCE = new BatchExecSortTableValuedAggRule
}
