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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedOperator}
import org.apache.flink.table.functions.{UserDefinedAggregateFunction, TableValuedAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.cost.BatchExecCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.dataformat.{BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.runtime.operator.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.types.{BaseRowType, DataTypes}

abstract class BatchExecSortAggregateBase(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchExecGroupAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    isMerge,
    isFinal) {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    if (isTableValuedAgg(aggregates) && isFinal) {
      val inputRows = mq.getRowCount(getInput())
      if (inputRows == null) {
        return null
      }
      val cpuCost = FUNC_CPU_COST * inputRows
      val outRows =  mq.getRowCount(this)
      val averageRowSize: Double = mq.getAverageRowSize(this)
      val memCost = averageRowSize
      val rowCountCost: Double =
        if (outRows != null && isTableValuedAgg(aggregates) && isFinal) {
          outRows * 2
        } else {
          outRows
        }
      val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
      costFactory.makeCost(rowCountCost, cpuCost, 0, 0, memCost)
    } else {
      val inputRows = mq.getRowCount(getInput())
      if (inputRows == null) {
        return null
      }
      // sort is not done here
      var cpuCost = FUNC_CPU_COST * inputRows * aggCallToAggFunction.size
      // Punish One-phase aggregate if it's input data is skew on groupKeys.
      if (isFinal && !isMerge && isSkewOnGroupKeys(mq)) {
        cpuCost = cpuCost * getSkewPunishFactor
      }
      val averageRowSize: Double = mq.getAverageRowSize(this)
      val memCost = averageRowSize
      val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
      costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
    }
  }

  override def getOutputRowType: BaseRowType = {
    if (grouping.isEmpty) {
      FlinkTypeFactory.toInternalBaseRowType(getRowType, classOf[GenericRow])
    } else {
      FlinkTypeFactory.toInternalBaseRowType(getRowType, classOf[JoinedRow])
    }
  }

  private[flink] def codegenWithKeys(
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: BaseRowType,
      outputType: BaseRowType): GeneratedOperator = {
    val config = tableEnv.config
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    // register udaggs
    aggCallToAggFunction.map(_._2).filter(a => a.isInstanceOf[UserDefinedAggregateFunction[_]])
        .map(a => ctx.addReusableFunction(a))

    val currentKeyTerm = "currentKey"
    val currentKeyWriterTerm = "currentKeyWriter"

    val keyProjectionCode = genGroupKeyProjectionCode("SortAgg", ctx,
      groupKeyRowType, getGrouping, inputType, inputTerm, currentKeyTerm, currentKeyWriterTerm)

    val keyNotEquals = genGroupKeyChangedCheckCode(currentKeyTerm, lastKeyTerm)

    val (initAggBufferCode, doAggregateCode, aggOutputExpr) = genSortAggCodes(
      isMerge, isFinal, ctx, config, builder, getGrouping, getAuxGrouping, inputRelDataType,
      aggCallToAggFunction, aggregates, udaggs, inputTerm, inputType,
      aggBufferNames, aggBufferTypes, outputType)

    ctx.addOutputRecord(DataTypes.internal(outputType), joinedRow)
    val binaryRow = classOf[BinaryRow].getName
    ctx.addReusableMember(s"$binaryRow $lastKeyTerm = null;")

    val processCode =
      s"""
         |hasInput = true;
         |${ctx.reuseInputUnboxingCode(Set(inputTerm))}
         |
         |// project key from input
         |$keyProjectionCode
         |if ($lastKeyTerm == null) {
         |  $lastKeyTerm = $currentKeyTerm.copy();
         |
         |  // init agg buffer
         |  $initAggBufferCode
         |} else if ($keyNotEquals) {
         |
         |  ${getAggOutputCode(aggOutputExpr, isFinal, withKey = true)}
         |
         |  $lastKeyTerm = $currentKeyTerm.copy();
         |
         |  // init agg buffer
         |  $initAggBufferCode
         |}
         |
         |// do doAggregateCode
         |$doAggregateCode
         |""".stripMargin.trim

    val endInputCode =
      s"""
         |if (hasInput) {
         |  ${getAggOutputCode(aggOutputExpr, isFinal, withKey = true)}
         |}
       """.stripMargin

    val className = if (isFinal) "SortAggregateWithKeys" else "LocalSortAggregateWithKeys"
    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputRelDataType, config)
  }
}
