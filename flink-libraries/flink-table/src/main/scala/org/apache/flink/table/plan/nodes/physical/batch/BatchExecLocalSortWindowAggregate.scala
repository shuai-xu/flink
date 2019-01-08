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

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{BaseRowType, DataTypes}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.runtime.OneInputSubstituteStreamOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{BatchExecRelVisitor, ExecResourceUtil}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder

import java.util

class BatchExecLocalSortWindowAggregate(
    window: LogicalWindow,
    val inputTimestampIndex: Int,
    inputTimestampType: RelDataType,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    enableAssignPane: Boolean = false)
  extends BatchExecSortWindowAggregateBase(
    window,
    inputTimestampIndex,
    inputTimestampType,
    namedProperties,
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    enableAssignPane,
    isMerge = false,
    isFinal = false) {

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLocalSortWindowAggregate(
      window,
      inputTimestampIndex,
      inputTimestampType,
      namedProperties,
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      rowRelDataType,
      inputRelDataType,
      grouping,
      auxGrouping,
      enableAssignPane)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv)
    val outputRowType = getOutputType

    val ctx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val (windowSize: Long, slideSize: Long) = getWindowDef(window)
    val windowStart = 0L
    val groupBufferLimitSize = ExecResourceUtil.getWindowAggBufferLimitSize(
      tableEnv.getConfig.getConf)

    val inputType = DataTypes.internal(input.getOutputType).asInstanceOf[BaseRowType]
    val generatedOperator = if (grouping.isEmpty) {
      codegenWithoutKeys(ctx, tableEnv,
        inputType, outputRowType,
        groupBufferLimitSize, windowStart, windowSize, slideSize)
    } else {
      codegenWithKeys(ctx, tableEnv,
        inputType, outputRowType,
        groupBufferLimitSize, windowStart, windowSize, slideSize)
    }

    val operator = new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    tableEnv.getRUKeeper.addTransformation(this, transformation)

    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = "LocalSortWindowAggregateBatchExec"

}
