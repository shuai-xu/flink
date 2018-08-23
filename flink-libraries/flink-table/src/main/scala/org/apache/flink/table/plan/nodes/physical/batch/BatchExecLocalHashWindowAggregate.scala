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

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.operator.SubstituteStreamOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BatchExecResourceUtil

class BatchExecLocalHashWindowAggregate(
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
  extends BatchExecHashWindowAggregateBase(
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
    super.supplement(new BatchExecLocalHashWindowAggregate(
      window,
      inputTimestampIndex,
      inputTimestampType,
      namedProperties,
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputRelDataType,
      grouping,
      auxGrouping,
      enableAssignPane))
  }

  /**
    * Note: Override this method is very important because Calcite will compute relNode's digest
    * via this method and append a head name base on class name, w/o this overriding, digest header
    * will be the same parent class name, then may encounter 'weird' problems during optimization...
    */
  override def explainTerms(pw: RelWriter): RelWriter = super.explainTerms(pw)

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[BaseRow] = {

    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)
    val outputRowType = getOutputType

    val groupBufferLimitSize = BatchExecResourceUtil.getWindowAggBufferLimitSize(tableEnv.getConfig)

    val (windowSize: Long, slideSize: Long) = getWindowDef(window)
    val windowStart = 0L
    val ctx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val generatedOperator = codegen(ctx, tableEnv,
      DataTypes.internal(input.getOutputType).asInstanceOf[BaseRowType], outputRowType,
      groupBufferLimitSize, BatchExecResourceUtil.getManagedMemory(reservedResSpec) * BatchExecResourceUtil.SIZE_IN_MB,
      BatchExecResourceUtil.getManagedMemory(preferResSpec) * BatchExecResourceUtil.SIZE_IN_MB,
      windowStart, windowSize, slideSize)

    val operator = new SubstituteStreamOperator[BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    LOG.info(
      this + " the reserved: " + reservedResSpec + ", and the preferred: " + preferResSpec + ".")
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(reservedResSpec, preferResSpec)
    transformation
  }

  private def getOperatorName = "LocalWindowHashAggregateBatchExec"

  override def toString: String = getOperatorName
}
