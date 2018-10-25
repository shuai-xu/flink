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

package org.apache.flink.table.plan.nodes.physical.stream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedTableValuedAggHandleFunction}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataview.DataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.functions.utils.TableValuedAggSqlFunction
import org.apache.flink.table.plan.nodes.common.CommonTableValuedAggregate
import org.apache.flink.table.plan.nodes.common.CommonUtils._
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util._
import org.apache.flink.table.runtime.aggregate.{NonDeterministicMiniBatchGroupTableValuedAggFunction, MiniBatchGroupTableValuedAggFunction, GroupTableValuedAggFunction, NonDeterministicGroupTableValuedAggFunction}
import org.apache.flink.table.runtime.operator.KeyedProcessOperator
import org.apache.flink.table.runtime.operator.bundle.KeyedBundleOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging

/**
  * Stream physical node of table-valued agg function call
  */
class StreamExecGroupTableValuedAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    call: RexCall,
    groupKey: Seq[RexNode],
    groupKeyName: Seq[String])
  extends CommonTableValuedAggregate(cluster, traitSet, input, call, groupKey, groupKeyName)
    with StreamExecRel
    with Logging {

  val inputTypes =
    call.getOperands.toArray
    .map(n => FlinkTypeFactory.toInternalType(n.asInstanceOf[RexNode].getType))
  val inputIndex = inputTypes.zipWithIndex.map(t => t._2)
  val groupings: Array[Int] = groupKey.zipWithIndex.map(t => inputIndex.length + t._2).toArray
  val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
  val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)

  val sqlFunction: TableValuedAggSqlFunction =
    call.getOperator.asInstanceOf[TableValuedAggSqlFunction]
  val accType = sqlFunction.externalAccType
  val resultType = sqlFunction.externalResultType

  val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
    0,
    sqlFunction.getFunction,
    accType,
    true)

  val aggInfoList = AggregateInfoList(Array[AggregateInfo](
    AggregateInfo(
      null,
      sqlFunction.getFunction,
      0,
      inputIndex,
      Array(newExternalAccType),
      specs,
      resultType)))

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecGroupTableValuedAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      call,
      groupKey,
      groupKeyName)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(input.getRowType, groupings), groupings.nonEmpty)
      .item("aggApply", buildAggregationToString(
        input.getRowType,
        sqlFunction,
        inputIndex))
  }

  override def toString: String = {
    s"GroupTableValuedAggregate(${
      if (groupings.nonEmpty) {
        s"groupBy: (${groupingToString(input.getRowType, groupings)}), "
      } else {
        ""
      }
    }aggApply:(${
      buildAggregationToString(
        input.getRowType,
        sqlFunction,
        inputIndex)
    }))"
  }

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv    The [[StreamTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    * @return StreamTransformation of type [[BaseRow]]
    */
  override def translateToPlan(
    tableEnv: StreamTableEnvironment,
    queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    if (groupings.length > 0 && queryConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn("No state retention interval configured for a query which accumulates state. " +
                 "Please provide a query configuration with valid retention interval to prevent " +
                 "excessive " +
                 "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputTransformation =
      getInput.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)

    val inputRowType = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
    val outputRow = FlinkTypeFactory.toInternalBaseRowTypeInfo(deriveRowType, classOf[BaseRow])

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig, supportReference = true),
      tableEnv.getRelBuilder,
      inputTypes,
      needRetraction,
      needMerge = false,
      tableEnv.getConfig.getNullCheck)

    val generateAggHandler: GeneratedTableValuedAggHandleFunction =
      generator.generateTableValuedAggHandler("GroupTableValuedAggHandler", aggInfoList)

    val operator = if (queryConfig.isMiniBatchEnabled || queryConfig.isMicroBatchEnabled) {

      val tableValuedGroupAggFunction = if (sqlFunction.getFunction.isDeterministic) {
        new MiniBatchGroupTableValuedAggFunction(generateAggHandler,
          accType,
          generateRetraction,
          groupings.isEmpty)
      } else {
        new NonDeterministicMiniBatchGroupTableValuedAggFunction(
          generateAggHandler,
          accType,
          DataTypes.internal(outputRow).asInstanceOf[BaseRowType],
          generateRetraction,
          groupings.isEmpty
        )
      }

      // input element are all binary row as they are came from network
      val inputType = new BaseRowTypeInfo(classOf[BaseRow], inputRowType.getFieldTypes: _*)
        .asInstanceOf[BaseRowTypeInfo[BaseRow]]
      // minibatch group agg stores list of input as bundle buffer value
      val valueType = new ListTypeInfo[BaseRow](inputType)

      new KeyedBundleOperator(
        tableValuedGroupAggFunction,
        getMiniBatchTrigger(queryConfig, useLocalAgg = false),
        valueType)

    } else {

      val tableValuedGroupAggFunction = if (sqlFunction.getFunction.isDeterministic) {
        new GroupTableValuedAggFunction(
          generateAggHandler,
          accType,
          generateRetraction,
          groupings.isEmpty,
          queryConfig
        )
      } else {
        new NonDeterministicGroupTableValuedAggFunction(
          generateAggHandler,
          accType,
          DataTypes.internal(outputRow).asInstanceOf[BaseRowType],
          generateRetraction,
          groupings.isEmpty,
          queryConfig
        )
      }

      val operator =
        new KeyedProcessOperator[BaseRow, BaseRow, BaseRow](tableValuedGroupAggFunction)
      operator.setRequireState(true)

      operator
    }

    val selector = StreamExecUtil.getKeySelector(groupings, inputRowType)

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      toString,
      operator,
      outputRow,
      tableEnv.execEnv.getParallelism)

    if (groupings.isEmpty) {
      ret.forceNonParallel()
    }

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

}
