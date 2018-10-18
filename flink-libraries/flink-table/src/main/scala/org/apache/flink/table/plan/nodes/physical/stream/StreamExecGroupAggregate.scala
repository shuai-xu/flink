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
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.{ImmutableBitSet, Pair}
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.common.CommonAggregate
import org.apache.flink.table.plan.nodes.common.CommonUtils._
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.AggregateUtil.transformToStreamAggregateInfoList
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateUtil, StreamExecUtil}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.aggregate.{GroupAggFunction, MiniBatchGroupAggFunction}
import org.apache.flink.table.runtime.operator.KeyedProcessOperator
import org.apache.flink.table.runtime.operator.bundle.KeyedBundleOperator
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.typeutils._
import org.apache.flink.table.util.Logging

/**
  *
  * Flink RelNode for data stream unbounded group aggregate
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation
  * @param inputRelDataType The type consumed by this RelNode
  * @param aggCalls         List of calls to aggregate functions
  * @param outputDataType   The type emitted by this RelNode
  * @param groupSet        The position (in the input Row) of the grouping keys
  */
class StreamExecGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val aggCalls: Seq[AggregateCall],
    val inputRelDataType: RelDataType,
    outputDataType: RelDataType,
    groupSet: ImmutableBitSet,
    /* flag indicating whether to skip StreamExecSplitAggregateRule */
    var skipSplit: Boolean = false)
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonAggregate
  with StreamExecRel
  with Logging {

  override def deriveRowType(): RelDataType = outputDataType

  def setSkipSplit(skipSplit: Boolean): Unit = this.skipSplit = skipSplit

  def getGroupSet: ImmutableBitSet = groupSet

  def getGroupings: Array[Int] = groupings

  val groupings: Array[Int] = groupSet.toArray

  val aggInfoList: AggregateInfoList = {
    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val modifiedMono = FlinkRelMetadataQuery.reuseOrCreate(cluster.getMetadataQuery)
      .getRelModifiedMonotonicity(this)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      groupings.length, needRetraction, modifiedMono, aggCalls)
    transformToStreamAggregateInfoList(
      aggCalls,
      input.getRowType,
      needRetractionArray,
      needInputCount = needRetraction,
      isStateBackendDataViews = true)
  }

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      aggCalls,
      inputRelDataType,
      outputDataType,
      groupSet,
      skipSplit)
  }

  override def toString: String = {
    s"GroupAggregate(${
      if (groupings.nonEmpty) {
        s"groupBy: (${groupingToString(inputRelDataType, groupings)}), "
      } else {
        ""
      }
    }select:(${
      aggregationToString(
        inputRelDataType,
        groupings,
        getRowType,
        aggCalls,
        aggInfoList.getActualFunctions,
        isMerge = false,
        isGlobal = true)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputRelDataType, groupings), groupings.nonEmpty)
      .item("select", aggregationToString(
          inputRelDataType,
          groupings,
          getRowType,
          aggCalls,
          aggInfoList.getActualFunctions,
          isMerge = false,
          isGlobal = true))
  }

  @VisibleForTesting
  def explainAgg: JList[Pair[String, AnyRef]] = {
    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val modifiedMono = cluster.getMetadataQuery.asInstanceOf[FlinkRelMetadataQuery]
      .getRelModifiedMonotonicity(this)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      groupings.length, needRetraction, modifiedMono, aggCalls)
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("groupBy", groupingToString(inputRelDataType, groupings)))
    values.add(Pair.of("select", aggregationToString(
      inputRelDataType,
      groupings,
      getRowType,
      aggCalls,
      Nil)))
    values.add(Pair.of("aggWithRetract", util.Arrays.toString(needRetractionArray)))
    values
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    if (groupings.length > 0 && queryConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn("No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val inputTransformation = getInput.asInstanceOf[StreamExecRel].translateToPlan(
      tableEnv, queryConfig)

    val outRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(outputDataType, classOf[BaseRow])
    val inputRowType = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]

    val aggString = aggregationToString(
      inputRelDataType,
      groupings,
      getRowType,
      aggCalls,
      Nil)

    val opName = if (groupings.nonEmpty) {
      s"groupBy: (${groupingToString(inputRelDataType, groupings)}), " +
        s"select: ($aggString)"
    } else {
      s"select: ($aggString)"
    }

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig, supportReference = true),
      tableEnv.getRelBuilder,
      needRetraction,
      needMerge = false,
      tableEnv.getConfig.getNullCheck)
      .bindInput(inputRowType.getFieldTypes.map(DataTypes.internal))

    val aggsHandler = generator.generateAggsHandler("GroupAggsHandler", aggInfoList)
    val accTypes = aggInfoList.getAccTypes.map(DataTypes.internal)
    val aggValueTypes = aggInfoList.getActualValueTypes.map(DataTypes.internal)
    val inputCountIndex = aggInfoList.getCount1AccIndex

    val operator = if (queryConfig.isMiniBatchEnabled || queryConfig.isMicroBatchEnabled) {
      val aggFunction = new MiniBatchGroupAggFunction(
        aggsHandler,
        accTypes,
        aggValueTypes,
        inputCountIndex,
        generateRetraction,
        groupings.isEmpty)

      // input element are all binary row as they are came from network
      val inputType = new BaseRowTypeInfo(classOf[BaseRow], inputRowType.getFieldTypes: _*)
        .asInstanceOf[BaseRowTypeInfo[BaseRow]]
      // minibatch group agg stores list of input as bundle buffer value
      val valueType = new ListTypeInfo[BaseRow](inputType)

      new KeyedBundleOperator(
        aggFunction,
        getMiniBatchTrigger(queryConfig, useLocalAgg = false),
        valueType)
    } else {
      val aggFunction = new GroupAggFunction(
        aggsHandler,
        accTypes,
        aggValueTypes,
        inputCountIndex,
        generateRetraction,
        groupings.isEmpty,
        queryConfig)

      val operator = new KeyedProcessOperator[BaseRow, BaseRow, BaseRow](aggFunction)
      operator.setRequireState(true)
      operator
    }

    val selector = StreamExecUtil.getKeySelector(groupings, inputRowType)

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      s"GroupAggregate($opName)",
      operator,
      outRowType,
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

