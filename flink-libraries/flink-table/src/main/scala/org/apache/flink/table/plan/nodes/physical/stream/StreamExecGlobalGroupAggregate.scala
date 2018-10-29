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

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Pair
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedAggsHandleFunction}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonAggregate
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.{AggregateInfoList, StreamExecUtil}
import org.apache.flink.table.runtime.aggregate.MiniBatchGlobalGroupAggFunction
import org.apache.flink.table.runtime.operator.bundle.KeyedBundleOperator
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging

/**
  *
  * Flink RelNode for data stream unbounded global group aggregate
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation
  * @param aggInputSchema   The schema of input node of local aggregate node
  * @param localAggInfoList      The information list about the node's local aggregates
  *                              which use heap dataviews
  * @param globalAggInfoList      The information list about the node's global aggregates
  *                               which use state dataviews
  * @param outputDataType   The type emitted by this RelNode
  * @param groupings        The position (in the input Row) of the grouping keys
  */
class StreamExecGlobalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    localAggInfoList: AggregateInfoList,
    val globalAggInfoList: AggregateInfoList,
    aggInputSchema: BaseRowSchema,
    outputDataType: RelDataType,
    groupings: Array[Int])
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonAggregate
  with StreamExecRel
  with Logging {

  override def deriveRowType(): RelDataType = outputDataType

  def getGroupings: Array[Int] = groupings

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGlobalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      localAggInfoList,
      globalAggInfoList,
      aggInputSchema,
      outputDataType,
      groupings)
  }

  override def toString: String = {
    s"GlobalGroupAggregate(${
      if (!groupings.isEmpty) {
        s"groupBy: (${groupingToString(inputNode.getRowType, groupings)}), "
      } else {
        ""
      }
    }select:(${
      aggregationToString(
        inputNode.getRowType,
        groupings,
        Array.empty[Int],
        getRowType,
        globalAggInfoList.getActualAggregateCalls,
        globalAggInfoList.getActualFunctions,
        isMerge = true,
        isGlobal = true,
        globalAggInfoList.distinctInfos)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputNode.getRowType, groupings), groupings.nonEmpty)
      .item("select", aggregationToString(
        inputNode.getRowType,
        groupings,
        Array.empty[Int],
        getRowType,
        globalAggInfoList.getActualAggregateCalls,
        globalAggInfoList.getActualFunctions,
        isMerge = true,
        isGlobal = true,
        globalAggInfoList.distinctInfos))
  }

  @VisibleForTesting
  def explainAgg: JList[Pair[String, AnyRef]] = {
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("groupBy", groupingToString(inputNode.getRowType, groupings)))
    values.add(Pair.of("select", aggregationToString(
      inputNode.getRowType,
      groupings,
      Array.empty[Int],
      getRowType,
      globalAggInfoList.getActualAggregateCalls,
      globalAggInfoList.getActualFunctions,
      isMerge = true,
      isGlobal = true,
      globalAggInfoList.distinctInfos)))
    values.add(Pair.of("aggs", globalAggInfoList
      .aggInfos
      .map(e => e.function.toString)
      .mkString(", ")))
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

    val aggString = aggregationToString(
      inputNode.getRowType,
      groupings,
      Array.empty[Int],
      getRowType,
      globalAggInfoList.getActualAggregateCalls,
      globalAggInfoList.getActualFunctions,
      isMerge = true,
      isGlobal = true,
      globalAggInfoList.distinctInfos)

    val opName = if (groupings.nonEmpty) {
      s"groupBy: (${groupingToString(inputNode.getRowType, groupings)}), " +
        s"select: ($aggString)"
    } else {
      s"select: ($aggString)"
    }

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)

    val localAggsHandler = generateAggsHandler(
      "LocalGroupAggsHandler",
      localAggInfoList,
      mergedAccOffset = groupings.length,
      mergedAccOnHeap = true,
      localAggInfoList.getAccTypes,
      tableEnv.getConfig,
      tableEnv.getRelBuilder,
      // the local aggregate result will be buffered, so need copy
      inputFieldCopy = true)

    val globalAggsHandler = generateAggsHandler(
      "GlobalGroupAggsHandler",
      globalAggInfoList,
      mergedAccOffset = 0,
      mergedAccOnHeap = true,
      localAggInfoList.getAccTypes,
      tableEnv.getConfig,
      tableEnv.getRelBuilder,
      // if global aggregate result will be put into state, then not need copy
      // but this global aggregate result will be put into a buffered map first,
      // then multiput to state, so it need copy
      inputFieldCopy = true)

    val globalAccTypes = globalAggInfoList.getAccTypes.map(DataTypes.internal)
    val globalAggValueTypes = globalAggInfoList.getActualValueTypes.map(DataTypes.internal)
    val inputCountIndex = globalAggInfoList.getCount1AccIndex

    val operator = if (queryConfig.isMiniBatchEnabled || queryConfig.isMicroBatchEnabled) {
      val aggFunction = new MiniBatchGlobalGroupAggFunction(
        localAggsHandler,
        globalAggsHandler,
        globalAccTypes,
        globalAggValueTypes,
        inputCountIndex,
        generateRetraction,
        groupings.isEmpty)

      val localAccTypes = localAggInfoList.getAccTypes.map(DataTypes.toTypeInfo)
      // the bundle buffer value type is local acc type which contains mapview type
      val valueTypeInfo = new BaseRowTypeInfo(classOf[BaseRow], localAccTypes: _*)
      new KeyedBundleOperator(
        aggFunction,
        getMiniBatchTrigger(queryConfig, useLocalAgg = true),
        valueTypeInfo)
    } else {
      throw new TableException("Local-Global optimization is only worked in minibatch mode")
    }

    val inputTypeInfo = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[BaseRow]]
    val selector = StreamExecUtil.getKeySelector(groupings, inputTypeInfo)

    // partitioned aggregation
    val ret = new OneInputTransformation(
      inputTransformation,
      s"GlobalGroupAggregate($opName)",
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

  def generateAggsHandler(
    name: String,
    aggInfoList: AggregateInfoList,
    mergedAccOffset: Int,
    mergedAccOnHeap: Boolean,
    mergedAccExternalTypes: Array[DataType],
    config: TableConfig,
    relBuilder: RelBuilder,
    inputFieldCopy: Boolean): GeneratedAggsHandleFunction = {

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(config, supportReference = true),
      relBuilder,
      aggInputSchema.fieldTypes,
      needRetract = false,
      needMerge = true,
      config.getNullCheck,
      inputFieldCopy)

    generator
      .withMerging(mergedAccOffset, mergedAccOnHeap, mergedAccExternalTypes)
      .generateAggsHandler(name, aggInfoList)
  }
}

