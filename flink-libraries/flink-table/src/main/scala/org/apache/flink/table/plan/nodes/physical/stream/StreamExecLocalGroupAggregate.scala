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
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.Pair
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.plan.nodes.common.CommonAggregate
import org.apache.flink.table.plan.nodes.common.CommonUtils._
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.{AggregateInfoList, StreamExecUtil}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.aggregate.MiniBatchLocalGroupAggFunction
import org.apache.flink.table.runtime.operator.bundle.BundleOperator
import org.apache.flink.table.types.{DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging

/**
  *
  * Flink RelNode for data stream unbounded local group aggregate
  *
  * @param cluster          Cluster of the RelNode, represent for an environment of related
  *                         relational expressions during the optimization of a query.
  * @param traitSet         Trait set of the RelNode
  * @param inputNode        The input RelNode of aggregation
  * @param aggInfoList      The information list about the node's aggregates
  * @param inputRelDataType The type of the rows consumed by this RelNode
  * @param outputDataType   The type of the rows emitted by this RelNode
  * @param groupings        The position (in the input Row) of the grouping keys
  */
class StreamExecLocalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val aggInfoList: AggregateInfoList,
    inputRelDataType: RelDataType,
    outputDataType: RelDataType,
    groupings: Array[Int],
    val aggCalls: Seq[AggregateCall])
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonAggregate
  with StreamExecRel
  with Logging {

  override def deriveRowType(): RelDataType = outputDataType

  def getGroupings: Array[Int] = groupings

  override def producesUpdates = false

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecLocalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      aggInfoList,
      inputRelDataType,
      outputDataType,
      groupings,
      aggCalls)
  }

  override def toString: String = {
    s"LocalGroupAggregate(${
      if (!groupings.isEmpty) {
        s"groupBy: (${groupingToString(inputRelDataType, groupings)}), "
      } else {
        ""
      }
    }select:(${
      aggregationToString(
        inputRelDataType,
        groupings,
        Array.empty[Int],
        getRowType,
        aggInfoList.aggInfos.map(_.agg),
        aggInfoList.aggInfos.map(_.function),
        isMerge = false,
        isGlobal = false,
        aggInfoList.distinctInfos)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputRelDataType, groupings), !groupings.isEmpty)
      .item(
        "select", aggregationToString(
          inputRelDataType,
          groupings,
          Array.empty[Int],
          getRowType,
          aggInfoList.aggInfos.map(_.agg),
          aggInfoList.aggInfos.map(_.function),
          isMerge = false,
          isGlobal = false,
          aggInfoList.distinctInfos))
  }

  @VisibleForTesting
  def explainAgg: JList[Pair[String, AnyRef]] = {
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("groupBy", groupingToString(inputRelDataType, groupings)))
    values.add(Pair.of("select", aggregationToString(
      inputRelDataType,
      groupings,
      Array.empty[Int],
      getRowType,
      aggInfoList.aggInfos.map(_.agg),
      aggInfoList.aggInfos.map(_.function),
      isMerge = false,
      isGlobal = false,
      aggInfoList.distinctInfos)))
    values.add(Pair.of("aggs", aggInfoList
      .aggInfos
      .map(e => e.function.toString)
      .mkString(", ")))
    values
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    val inputTransformation = getInput.asInstanceOf[StreamExecRel].translateToPlan(
      tableEnv, queryConfig)
    val inputRowType = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
    val outRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(outputDataType, classOf[BaseRow])

    val aggString = aggregationToString(
      inputRelDataType,
      groupings,
      Array.empty[Int],
      getRowType,
      aggInfoList.aggInfos.map(_.agg),
      aggInfoList.aggInfos.map(_.function),
      isMerge = false,
      isGlobal = false,
      aggInfoList.distinctInfos)

    val opName = if (groupings.nonEmpty) {
      s"groupBy: (${groupingToString(inputRelDataType, groupings)}), " +
        s"select: ($aggString)"
    } else {
      s"select: ($aggString)"
    }

    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig, supportReference = true),
      tableEnv.getRelBuilder,
      needRetraction,
      needMerge = true,
      tableEnv.getConfig.getNullCheck)
      .bindInput(inputRowType.getFieldTypes.map(DataTypes.internal))

    val aggsHandler = generator.generateAggsHandler("GroupAggsHandler", aggInfoList)
    val aggFunction = new MiniBatchLocalGroupAggFunction(aggsHandler)
    val accTypes = aggInfoList.getAccTypes
    // serialize as GenericRow, deserialize as BinaryRow
    val valueTypeInfo = new BaseRowTypeInfo(
      classOf[BaseRow],
      accTypes.map(DataTypes.toTypeInfo): _*)

    val inputTypeInfo = inputTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo[BaseRow]]
    val selector = StreamExecUtil.getKeySelector(groupings, inputTypeInfo)

    val operator = new BundleOperator(
      aggFunction,
      getMiniBatchTrigger(queryConfig, useLocalAgg = true),
      selector.getProducedType,
      valueTypeInfo,
      selector)

    new OneInputTransformation(
      inputTransformation,
      s"LocalGroupAggregate($opName)",
      operator,
      outRowType,
      inputTransformation.getParallelism)
  }
}

