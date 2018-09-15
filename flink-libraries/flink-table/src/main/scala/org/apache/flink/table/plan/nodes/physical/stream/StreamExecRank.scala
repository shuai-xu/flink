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
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Rank
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.RankUtil._
import org.apache.flink.table.plan.util.{RankRange, RankUtil, StreamExecUtil}
import org.apache.flink.table.runtime.operator.KeyedProcessOperator
import org.apache.flink.table.runtime.rank._
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConverters._

class StreamExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    inputSchema: BaseRowSchema,
    schema: BaseRowSchema,
    rankFunction: SqlRankFunction,
    partitionKey: Array[Int],
    sortCollation: RelCollation,
    rankRange: RankRange,
    val outputRankFunColumn: Boolean)
  extends Rank(
    cluster,
    traitSet,
    inputNode,
    rankFunction,
    ImmutableBitSet.of(partitionKey: _*),
    sortCollation,
    rankRange)
  with StreamExecRel {

  var strategy: RankStrategy = _

  def getStrategy(
      queryConfig: Option[StreamQueryConfig] = None,
      forceRecompute: Boolean = false): RankStrategy = {
    if (strategy == null || forceRecompute) {
      val qc: StreamQueryConfig = queryConfig.getOrElse(
        cluster.getPlanner.getContext.unwrap(classOf[StreamQueryConfig]))
      strategy = RankUtil.analyzeRankStrategy(cluster, qc, inputNode, sortCollation)
    }
    strategy
  }

  override def producesUpdates = true

  override def consumesRetractions = true

  override def needsUpdatesAsRetraction(input: RelNode): Boolean =
    getStrategy(forceRecompute = true) == RetractRank

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    val rank = new StreamExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      inputSchema,
      schema,
      rankFunction,
      partitionKey,
      sortCollation,
      rankRange,
      outputRankFunColumn)
    rank.strategy = this.strategy
    rank
  }

  private def partitionFieldsToString(partitionKey: Array[Int], rowType: RelDataType): String = {
    partitionKey.map(rowType.getFieldNames.get(_)).mkString(", ")
  }

  private def selectToString: String = {
    if (inputSchema.arity == schema.arity) {
      "*"
    } else {
      "*, rowNum"
    }
  }

  override def toString: String = {
    var result =
      s"${getStrategy()}(orderBy: (${Rank.sortFieldsToString(sortCollation, schema.relDataType)})"
    if (partitionKey.nonEmpty) {
      result += s", partitionBy: (${partitionFieldsToString(partitionKey, schema.relDataType)})"
    }
    result += s", $selectToString"
    result += s", ${rankRange.toString(inputSchema.fieldNames)})"
    result
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("rankFunction", rankFunction.getKind)
      .itemIf("partitionBy",
        partitionFieldsToString(partitionKey, schema.relDataType),
        partitionKey.nonEmpty)
      .item("orderBy", Rank.sortFieldsToString(sortCollation, schema.relDataType))
      .item("rankRange", rankRange.toString(inputSchema.fieldNames))
      .item("strategy", getStrategy())
      .item("select", selectToString)
  }

  override def translateToPlan(
    tableEnv: StreamTableEnvironment,
    queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    val rankKind = rankFunction.getKind match {
      case SqlKind.ROW_NUMBER => SqlKind.ROW_NUMBER
      case SqlKind.RANK =>
        throw TableException("RANK() on streaming table is not supported currently")
      case SqlKind.DENSE_RANK =>
        throw TableException("DENSE_RANK() on streaming table is not supported currently")
      case k =>
        throw TableException(s"Streaming tables do not support $k rank function.")
    }

    val inputTransform = getInput.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)

    val inputRowTypeInfo = new BaseRowTypeInfo(classOf[BaseRow], inputSchema.fieldTypeInfos: _*)
    val fieldCollation = sortCollation.getFieldCollations.asScala
    val sortKeySelector = createSortKeySelector(fieldCollation, inputSchema)
    val (sortKeyType, sorter) = createSortKeyTypeAndSorter(inputSchema, fieldCollation)

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val cacheSize = queryConfig.getTopNCacheSize

    val processFunction = getStrategy(Some(queryConfig), forceRecompute = true) match {
      case AppendFastRank =>
        new AppendRankFunction(
          inputRowTypeInfo,
          sortKeyType,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          generateRetraction,
          queryConfig)

      case UpdateFastRank(primaryKeys) =>
        val rowKeyType = createRowKeyType(primaryKeys, inputSchema)
        val rowKeySelector = createKeySelector(primaryKeys, inputSchema)
        new UpdateRankFunction(
          inputRowTypeInfo,
          rowKeyType,
          rowKeySelector,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          generateRetraction,
          queryConfig)

      case ApproxUpdateRank(primaryKeys) =>
        val approxBufferMultiplier = queryConfig.getTopNApproxBufferMultiplier
        val approxBufferMinSize = queryConfig.getTopNApproxBufferMinSize
        val rowKeyType = createRowKeyType(primaryKeys, inputSchema)
        val rowKeySelector = createKeySelector(primaryKeys, inputSchema)
        new ApproxUpdateRankFunction(
          inputRowTypeInfo,
          rowKeyType,
          rowKeySelector,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          approxBufferMultiplier,
          approxBufferMinSize,
          generateRetraction,
          queryConfig)

      case UnaryUpdateRank(primaryKeys) =>
        // unary update rank requires a key selector that returns key of other types rather
        // than BaseRow
        val genSortKeyExtractor = getUnarySortKeyExtractor(fieldCollation, inputSchema)
        val unarySortKeyType = inputSchema.fieldTypeInfos(fieldCollation.head.getFieldIndex)
        val rowKeyType = createRowKeyType(primaryKeys, inputSchema)
        val rowKeySelector = createKeySelector(primaryKeys, inputSchema)
        new UnarySortUpdateRankFunction[Any](
          inputRowTypeInfo,
          rowKeyType,
          unarySortKeyType,
          rowKeySelector,
          genSortKeyExtractor,
          getOrderFromFieldCollation(fieldCollation.head),
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          generateRetraction,
          queryConfig)

      case RetractRank =>
        new RetractRankFunction(
          inputRowTypeInfo,
          sortKeyType,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          generateRetraction,
          queryConfig)
    }
    val outputBaseInfo = schema.typeInfo(classOf[BaseRow]).asInstanceOf[BaseRowTypeInfo[BaseRow]]
    val rankOpName = this.toString

    val inputTypeInfo = inputSchema.typeInfo(classOf[BaseRow])
    val selector = StreamExecUtil.getKeySelector(partitionKey, inputTypeInfo)

    val operator = new KeyedProcessOperator(processFunction)
    operator.setRequireState(true)
    val ret = new OneInputTransformation(
      inputTransform,
      rankOpName,
      operator,
      outputBaseInfo,
      tableEnv.execEnv.getParallelism)

    if (partitionKey.isEmpty) {
      ret.forceNonParallel()
    }

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
