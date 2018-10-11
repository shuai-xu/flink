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

package org.apache.flink.table.plan.rules.physical.stream

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.{SqlAggFunction, SqlKind}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList}
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.calcite.{FlinkRelBuilder, StreamExecRelFactories}
import org.apache.flink.table.runtime.functions.aggfunctions.CountDistinct.CountDistinctAggFunction
import org.apache.flink.table.runtime.functions.aggfunctions.{FirstValueWithRetractAggFunction, LastValueWithRetractAggFunction, MaxWithRetractAggFunction, MinWithRetractAggFunction}
import org.apache.flink.table.functions.sql.{AggSqlFunctions, ScalarSqlFunctions}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions.DIVIDE
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecExchange, StreamExecGroupAggregate}
import org.apache.flink.table.plan.rules.physical.stream.StreamExecSplitAggregateRule._
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule._
import org.apache.flink.table.plan.util.{AggregateInfo, AggregateUtil}
import org.apache.flink.table.plan.util.AggregateUtil.{doAllSupportSplit, transformToStreamAggregateInfoList}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule that convert aggregations containing DataView
  * to partial aggregations and final aggregations.
  *
  * This rule rewrites an aggregate query with aggregations containing DataView such as
  * count distinct aggregations into an expanded double aggregation. The first aggregation compute
  * the results in sub partition and the results are combined by the second aggregation.
  *
  * Examples:
  *
  * MyTable: a: BIGINT, b: INT, c: VARCHAR
  *
  * Original records:
  * +-----+-----+-----+
  * |  a  |  b  |  c  |
  * +-----+-----+-----+
  * |  1  |  1  |  c1 |
  * +-----+-----+-----+
  * |  1  |  2  |  c1 |
  * +-----+-----+-----+
  * |  2  |  1  |  c2 |
  * +-----+-----+-----+
  *
  * SQL:
  * SELECT SUM(b), COUNT(DISTINCT c), AVG(b) FROM MyTable GROUP BY a
  *
  * Plan:
  * {{{
  * StreamExecCalc(select=[$f1 AS EXPR$0, $f2 AS EXPR$1, CAST(/($f3, $f4)) AS EXPR$2])
  *   StreamExecGroupAggregate(groupBy=[a], select=[a, SUM($f2) AS $f1, $SUM0($f3_0) AS $f2,
  *       $SUM0($f4) AS $f3, $SUM0($f5) AS $f4])
  *     StreamExecExchange(distribution=[hash[a]])
  *       StreamExecGroupAggregate(groupBy=[a, $f3], select=[a, $f3, SUM(b) FILTER $g_1 AS $f2,
  *           COUNT(DISTINCT c) FILTER $g_0 AS $f3_0, $SUM0(b) FILTER $g_1 AS $f4,
  *           COUNT(b) FILTER $g_1 AS $f5])
  *         StreamExecExchange(distribution=[hash[a, $f3]])
  *           StreamExecCalc(select=[a, b, c, $f3, =($e, 1) AS $g_1, =($e, 0) AS $g_0])
  *             StreamExecExpand(expand=[a, b, c, $f3, $e])
  *               StreamExecCalc(select=[a, b, c, MOD(HASH_CODE(c), 1024) AS $f3])
  *                 StreamExecScan(table=[[_DataStreamTable_0]])
  * }}}
  *
  * '$e = 0' is equivalent to 'group by a, hash(c) % 1024'
  * '$e = 1' is equivalent to 'group by a'
  *
  * Expanded records:
  * +-----+-----+-----+------------------+-----+
  * |  a  |  b  |  c  |  hash(c) % 1024  | $e  |
  * +-----+-----+-----+------------------+-----+        ---+---
  * |  1  |  1  | null|       null       |  1  |           |
  * +-----+-----+-----+------------------+-----|  records expanded by record1
  * |  1  |  1  |  c1 |  hash(c1) % 1024 |  0  |           |
  * +-----+-----+-----+------------------+-----+        ---+---
  * |  1  |  2  | null|       null       |  1  |           |
  * +-----+-----+-----+------------------+-----+  records expanded by record2
  * |  1  |  2  |  c1 |  hash(c1) % 1024 |  0  |           |
  * +-----+-----+-----+------------------+-----+        ---+---
  * |  2  |  1  | null|       null       |  1  |           |
  * +-----+-----+-----+------------------+-----+  records expanded by record3
  * |  2  |  1  |  c2 |  hash(c2) % 1024 |  0  |           |
  * +-----+-----+-----+------------------+-----+        ---+---
  */
class StreamExecSplitAggregateRule(operand: RelOptRuleOperand, inputIsExchange: Boolean)
  extends RelOptRule(
    operand,
    StreamExecRelFactories.STREAM_EXEC_REL_BUILDER,
    "StreamExecSplitAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val queryConfig = call.getPlanner.getContext.unwrap(classOf[StreamQueryConfig])
    val agg: StreamExecGroupAggregate = call.rel(0)
    val realInput = if (inputIsExchange) call.rels(2) else call.rels(1)

    val needRetraction = StreamExecRetractionRules.isAccRetract(realInput)
    val modifiedMono = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
      .getRelModifiedMonotonicity(agg)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      agg.getGroupings.length, needRetraction, modifiedMono, agg.aggCalls)

    val aggInfoList = transformToStreamAggregateInfoList(
      agg.aggCalls,
      agg.getInput.getRowType,
      needRetractionArray,
      needInputCount = false,
      isStateBackendDataViews = true,
      needDistinctInfo = false)

    call.rels(1).isInstanceOf[StreamExecExchange] == inputIsExchange &&
      queryConfig.isMiniBatchEnabled &&
      queryConfig.isPartialAggEnabled &&
      !agg.skipSplit &&
      containsAggsWithDataView(aggInfoList.aggInfos) &&
      doAllSupportSplit(aggInfoList.aggInfos)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val queryConfig = call.getPlanner.getContext.unwrap(classOf[StreamQueryConfig])
    val originalAggregate: StreamExecGroupAggregate = call.rel(0)
    val realInput = if (inputIsExchange) call.rels(2) else call.rels(1)
    val needRetraction = StreamExecRetractionRules.isAccRetract(realInput)
    val modifiedMono = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
      .getRelModifiedMonotonicity(originalAggregate)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      originalAggregate.getGroupings.length, needRetraction, modifiedMono,
      originalAggregate.aggCalls)
    val aggInfos = transformToStreamAggregateInfoList(
      originalAggregate.aggCalls,
      originalAggregate.getInput.getRowType,
      needRetractionArray,
      needInputCount = false,
      isStateBackendDataViews = true,
      needDistinctInfo = false).aggInfos

    val cluster = originalAggregate.getCluster
    val relBuilder = call.builder().asInstanceOf[FlinkRelBuilder]
    relBuilder.push(realInput)

    // STEP 1: add hash fields if necessary
    val hashFieldIndexes: Array[Int] = aggInfos.flatMap { aggInfo =>
      if (containsDataView(aggInfo)) {
        aggInfo.argIndexes
      } else {
        Array.empty[Int]
      }
    }.distinct.diff(originalAggregate.getGroupings).sorted

    val hashFieldsMap: mutable.Map[Int, Int] = mutable.Map()
    val buckets = queryConfig.getPartialBucketNum

    if (hashFieldIndexes.nonEmpty) {
      val projects = new util.ArrayList[RexNode](relBuilder.fields)
      val hashFieldsOffset = projects.size()

      hashFieldIndexes.zipWithIndex.foreach { case (hashFieldIdx, index) =>
        val hashField = relBuilder.field(hashFieldIdx)
        val node: RexNode = relBuilder.call(
          SqlStdOperatorTable.MOD,
          relBuilder.call(ScalarSqlFunctions.HASH_CODE, hashField),
          relBuilder.literal(buckets))
        projects.add(node)
        hashFieldsMap.put(hashFieldIdx, hashFieldsOffset + index)
      }
      relBuilder.project(projects)
    }

    // STEP 2: construct partial aggregates
    val groupSetTreeSet = new util.TreeSet[ImmutableBitSet](ImmutableBitSet.ORDERING)
    val aggInfoToGroupSetMap = new util.HashMap[AggregateInfo, ImmutableBitSet]()
    aggInfos.foreach { aggInfo =>
      val groupSet = if (containsDataView(aggInfo)) {
        val groupSet = ImmutableBitSet.of(
          aggInfo.argIndexes.map { argIndex =>
            hashFieldsMap.getOrElse(argIndex, argIndex).asInstanceOf[Integer]
          }.toSeq).union(ImmutableBitSet.of(originalAggregate.getGroupings: _*))
        groupSet
      } else {
        ImmutableBitSet.of(originalAggregate.getGroupings: _*)
      }
      groupSetTreeSet.add(groupSet)
      aggInfoToGroupSetMap.put(aggInfo, groupSet)
    }
    val groupSets = ImmutableList.copyOf(asJavaIterable(groupSetTreeSet))
    val fullGroupSet = ImmutableBitSet.union(groupSets)

    // STEP 2.1: expand input fields
    val partialAggCalls = new util.ArrayList[AggregateCall]
    val partialAggCallToGroupSetMap = new util.HashMap[AggregateCall, ImmutableBitSet]()
    aggInfos.foreach { aggInfo =>
      val newAggCalls = getPartialAggFunction(aggInfo).map { aggFunction =>
        AggregateCall.create(
          aggFunction, aggInfo.agg.isDistinct, aggInfo.agg.isApproximate, aggInfo.agg.getArgList,
          aggInfo.agg.filterArg, fullGroupSet.cardinality, relBuilder.peek(), null, null)
      }
      partialAggCalls.addAll(newAggCalls)
      newAggCalls.foreach { aggCall =>
        partialAggCallToGroupSetMap.put(aggCall, aggInfoToGroupSetMap.get(aggInfo))
      }
    }
    val needExpand = groupSets.size() > 1
    val duplicateFieldMap = if (needExpand) {
      val (duplicateFieldMap, _) = buildExpandNode(
        cluster, relBuilder, partialAggCalls, fullGroupSet, groupSets)
      duplicateFieldMap
    } else {
      Map.empty[Integer, Integer]
    }

    // STEP 2.2: add filter columns for partial aggregates
    val filters = new mutable.LinkedHashMap[(ImmutableBitSet, Integer), Integer]
    val newPartialAggCalls = new util.ArrayList[AggregateCall]
    if (needExpand) {
      // GROUPING returns an integer (0, 1, 2...). Add a project to convert those values to BOOLEAN.
      val nodes = new util.ArrayList[RexNode](relBuilder.fields)
      val expandIdNode = nodes.remove(nodes.size - 1)
      val filterColumnsOffset: Int = nodes.size
      var x: Int = 0
      partialAggCalls.zipWithIndex.foreach { case (aggCall, index) =>
        val groupSet = partialAggCallToGroupSetMap.get(aggCall)
        val oldFilterArg = aggCall.filterArg
        val newArgList = aggCall.getArgList.map(a => duplicateFieldMap.getOrElse(a, a)).toList

        if (!filters.contains(groupSet, oldFilterArg)) {
          val expandId = genExpandId(fullGroupSet, groupSet)
          if (oldFilterArg >= 0) {
            nodes.add(relBuilder.alias(
              relBuilder.and(
                relBuilder.equals(expandIdNode, relBuilder.literal(expandId)),
                relBuilder.field(oldFilterArg)),
              "$g_" + expandId))
          } else {
            nodes.add(relBuilder.alias(
              relBuilder.equals(expandIdNode, relBuilder.literal(expandId)), "$g_" + expandId))
          }
          val newFilterArg = filterColumnsOffset + x
          filters.put((groupSet, oldFilterArg), newFilterArg)
          x += 1
        }

        val newFilterArg = filters((groupSet, oldFilterArg))
        val newAggCall = aggCall.adaptTo(
          relBuilder.peek(), newArgList, newFilterArg,
          fullGroupSet.cardinality, fullGroupSet.cardinality)
        newPartialAggCalls.add(newAggCall)
      }
      relBuilder.project(nodes)
    } else {
      newPartialAggCalls.addAll(partialAggCalls)
    }

    // STEP 2.3: construct partial aggregates
    relBuilder.aggregate(
      relBuilder.groupKey(fullGroupSet, ImmutableList.of[ImmutableBitSet](fullGroupSet)),
      newPartialAggCalls)
    relBuilder.peek().asInstanceOf[StreamExecGroupAggregate].setSkipSplit(true)

    // STEP 3: construct final aggregates
    val finalAggInputOffset = fullGroupSet.cardinality
    var x: Int = 0
    val finalAggCalls = new util.ArrayList[AggregateCall]
    var needMergeFinalAggOutput: Boolean = false
    aggInfos.foreach { aggInfo =>
      val newAggCalls = getFinalAggFunction(aggInfo).map { aggFunction =>
        val newArgList = ImmutableIntList.of(finalAggInputOffset + x)
        x += 1

        AggregateCall.create(
          aggFunction, false, aggInfo.agg.isApproximate, newArgList, -1,
          originalAggregate.getGroupings.length, relBuilder.peek(), null, null)
      }

      finalAggCalls.addAll(newAggCalls)
      if (newAggCalls.size > 1) {
        needMergeFinalAggOutput = true
      }
    }
    relBuilder.aggregate(
      relBuilder.groupKey(
        remap(fullGroupSet, originalAggregate.getGroupSet),
        remap(fullGroupSet, Seq(originalAggregate.getGroupSet))),
      finalAggCalls)
    val finalAggregate = relBuilder.peek().asInstanceOf[StreamExecGroupAggregate]
    finalAggregate.setSkipSplit(skipSplit = true)

    // STEP 4: convert final aggregation output to the original aggregation output.
    // For example, aggregate function AVG is transformed to SUM0 and COUNT, so the output of
    // the final aggregation is (sum, count). We should converted it to (sum / count)
    // for the final output.
    if (needMergeFinalAggOutput) {
      val nodes = new util.ArrayList[RexNode]
      (0 until finalAggregate.getGroupings.length).foreach { index =>
        nodes.add(RexInputRef.of(index, finalAggregate.getRowType))
      }

      var avgAggCount: Int = 0
      aggInfos.zipWithIndex.foreach { case (aggInfo, index) =>
        val newNode = if (aggInfo.agg.getAggregation.getKind == SqlKind.AVG) {
          val sumInputRef = RexInputRef.of(
            finalAggregate.getGroupings.length + index + avgAggCount,
            finalAggregate.getRowType)
          val countInputRef = RexInputRef.of(
            finalAggregate.getGroupings.length + index + avgAggCount + 1,
            finalAggregate.getRowType)
          avgAggCount += 1
          relBuilder.call(DIVIDE, sumInputRef, countInputRef)
        } else {
          RexInputRef.of(
            finalAggregate.getGroupings.length + index + avgAggCount,
            finalAggregate.getRowType)
        }
        nodes.add(newNode)
      }
      relBuilder.project(nodes)
    }

    relBuilder.convert(originalAggregate.getRowType, false)

    call.transformTo(relBuilder.build())
  }
}

object StreamExecSplitAggregateRule {
  val INSTANCE_WITHOUT_EXCHANGE: RelOptRule = new StreamExecSplitAggregateRule(
    operand(
      classOf[StreamExecGroupAggregate],
      operand(classOf[RelNode], any)), false)

  val INSTANCE_WITH_EXCHANGE: RelOptRule = new StreamExecSplitAggregateRule(
    operand(
      classOf[StreamExecGroupAggregate],
      operand(
        classOf[StreamExecExchange],
        operand(classOf[RelNode], any))), true)

  val PARTIAL_FINAL_MAP: Map[SqlAggFunction, (Seq[SqlAggFunction], Seq[SqlAggFunction])] = Map(
    AVG -> (Seq(SUM0, COUNT), Seq(SUM0, SUM0)),
    COUNT -> (Seq(COUNT), Seq(SUM0)),
    MIN -> (Seq(MIN), Seq(MIN)),
    MAX -> (Seq(MAX), Seq(MAX)),
    SUM -> (Seq(SUM), Seq(SUM)),
    SUM0 -> (Seq(SUM0), Seq(SUM0)),
    AggSqlFunctions.FIRST_VALUE ->
      (Seq(AggSqlFunctions.FIRST_VALUE), Seq(AggSqlFunctions.FIRST_VALUE)),
    AggSqlFunctions.LAST_VALUE ->
      (Seq(AggSqlFunctions.LAST_VALUE), Seq(AggSqlFunctions.LAST_VALUE)),
    AggSqlFunctions.CONCAT_AGG ->
      (Seq(AggSqlFunctions.CONCAT_AGG), Seq(AggSqlFunctions.CONCAT_AGG)),
    SINGLE_VALUE -> (Seq(SINGLE_VALUE), Seq(SINGLE_VALUE))
  )

  private def containsAggsWithDataView(aggInfos: Array[AggregateInfo]): Boolean = {
    aggInfos.exists(containsDataView)
  }

  private def containsDataView(aggInfo: AggregateInfo): Boolean = {
    aggInfo.function match {
      case _: MinWithRetractAggFunction[_] | _: MaxWithRetractAggFunction[_] |
           _: FirstValueWithRetractAggFunction[_] | _: LastValueWithRetractAggFunction[_] |
           _: CountDistinctAggFunction => true
      case _ => false
    }
  }

  private def getPartialAggFunction(aggInfo: AggregateInfo): Seq[SqlAggFunction] = {
    PARTIAL_FINAL_MAP.get(aggInfo.agg.getAggregation) match {
      case Some((partialAggFunctions, _)) => partialAggFunctions
      case None => throw new TableException(
        "Aggregation " + aggInfo.agg.getAggregation + " is not supported to split!")
    }
  }

  private def getFinalAggFunction(aggInfo: AggregateInfo): Seq[SqlAggFunction] = {
    PARTIAL_FINAL_MAP.get(aggInfo.agg.getAggregation) match {
      case Some((_, finalAggFunctions)) => finalAggFunctions
      case _ => throw new TableException(
        "Aggregation " + aggInfo.agg.getAggregation + " is not supported to split!")
    }
  }

  /**
    * Compute the group sets of the final aggregation.
    *
    * @param groupSet the group set of the previous partial aggregation
    * @param originalGroupSets the group set of the original aggregation
    */
  private def remap(groupSet: ImmutableBitSet, originalGroupSets: Iterable[ImmutableBitSet])
      : ImmutableList[ImmutableBitSet] = {
    val builder = ImmutableList.builder[ImmutableBitSet]
    for (originalGroupSet <- originalGroupSets) {
      builder.add(remap(groupSet, originalGroupSet))
    }
    builder.build
  }

  /**
    * Compute the group set of the final aggregation.
    *
    * @param groupSet the group set of the previous partial aggregation
    * @param originalGroupSet the group set of the original aggregation
    */
  private def remap(groupSet: ImmutableBitSet, originalGroupSet: ImmutableBitSet)
      : ImmutableBitSet = {
    val builder = ImmutableBitSet.builder
    for (bit <- originalGroupSet) {
      builder.set(remap(groupSet, bit))
    }
    builder.build
  }

  private def remap(groupSet: ImmutableBitSet, arg: Int): Int = {
    if (arg < 0) {
      -1
    } else {
      groupSet.indexOf(arg)
    }
  }
}
