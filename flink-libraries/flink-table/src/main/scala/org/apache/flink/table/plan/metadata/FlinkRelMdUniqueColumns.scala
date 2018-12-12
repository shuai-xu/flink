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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.metadata.FlinkMetadata.UniqueColumns
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalWindowAggregate, Rank}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.util.FlinkRelMdUtil
import org.apache.flink.table.plan.util.FlinkRelMdUtil.splitColumnsIntoLeftAndRight
import org.apache.flink.table.plan.util.FlinkRelOptUtil.checkAndSplitAggCalls

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{Bug, ImmutableBitSet, Util}

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

class FlinkRelMdUniqueColumns private extends MetadataHandler[UniqueColumns] {

  override def getDef: MetadataDef[UniqueColumns] = FlinkMetadata.UniqueColumns.DEF

  def getUniqueColumns(
      ts: TableScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val uniqueKeys = mq.getUniqueKeys(ts)
    if (uniqueKeys == null || uniqueKeys.isEmpty) {
      return columns
    }
    require(columns.forall(_ < ts.getRowType.getFieldCount))
    val none = Option.empty[ImmutableBitSet]
    // find the minimum uniqueKey
    val uniqueColumns = uniqueKeys.foldLeft(none) {
      (uniqueColumns, uniqueKey) =>
        val containUniqueKey = columns.contains(uniqueKey)
        uniqueColumns match {
          case Some(uniqueCols) =>
            if (containUniqueKey && uniqueCols.cardinality() > uniqueKey.cardinality()) {
              Some(uniqueKey)
            } else {
              uniqueColumns
            }
          case _ => if (containUniqueKey) Some(uniqueKey) else none
        }
    }
    uniqueColumns.getOrElse(columns)
  }

  def getUniqueColumns(
      expand: Expand,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val columnsSkipExpandId = columnList.filter(_ != expand.expandIdIndex)
    if (columnsSkipExpandId.isEmpty) {
      return columns
    }
    val inputUniqueCols = fmq.getUniqueColumns(
      expand.getInput, ImmutableBitSet.of(columnsSkipExpandId))
    if (columnList.contains(expand.expandIdIndex)) {
      inputUniqueCols.union(ImmutableBitSet.of(expand.expandIdIndex))
    } else {
      inputUniqueCols
    }
  }

  def getUniqueColumns(
      rank: Rank,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rankFunColumnIndex = FlinkRelMdUtil.getRankFunColumnIndex(rank)
    val columnSkipRankCol = columnList.filter(_ != rankFunColumnIndex)
    if (columnSkipRankCol.isEmpty) {
      return columns
    }
    val inputUniqueCols = fmq.getUniqueColumns(
      rank.getInput, ImmutableBitSet.of(columnSkipRankCol))
    if (columnList.contains(rankFunColumnIndex)) {
      inputUniqueCols.union(ImmutableBitSet.of(rankFunColumnIndex))
    } else {
      inputUniqueCols
    }
  }

  def getUniqueColumns(
      filter: Filter,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueColumns(filter.getInput, columns)
  }

  def getUniqueColumns(
      project: Project,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val projects = project.getProjects
    getUniqueColumnsOfProject(projects, project.getInput, mq, columns)
  }

  def getUniqueColumns(
      calc: Calc,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getUniqueColumnsOfProject(projects, calc.getInput, mq, columns)
  }

  private def getUniqueColumnsOfProject(
      projects: util.List[RexNode],
      input: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val mapInToOutRefPos = new mutable.HashMap[Integer, Integer]()
    // find non-RexInputRef and non-Constant columns
    val outNonRefOrConstantCols = new mutable.ArrayBuffer[Integer]()
    columns.foreach { column =>
      require(column < projects.size)
      projects.get(column) match {
        case ref: RexInputRef => mapInToOutRefPos.putIfAbsent(ref.getIndex, column)
        case call: RexCall if call.getKind.equals(SqlKind.AS) &&
          call.getOperands.head.isInstanceOf[RexInputRef] =>
          val index = call.getOperands.head.asInstanceOf[RexInputRef].getIndex
          mapInToOutRefPos.putIfAbsent(index, column)
        case _: RexLiteral => // do nothing
        case _ => outNonRefOrConstantCols += column
      }
    }

    if (mapInToOutRefPos.isEmpty) {
      val nonConstantCols = columnList.filterNot { column =>
        projects.get(column).isInstanceOf[RexLiteral]
      }
      if (nonConstantCols.isEmpty) {
        // all columns are constant, return first column
        ImmutableBitSet.of(columnList.head)
      } else {
        // return non-constant columns
        ImmutableBitSet.of(nonConstantCols)
      }
    } else {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val inputColumns = ImmutableBitSet.of(mapInToOutRefPos.keys.toList)
      val inputUniqueCols = fmq.getUniqueColumns(input, inputColumns)
      val outputUniqueCols = inputUniqueCols.asList.map {
        k => mapInToOutRefPos.getOrElse(k, throw new IllegalArgumentException(s"Illegal index: $k"))
      }
      ImmutableBitSet.of(outputUniqueCols).union(ImmutableBitSet.of(outNonRefOrConstantCols))
    }
  }

  def getUniqueColumns(
      exchange: Exchange,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueColumns(exchange.getInput, columns)
  }

  def getUniqueColumns(
      rel: SetOp,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueColumns(
      sort: Sort,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueColumns(sort.getInput, columns)
  }

  def getUniqueColumns(
      rel: Correlate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueColumns(
      rel: BatchExecCorrelate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueColumns(
      join: Join,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    require(join.getSystemFieldList.isEmpty)
    val leftFieldCount = join.getLeft.getRowType.getFieldCount
    val (leftColumns, rightColumns) = splitColumnsIntoLeftAndRight(leftFieldCount, columns)

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val leftUniqueCols = fmq.getUniqueColumns(join.getLeft, leftColumns)
    val rightUniqueCols = fmq.getUniqueColumns(join.getRight, rightColumns)

    val joinType = join.getJoinType
    val joinInfo = join.analyzeCondition()
    val leftJoinKeys = ImmutableBitSet.of(joinInfo.leftKeys)
    val rightJoinKeys = ImmutableBitSet.of(joinInfo.rightKeys)
    // for INNER and LEFT join, returns leftUniqueColumns if the join keys of RHS are unique
    if (leftJoinKeys.nonEmpty
      && leftUniqueCols.contains(leftJoinKeys)
      && !joinType.generatesNullsOnLeft()) {
      val isRightJoinKeysUnique = fmq.areColumnsUnique(join.getRight, rightJoinKeys)
      if (isRightJoinKeysUnique != null && isRightJoinKeysUnique) {
        return leftUniqueCols
      }
    }

    val outputRightUniqueCols = rightUniqueCols.asList.map(c => Integer.valueOf(c + leftFieldCount))
    // for INNER and RIGHT join, returns rightUniqueColumns if the join keys of LHS are unique
    if (rightJoinKeys.nonEmpty
      && rightUniqueCols.contains(rightJoinKeys)
      && !joinType.generatesNullsOnRight()) {
      val isLeftJoinKeysUnique = fmq.areColumnsUnique(join.getLeft, leftJoinKeys)
      if (isLeftJoinKeysUnique != null && isLeftJoinKeysUnique) {
        return ImmutableBitSet.of(outputRightUniqueCols)
      }
    }

    leftUniqueCols.union(ImmutableBitSet.of(outputRightUniqueCols))
  }

  def getUniqueColumns(
      semiJoin: SemiJoin,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    require(semiJoin.getSystemFieldList.isEmpty)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getUniqueColumns(semiJoin.getLeft, columns)
  }

  def getUniqueColumns(
      agg: Aggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = agg.getGroupSet.map(_.toInt).toArray
    getUniqueColumnsOfAggregate(agg.getRowType.getFieldCount, grouping, agg.getInput, mq, columns)
  }

  def getUniqueColumns(
      agg: BatchExecGroupAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = agg.getGrouping
    getUniqueColumnsOfAggregate(agg.getRowType.getFieldCount, grouping, agg.getInput, mq, columns)
  }

  private def getUniqueColumnsOfAggregate(
      outputFiledCount: Int,
      grouping: Array[Int],
      input: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val columnList = columns.toList
    val groupingInToOutMap = new mutable.HashMap[Integer, Integer]()
    columnList.foreach { column =>
      require(column < outputFiledCount)
      if (column < grouping.length) {
        groupingInToOutMap.put(grouping(column), column)
      }
    }
    if (groupingInToOutMap.isEmpty) {
      columns
    } else {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val inputColumns = ImmutableBitSet.of(groupingInToOutMap.keys.toList)
      val inputUniqueCols = fmq.getUniqueColumns(input, inputColumns)
      val groupingUniqueCols = inputUniqueCols.asList.map { k =>
        groupingInToOutMap.getOrElse(k, throw new IllegalArgumentException(s"Illegal index: $k"))
      }
      val nonGroupingCols = if (inputColumns.toArray.sorted.sameElements(grouping.sorted)) {
        // if values of inputColumns are grouping columns, nonGroupingCols can be dropped.
        // (because grouping columns are unique.)
        Seq.empty[Integer]
      } else {
        val groupingOutColumns = groupingInToOutMap.values
        columnList.filterNot(groupingOutColumns.contains(_))
      }
      ImmutableBitSet.of(groupingUniqueCols).union(ImmutableBitSet.of(nonGroupingCols))
    }
  }

  def getUniqueColumns(
      window: LogicalWindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = window.getGroupSet.map(_.toInt).toArray
    val namedProperties = window.getNamedProperties
    val (auxGroupSet, _) = checkAndSplitAggCalls(window)
    if (window.indicator) {
      require(auxGroupSet.isEmpty)
    }
    getUniqueColumnsOfWindow(window, grouping, auxGroupSet, namedProperties, mq, columns)
  }

  def getUniqueColumns(
      window: FlinkLogicalWindowAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = window.getGroupSet.map(_.toInt).toArray
    val namedProperties = window.getNamedProperties
    val (auxGroupSet, _) = checkAndSplitAggCalls(window)
    getUniqueColumnsOfWindow(window, grouping, auxGroupSet, namedProperties, mq, columns)
  }

  def getUniqueColumns(
      window: BatchExecWindowAggregateBase,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val grouping = window.getGrouping
    val namedProperties = window.getNamedProperties
    getUniqueColumnsOfWindow(window, grouping, window.getAuxGrouping, namedProperties, mq, columns)
  }

  private def getUniqueColumnsOfWindow(
      window: SingleRel,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      namedProperties: Seq[NamedWindowProperty],
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val fieldCount = window.getRowType.getFieldCount
    val columnList = columns.toList
    val groupingInToOutMap = new mutable.HashMap[Integer, Integer]()
    columnList.foreach { column =>
      require(column < fieldCount)
      if (column < grouping.length) {
        groupingInToOutMap.put(grouping(column), column)
      }
    }
    if (groupingInToOutMap.isEmpty) {
      columns
    } else {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      val inputColumns = ImmutableBitSet.of(groupingInToOutMap.keys.toList)
      val inputUniqueCols = fmq.getUniqueColumns(window.getInput, inputColumns)
      val groupingUniqueCols = inputUniqueCols.asList.map { i =>
        groupingInToOutMap.getOrElse(i, throw new IllegalArgumentException(s"Illegal index: $i"))
      }
      if (columns.equals(ImmutableBitSet.of(grouping ++ auxGrouping: _*))) {
        return ImmutableBitSet.of(groupingUniqueCols)
      }

      val groupingOutCols = groupingInToOutMap.values
      // TODO drop some nonGroupingCols base on FlinkRelMdColumnUniqueness#areColumnsUnique(window)
      val nonGroupingCols = columnList.filterNot(groupingOutCols.contains)
      ImmutableBitSet.of(groupingUniqueCols).union(ImmutableBitSet.of(nonGroupingCols))
    }
  }

  def getUniqueColumns(
      over: Window,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    getUniqueColumnsOfOver(over.getRowType.getFieldCount, over.getInput, mq, columns)
  }

  def getUniqueColumns(
      over: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    getUniqueColumnsOfOver(over.getRowType.getFieldCount, over.getInput, mq, columns)
  }

  private def getUniqueColumnsOfOver(
      outputFiledCount: Int,
      input: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    val inputFieldCount = input.getRowType.getFieldCount
    val (inputColumns, nonInputColumns) = columns.toList.partition(_ < inputFieldCount)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputUniqueCols = fmq.getUniqueColumns(input, ImmutableBitSet.of(inputColumns))
    inputUniqueCols.union(ImmutableBitSet.of(nonInputColumns))
  }

  // Catch-all rule when none of the others apply.
  def getUniqueColumns(
      rel: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = columns

  def getUniqueColumns(
      rel: RelSubset,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet): ImmutableBitSet = {
    if (!Bug.CALCITE_1048_FIXED) {
      //if the best node is null, so we can get the uniqueKeys based original node, due to
      //the original node is logically equivalent as the rel.
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
      fmq.getUniqueColumns(Util.first(rel.getBest, rel.getOriginal), columns)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

}

object FlinkRelMdUniqueColumns {

  private val INSTANCE = new FlinkRelMdUniqueColumns

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.UniqueColumns.METHOD, INSTANCE)

}
