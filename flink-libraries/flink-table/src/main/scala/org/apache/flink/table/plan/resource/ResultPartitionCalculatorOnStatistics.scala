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

package org.apache.flink.table.plan.resource

import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelDistribution, RelNode, SingleRel}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.util.{BatchExecResourceUtil, Logging}
import java.lang.{Boolean => JBool}

/**
  * Infer result partition count according to statistics.
  */
class ResultPartitionCalculatorOnStatistics(
    tableConfig: TableConfig,
    tableEnv: BatchTableEnvironment,
    runningUnitKeeper: RunningUnitKeeper,
    mq: RelMetadataQuery)
    extends BatchExecRelVisitor[Unit] with Logging {

  private def setResultParallelism(
      rel: BatchExecRel[_], parallelism: Int, fixed: JBool): Unit = {
    runningUnitKeeper.getRelShuffleStage(rel).setResultParallelism(parallelism, fixed)
  }

  private def calculateSource(scanBatchExec: BatchExecScan): Unit = {
    val (locked, sourceParallelism) = scanBatchExec.getTableSourceResultPartitionNum(tableEnv)
    // we expect sourceParallelism > 0 always, only for the mocked test case.
    if (locked && sourceParallelism > 0) {
      // if parallelism locked, use set parallelism directly.
      setResultParallelism(scanBatchExec, sourceParallelism, true)
      return
    }

    val rowCount = mq.getRowCount(scanBatchExec)
    val io = rowCount * mq.getAverageRowSize(scanBatchExec)
    LOG.info("source row count is : " + rowCount)
    LOG.info("source data size is : " + io)
    val rowsPerPartition = BatchExecResourceUtil.getRelCountPerPartition(tableConfig)
    val sizePerPartition = BatchExecResourceUtil.getSourceSizePerPartition(tableConfig)
    val maxNum = BatchExecResourceUtil.getSourceMaxParallelism(tableConfig)
    val count = Math.min(maxNum,
      Math.max(
        Math.max(
          io / sizePerPartition / BatchExecResourceUtil.SIZE_IN_MB,
          rowCount / rowsPerPartition),
        1).toInt)
    setResultParallelism(scanBatchExec, count, false)
  }

  override def visit(boundedStreamScan: BatchExecBoundedDataStreamScan): Unit = {
    calculateSource(boundedStreamScan)
  }

  override def visit(scanTableSource: BatchExecTableSourceScan): Unit = {
    calculateSource(scanTableSource)
  }

  override def visit(values: BatchExecValues): Unit = {
    setResultParallelism(values, 1, true)
  }

  private def calculateSingle[T <: SingleRel with RowBatchExecRel](singleRel: T): Unit = {
    visitChildren(singleRel)
    singleRel.getInput(0) match {
      case ex: BatchExecExchange =>
        if (ex.distribution.getType == RelDistribution.Type.SINGLETON) {
          setResultParallelism(singleRel, 1, true)
          return
        }
      case _ => Unit
    }
    val maxParallelism = BatchExecResourceUtil.getOperatorMaxParallelism(tableConfig)
    val rowCount = mq.getRowCount(singleRel.getInput)
    val minParallelism = BatchExecResourceUtil.getOperatorMinParallelism(tableConfig)

    val resultParallelism = Math.max(
      Math.min(rowCount /
        BatchExecResourceUtil.getRelCountPerPartition(tableConfig), maxParallelism),
      minParallelism).toInt
    setResultParallelism(singleRel, resultParallelism, false)
  }

  private def calculateBiRel[T <: BiRel with RowBatchExecRel](biRel: T): Unit = {
    visitChildren(biRel)
    val maxParallelism = BatchExecResourceUtil.getOperatorMaxParallelism(tableConfig)
    val leftRowCount = mq.getRowCount(biRel.getLeft)
    val rightRowCount = mq.getRowCount(biRel.getRight)
    val maxRowCount = Math.max(leftRowCount, rightRowCount)
    val minParallelism = BatchExecResourceUtil.getOperatorMinParallelism(tableConfig)

    val resultParallelism = Math.max(
      Math.min(maxParallelism,
        maxRowCount / BatchExecResourceUtil.getRelCountPerPartition(tableConfig)),
      minParallelism).toInt
    setResultParallelism(biRel, resultParallelism, false)
  }

  override def visit(calc: BatchExecCalc): Unit = calculateSingle(calc)

  override def visit(correlate: BatchExecCorrelate): Unit = calculateSingle(correlate)

  override def visit(exchange: BatchExecExchange): Unit = {
    visitChildren(exchange)
  }

  override def visit(reused: BatchExecReused): Unit = visitChildren(reused)

  override def visit(expand: BatchExecExpand): Unit = calculateSingle(expand)

  override def visit(hashAggregate: BatchExecHashAggregate): Unit = calculateSingle(hashAggregate)

  override def visit(hashAggregate: BatchExecHashWindowAggregate): Unit =
    calculateSingle(hashAggregate)

  override def visit(hashJoin: BatchExecHashJoinBase): Unit = calculateBiRel(hashJoin)

  override def visit(sortMergeJoin: BatchExecSortMergeJoinBase): Unit =
    calculateBiRel(sortMergeJoin)

  override def visit(nestedLoopJoin: BatchExecNestedLoopJoinBase): Unit =
    calculateBiRel(nestedLoopJoin)

  override def visit(localHashAggregate: BatchExecLocalHashAggregate): Unit =
    calculateSingle(localHashAggregate)

  override def visit(sortAggregate: BatchExecSortAggregate): Unit = calculateSingle(sortAggregate)

  override def visit(localHashAggregate: BatchExecLocalHashWindowAggregate): Unit =
    calculateSingle(localHashAggregate)

  override def visit(localSortAggregate: BatchExecLocalSortAggregate): Unit =
    calculateSingle(localSortAggregate)

  override def visit(localSortAggregate: BatchExecLocalSortWindowAggregate): Unit =
    calculateSingle(localSortAggregate)

  override def visit(sortAggregate: BatchExecSortWindowAggregate): Unit =
    calculateSingle(sortAggregate)

  override def visit(overWindowAgg: BatchExecOverAggregate): Unit = calculateSingle(overWindowAgg)

  override def visit(limit: BatchExecLimit): Unit = calculateSingle(limit)

  override def visit(sort: BatchExecSort): Unit = calculateSingle(sort)

  override def visit(sortLimit: BatchExecSortLimit): Unit = calculateSingle(sortLimit)

  override def visit(union: BatchExecUnion): Unit = visitChildren(union)

  override def visit(joinTable: BatchExecJoinTable): Unit = calculateSingle(joinTable)

  override def visit(batchExec: BatchExecRel[_]): Unit = {
    throw new TableException("could not reach here. " + batchExec.getClass)
  }

  def visitChildren(rowBatchExec: RowBatchExecRel): Unit = {
    import _root_.scala.collection.JavaConversions._
    rowBatchExec.getInputs.foreach((child: RelNode) =>
      child.asInstanceOf[RowBatchExecRel].accept(this))
  }

  override def visit(segmentTop: BatchExecSegmentTop): Unit = calculateSingle(segmentTop)
}
