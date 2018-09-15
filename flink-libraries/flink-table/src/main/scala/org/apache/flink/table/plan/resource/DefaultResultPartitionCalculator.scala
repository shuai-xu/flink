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

import org.apache.calcite.rel.{RelDistribution, RelNode, SingleRel}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.util.BatchExecResourceUtil.InferMode
import org.apache.flink.table.util.{BatchExecResourceUtil, Logging}
import org.apache.flink.util.Preconditions

/**
  * Calculating resultPartitionCount with visiting every [[BatchExecRel]].
  * @param tConfig table config.
  */
class DefaultResultPartitionCalculator(
    tConfig: TableConfig,
    tEnv: BatchTableEnvironment)
  extends BatchExecRelVisitor[Unit] with Logging {

  def calculate(scanBatchExec: BatchExecScan): Unit = {
    val (locked, sourceParallelism) = scanBatchExec.getTableSourceResultPartitionNum(tEnv)
    // we expect sourceParallelism > 0 always, only for the mocked test case.
    if (locked && sourceParallelism > 0) {
      // if parallelism is locked, use set parallelism directly.
      scanBatchExec.resultPartitionCount = sourceParallelism
      return
    }

    val infer = !BatchExecResourceUtil.getInferMode(tConfig).equals(InferMode.NONE)
    LOG.info("infer source partitions num: " + infer)
    if (infer) {
      val mq = scanBatchExec.getCluster.getMetadataQuery
      val rowCount = mq.getRowCount(scanBatchExec)
      val io = rowCount * mq.getAverageRowSize(scanBatchExec)
      LOG.info("source row count is : " + rowCount)
      LOG.info("source data size is : " + io)
      val rowsPerPartition = BatchExecResourceUtil.getRelCountPerPartition(tConfig)
      val sizePerPartition = BatchExecResourceUtil.getSourceSizePerPartition(tConfig)
      val maxNum = BatchExecResourceUtil.getSourceMaxParallelism(tConfig)
      scanBatchExec.resultPartitionCount = Math.min(maxNum,
        Math.max(
          Math.max(
            io / sizePerPartition / BatchExecResourceUtil.SIZE_IN_MB,
            rowCount / rowsPerPartition),
          1).toInt)
    } else {
      scanBatchExec.resultPartitionCount = BatchExecResourceUtil
          .getOperatorDefaultParallelism(tConfig)
    }
  }

  def calculate(unionBatchExec: BatchExecUnion): Unit = {
    if (unionBatchExec.resultPartitionCount > 0) return
    visitChildren(unionBatchExec)
    unionBatchExec.resultPartitionCount = BatchExecResourceUtil.
        getOperatorDefaultParallelism(tConfig)
  }

  def calculate(joinBatchExec: BatchExecJoinBase): Unit = {
    if (joinBatchExec.resultPartitionCount > 0) return
    visitChildren(joinBatchExec)
    val rightResultPartitionCount =
      joinBatchExec.getRight.asInstanceOf[RowBatchExecRel].resultPartitionCount
    val leftResultPartitionCount =
      joinBatchExec.getLeft.asInstanceOf[RowBatchExecRel].resultPartitionCount

    joinBatchExec.resultPartitionCount = joinBatchExec.getLeft match {
      case exchange: BatchExecExchange => {
        exchange.distribution.getType match {
          case RelDistribution.Type.RANDOM_DISTRIBUTED => leftResultPartitionCount
          case _ => rightResultPartitionCount
        }
      }
      case _: BatchExecUnion => rightResultPartitionCount
      case _ => leftResultPartitionCount
    }
  }

  def calculate(exchangeBatchExec: BatchExecExchange): Unit = {
    if (exchangeBatchExec.resultPartitionCount > 0) return

    visitChildren(exchangeBatchExec)
    exchangeBatchExec.resultPartitionCount = exchangeBatchExec.distribution.getType match {
      case RelDistribution.Type.SINGLETON => 1
      case _ => BatchExecResourceUtil.getOperatorDefaultParallelism(tConfig)
    }
  }

  def calculate(singleRel: SingleRel): Unit = {
    Preconditions.checkArgument(singleRel.isInstanceOf[BatchExecRel[_]])
    val batchExecRel = singleRel.asInstanceOf[RowBatchExecRel]
    if (batchExecRel.resultPartitionCount > 0) return
    visitChildren(batchExecRel)
    batchExecRel.resultPartitionCount = batchExecRel.asInstanceOf[SingleRel]
        .getInput.asInstanceOf[BatchExecRel[_]].resultPartitionCount
  }

  def calculate(valuesBatchExec: BatchExecValues): Unit = {
    valuesBatchExec.resultPartitionCount = 1
  }

  def visitChildren(rowBatchExec: RowBatchExecRel): Unit = {
    import _root_.scala.collection.JavaConversions._
    rowBatchExec.getInputs.foreach((child: RelNode) =>
      child.asInstanceOf[RowBatchExecRel].accept(this))
  }

  override def visit(dataStreamScan: BatchExecBoundedDataStreamScan): Unit =
    calculate(dataStreamScan)

  override def visit(scanTableSource: BatchExecTableSourceScan): Unit = calculate(scanTableSource)

  override def visit(values: BatchExecValues): Unit = calculate(values)

  override def visit(union: BatchExecUnion): Unit = calculate(union)

  override def visit(calc: BatchExecCalc): Unit = calculate(calc)

  override def visit(correlate: BatchExecCorrelate): Unit = calculate(correlate)

  override def visit(exchange: BatchExecExchange): Unit = calculate(exchange)

  override def visit(reused: BatchExecReused): Unit = calculate(reused)

  override def visit(expand: BatchExecExpand): Unit = calculate(expand)

  override def visit(hashAggregate: BatchExecHashAggregate): Unit = calculate(hashAggregate)

  override def visit(hashAggregate: BatchExecHashWindowAggregate): Unit = calculate(hashAggregate)

  override def visit(hashJoin: BatchExecHashJoinBase): Unit = calculate(hashJoin)

  override def visit(limit: BatchExecLimit): Unit = calculate(limit)

  override def visit(localHashAggregate: BatchExecLocalHashWindowAggregate): Unit =
    calculate(localHashAggregate)

  override def visit(localHashAggregate: BatchExecLocalHashAggregate): Unit =
    calculate(localHashAggregate)

  override def visit(localSortAggregate: BatchExecLocalSortWindowAggregate): Unit =
    calculate(localSortAggregate)

  override def visit(localSortAggregate: BatchExecLocalSortAggregate): Unit =
    calculate(localSortAggregate)

  override def visit(nestedLoopJoin: BatchExecNestedLoopJoinBase): Unit = calculate(nestedLoopJoin)

  override def visit(overWindowAgg: BatchExecOverAggregate): Unit = calculate(overWindowAgg)

  override def visit(sortAggregate: BatchExecSortAggregate): Unit = calculate(sortAggregate)

  override def visit(sortAggregate: BatchExecSortWindowAggregate): Unit = calculate(sortAggregate)

  override def visit(sort: BatchExecSort): Unit = calculate(sort)

  override def visit(sortLimit: BatchExecSortLimit): Unit = calculate(sortLimit)

  override def visit(sortMergeJoin: BatchExecSortMergeJoinBase): Unit = calculate(sortMergeJoin)

  override def visit(joinTable: BatchExecJoinTable): Unit = calculate(joinTable)

  override def visit(rank: BatchExecRank): Unit = {
    calculate(rank)
  }

  override def visit(batchExec: BatchExecRel[_]): Unit = {
    throw new TableException(s"could not reach here: ${batchExec.getClass}")
  }
}
