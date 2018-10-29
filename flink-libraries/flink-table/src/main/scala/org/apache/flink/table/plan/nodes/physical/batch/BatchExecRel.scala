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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.codegen.SortCodeGenerator
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.plan.cost.BatchExecCost
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery.reuseOrCreate
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.runtime.sort.BinaryIndexedSortable
import org.apache.flink.table.typeutils.BinaryRowSerializer
import org.apache.flink.table.util.{FlinkRelOptUtil, Logging}

import scala.collection.JavaConversions._

trait BatchExecRel[T] extends FlinkPhysicalRel with Logging {

  /**
    * Defines an unique reuse id for reused node and
    * this value will be referenced by [[BatchExecReused]] when explains plan.
    */
  private var reuseId: Option[Int] = None

  private var reusedTransformation: Option[StreamTransformation[T]] = None

  /**
    * Generates a reuse id if it does not exist.
    */
  def genReuseId(): Unit = {
    if (reuseId.isEmpty) {
      reuseId = Some(BatchExecRel.reuseIdCounter.incrementAndGet())
    }
  }

  /**
    * Returns reuse id if it exists, otherwise -1.
    */
  def getReuseId: Int = reuseId.getOrElse(-1)

  /**
    * Returns true if this node is reused, else false.
    */
  def isReused: Boolean = reuseId.isDefined

  /**
    * Returns true if this node is a barrier node, else false.
    */
  def isBarrierNode: Boolean = false

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  protected def translateToPlanInternal(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[T]

  /**
    * Translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  def translateToPlan(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[T] = {
    if (!isReused) {
      // the `reusedTransformation` of non-reused node is always None
      require(reusedTransformation.isEmpty)
    }
    reusedTransformation match {
      case Some(transformation) if isReused => transformation
      case _ =>
        val transformation = translateToPlanInternal(tableEnv, queryConfig)
        if (isReused) {
          reusedTransformation = Some(transformation)
        }
        val config = FlinkRelOptUtil.getTableConfig(this)
        if (config.getOperatorMetricCollect) {
          val nameWithId = s"${transformation.getName}, __id__=[$getId]"
          transformation.setName(nameWithId)
        }
        transformation
    }
  }

  /**
    * Accepts a visit from a [[BatchExecRelVisitor]].
    *
    * @param visitor BatchExecRelVisitor
    * @tparam R Return type
    */
  def accept[R](visitor: BatchExecRelVisitor[R]): R

  /**
    * Try to satisfy required traits by descendant of current node. If descendant can satisfy
    * required traits, and current node will not destroy it, then returns the new node with
    * converted inputs.
    *
    * @param requiredTraitSet required traits
    * @return A converted node which satisfy required traits by inputs node of current node.
    *         Returns null if required traits cannot be pushed down into inputs.
    */
  def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = null

  /**
    * Supplement reuseId of this batchExecRel to the copied RowBatchExecRel.
    * @param copiedRelNode copied RowBatchExecRel
    */
  protected final def supplement[P <: BatchExecRel[T]](copiedRelNode: P): P = {
    copiedRelNode.reuseId = reuseId
    copiedRelNode
  }
}

trait RowBatchExecRel extends BatchExecRel[BaseRow]


object BatchExecRel {
  private val reuseIdCounter = new AtomicInteger(0)

  //we aim for a 200% utilization of the bucket table.
  val HASH_COLLISION_WEIGHT = 2

  private[flink] def resetReuseIdCounter(): Unit = reuseIdCounter.set(0)

  private[flink] def getBatchExecMemCost(relNode: BatchExecRel[_]): Double = {
    val mq = reuseOrCreate(relNode.getCluster.getMetadataQuery)
    val relCost = mq.getNonCumulativeCost(relNode)
    relCost match {
      case execCost: BatchExecCost => execCost.memory
      case _ => 0d
    }
  }

  private[flink] def calcNeedMemoryForSort(mq: RelMetadataQuery, input: RelNode): Double = {
    //TODO It's hard to make sure that the normailized key's length is accurate in optimized stage.
    val normalizedKeyBytes = SortCodeGenerator.MAX_NORMALIZED_KEY_LEN
    val rowCount = mq.getRowCount(input)
    val averageRowSize = binaryRowAverageSize(input)
    val recordAreaInBytes = rowCount * (averageRowSize + BinaryRowSerializer.LENGTH_SIZE_IN_BYTES)
    val indexAreaInBytes = rowCount * (normalizedKeyBytes + BinaryIndexedSortable.OFFSET_LEN)
    recordAreaInBytes + indexAreaInBytes
  }

  private[flink] def binaryRowAverageSize(rel: RelNode): Double = {
    val binaryType = FlinkTypeFactory.toInternalBaseRowType(rel.getRowType, classOf[BinaryRow])
    val mq = reuseOrCreate(rel.getCluster.getMetadataQuery)
    val columnSizes = mq.getAverageColumnSizes(rel)
    var length = 0d
    columnSizes.zip(binaryType.getFieldTypes).foreach { case (columnSize, internal) =>
      if (BinaryRow.isFixedLength(internal)) {
        length += 8
      } else {
        if (columnSize == null) {
          // find a better way of computing generic type field variable-length
          // right now we use a small value assumption
          length += 16
        } else {
          // the 8 bytes is used store the length and offset of variable-length part.
          length += columnSize + 8
        }
      }
    }
    length += BinaryRow.calculateBitSetWidthInBytes(columnSizes.size())
    length
  }
}
