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

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.SortCodeGenerator
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.cost.FlinkBatchCost
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.runtime.sort.BinaryIndexedSortable
import org.apache.flink.table.typeutils.BinaryRowSerializer
import org.apache.flink.table.util.{BatchExecRelVisitor, Logging}

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

import scala.collection.JavaConversions._

trait BatchExecRel[T] extends FlinkPhysicalRel with Logging {

  private var reusedTransformation: Option[StreamTransformation[T]] = None

  /**
    * Returns true if this node is a barrier node, else false.
    */
  def isBarrierNode: Boolean = false

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  protected def translateToPlanInternal(tableEnv: BatchTableEnvironment): StreamTransformation[T]

  /**
    * Translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  def translateToPlan(tableEnv: BatchTableEnvironment): StreamTransformation[T] = {
    reusedTransformation match {
      case Some(transformation) => transformation
      case _ =>
        val transformation = translateToPlanInternal(tableEnv)
        reusedTransformation = Some(transformation)
        val config = FlinkRelOptUtil.getTableConfig(this)
        if (config.getConf.getBoolean(
          TableConfigOptions.SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED)) {
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
}

trait RowBatchExecRel extends BatchExecRel[BaseRow]


object BatchExecRel {

  //we aim for a 200% utilization of the bucket table.
  val HASH_COLLISION_WEIGHT = 2

  private[flink] def getBatchExecMemCost(relNode: BatchExecRel[_]): Double = {
    val mq = FlinkRelMetadataQuery.reuseOrCreate(relNode.getCluster.getMetadataQuery)
    val relCost = mq.getNonCumulativeCost(relNode)
    relCost match {
      case execCost: FlinkBatchCost => execCost.memory
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
    val mq = FlinkRelMetadataQuery.reuseOrCreate(rel.getCluster.getMetadataQuery)
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
