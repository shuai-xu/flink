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

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.SortCodeGenerator
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.cost.FlinkBatchCost
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.runtime.sort.BinaryIndexedSortable
import org.apache.flink.table.typeutils.BinaryRowSerializer
import org.apache.flink.table.util.Logging

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.lang.{Double => JDouble}
import java.util

import scala.collection.JavaConversions._

trait BatchExecRel[T] extends FlinkPhysicalRel with BatchExecNode[T] with Logging {

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

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[BatchExecRel[_]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    require(ordinalInParent >= 0 && ordinalInParent < getInputs.size())
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[BatchExecRel[_]])
  }

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def getEstimatedRowCount: JDouble = getCluster.getMetadataQuery.getRowCount(this)

  override def getEstimatedTotalMem: JDouble = {
    val relCost = getCluster.getMetadataQuery.getNonCumulativeCost(this)
    relCost match {
      case execCost: FlinkBatchCost => execCost.memory
      case _ => 0d
    }
  }

  override def getEstimatedAverageRowSize: JDouble =
    getCluster.getMetadataQuery.getAverageRowSize(this)
}

trait RowBatchExecRel extends BatchExecRel[BaseRow]

object BatchExecRel {

  //we aim for a 200% utilization of the bucket table.
  val HASH_COLLISION_WEIGHT = 2

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
    val binaryType = FlinkTypeFactory.toInternalRowType(rel.getRowType)
    val mq = FlinkRelMetadataQuery.reuseOrCreate(rel.getCluster.getMetadataQuery)
    val columnSizes = mq.getAverageColumnSizes(rel)
    var length = 0d
    columnSizes.zip(binaryType.getFieldTypes.map(_.toInternalType)).foreach {
      case (columnSize, internal) =>
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
