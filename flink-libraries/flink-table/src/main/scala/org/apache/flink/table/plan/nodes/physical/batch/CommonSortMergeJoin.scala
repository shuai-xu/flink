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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.codegen.{GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.runtime.aggregate.SortUtil
import org.apache.flink.table.runtime.sort.BinaryExternalSorter
import org.apache.flink.table.types.BaseRowType
import org.apache.flink.table.typeutils.TypeUtils

/**
  * Common logical for SortMerge Join.
  */
trait CommonSortMergeJoin {

  protected def newGeneratedSorter(originalKeys: Array[Int], t: BaseRowType): GeneratedSorter = {
    val originalOrders = originalKeys.map((_) => true)
    val (keys, orders, nullsIsLast) = SortUtil.deduplicationSortKeys(
      originalKeys,
      originalOrders,
      SortUtil.getNullDefaultOrders(originalOrders))

    val types = keys.map(t.getFieldTypes()(_))
    val compAndSers = types.zip(orders).map { case (internalType, order) =>
      (TypeUtils.createComparator(internalType, order), TypeUtils.createSerializer(internalType))
    }
    val comps = compAndSers.map(_._1)
    val sers = compAndSers.map(_._2)

    val gen = new SortCodeGenerator(keys, types, comps, orders, nullsIsLast)
    GeneratedSorter(
      gen.generateNormalizedKeyComputer("SMJComputer"),
      gen.generateRecordComparator("SMJComparator"),
      sers, comps)
  }

  protected def inferLeftRowCountRatio(left: RelNode, right: RelNode, relMq: RelMetadataQuery)
  : Double = {
    val mq = FlinkRelMetadataQuery.reuseOrCreate(relMq)
    val leftRowCnt = mq.getRowCount(left)
    val rightRowCnt = mq.getRowCount(right)
    if (leftRowCnt == null || rightRowCnt == null) {
      0.5d
    } else {
      leftRowCnt / (rightRowCnt + leftRowCnt)
    }
  }

  protected def calcSortMemory(ratio: Double, totalSortMemory: Long): Long = {
    val minGuaranteedMemory = BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM
    val maxGuaranteedMemory = totalSortMemory - BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM
    val inferLeftSortMemory = (totalSortMemory * ratio).toLong
    Math.max(Math.min(inferLeftSortMemory, maxGuaranteedMemory), minGuaranteedMemory)
  }
}
