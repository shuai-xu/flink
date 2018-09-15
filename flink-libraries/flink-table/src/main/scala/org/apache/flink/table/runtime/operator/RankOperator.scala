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

package org.apache.flink.table.runtime.operator

import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedSorter}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.runtime.sort.RecordComparator

class RankOperator(
    partitionByGeneratedSorter: GeneratedSorter,
    orderByGeneratedSorter: GeneratedSorter,
    rankStart: Long,
    rankEnd: Long,
    outputRankFunColumn: Boolean)
  extends AbstractStreamOperatorWithMetrics[BaseRow]
  with OneInputStreamOperator[BaseRow, BaseRow] {

  private var partitionBySorter: RecordComparator = _

  private var orderBySorter: RecordComparator = _

  // only supports RANK function now
  private var rank: Long = 0L

  /** row stores rank column */
  private var rankValueRow: GenericRow = _

  /** row stores element row and rank value row */
  private var joinedRow: JoinedRow = _

  private var lastInput: BaseRow = _

  private var collector: StreamRecordCollector[BaseRow] = _

  override def open(): Unit = {
    super.open()

    partitionBySorter = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      partitionByGeneratedSorter.comparator.name,
      partitionByGeneratedSorter.comparator.code).newInstance.asInstanceOf[RecordComparator]
    partitionBySorter.init(
      partitionByGeneratedSorter.serializers, partitionByGeneratedSorter.comparators)

    orderBySorter = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      orderByGeneratedSorter.comparator.name,
      orderByGeneratedSorter.comparator.code).newInstance.asInstanceOf[RecordComparator]
    orderBySorter.init(
      partitionByGeneratedSorter.serializers, partitionByGeneratedSorter.comparators)

    if (outputRankFunColumn) {
      joinedRow = new JoinedRow()
      rankValueRow = new GenericRow(1)
    }

    collector = new StreamRecordCollector[BaseRow](output)
  }

  override def processElement(element: StreamRecord[BaseRow]): Unit = {
    val input = element.getValue
    if (lastInput == null || partitionBySorter.compare(lastInput, input) != 0) {
      rank = 1L // reset rank value for new group
    } else if (orderBySorter.compare(lastInput, input) != 0) {
      rank += 1 // add 1 if order-by value is change in a group
    }
    emitInternal(input)
    lastInput = input.copy()
  }

  private def emitInternal(element: BaseRow): Unit = {
    if (rank >= rankStart && rank <= rankEnd) {
      if (outputRankFunColumn) {
        rankValueRow.setLong(0, rank)
        collector.collect(joinedRow.replace(element, rankValueRow))
      } else {
        collector.collect(element)
      }
    }
  }
}
