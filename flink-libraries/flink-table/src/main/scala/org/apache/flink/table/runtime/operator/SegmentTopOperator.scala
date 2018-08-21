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
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.sort.RecordComparator

/**
  * Operator for segment-top, this operator does not cache any data.
  * TODO: codeGen all this operator and compare the fields needed without record copy.
  */
class SegmentTopOperator(
    groupingGeneratedSorter: GeneratedSorter,
    withTiesGeneratedSorter: GeneratedSorter)
  extends AbstractStreamOperatorWithMetrics[BaseRow]
  with OneInputStreamOperator[BaseRow, BaseRow] {

  private var groupingSorter: RecordComparator = _

  private var withTiesSorter: RecordComparator = _

  private var lastInput: BaseRow = _

  private var lastEmitted: BaseRow = _

  private var collector: StreamRecordCollector[BaseRow] = _

  override def open(): Unit = {
    super.open()

    groupingSorter = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      groupingGeneratedSorter.comparator.name,
      groupingGeneratedSorter.comparator.code).newInstance.asInstanceOf[RecordComparator]
    groupingSorter.init(groupingGeneratedSorter.serializers, groupingGeneratedSorter.comparators)

    withTiesSorter = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      withTiesGeneratedSorter.comparator.name,
      withTiesGeneratedSorter.comparator.code).newInstance.asInstanceOf[RecordComparator]
    withTiesSorter.init(groupingGeneratedSorter.serializers, groupingGeneratedSorter.comparators)

    collector = new StreamRecordCollector[BaseRow](output)
  }

  override def processElement(element: StreamRecord[BaseRow]): Unit = {
    val input = element.getValue
    if (lastInput == null || groupingSorter.compare(lastInput, input) != 0) {
      emitInternal(input)
    } else if (withTiesSorter.compare(lastEmitted, input) == 0) {
      emitInternal(input)
    }
    lastInput = input.copy()
  }

  private def emitInternal(element: BaseRow): Unit = {
    lastEmitted = element.copy()
    collector.collect(element)
  }
}
