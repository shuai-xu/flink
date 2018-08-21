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

package org.apache.flink.table.runtime.operator.sort

import java.util

import org.apache.flink.api.common.state2.ListState
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.runtime.operators.sort.{IndexedSorter, QuickSort}
import org.apache.flink.runtime.state.StateSnapshotContext
import org.apache.flink.runtime.state2.partitioned.PartitionedListStateDescriptor
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.TableException
import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedSorter}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.operator.StreamRecordCollector
import org.apache.flink.table.runtime.sort.{BinaryInMemorySortBuffer, NormalizedKeyComputer, RecordComparator}
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowSerializer, BaseRowTypeInfo, BinaryRowSerializer, TypeUtils}
import org.apache.flink.table.util.{BaseRowUtil, Logging}

import scala.collection.mutable

class StreamSortOperator(
     inputRowType: BaseRowTypeInfo[BaseRow],
     gSorter: GeneratedSorter,
     memorySize: Double)
  extends AbstractStreamOperator[BaseRow]
  with OneInputStreamOperator[BaseRow, BaseRow]
  with Logging {

  /** The map store elements. **/
  @transient private var inputBuffer: mutable.HashMap[BaseRow, Int] = _

  @transient private var sortBuffer: BinaryInMemorySortBuffer = _

  @transient private var sorter: IndexedSorter = _

  @transient private var binarySerializer: BinaryRowSerializer = _

  @transient private var baseRowSerializer: BaseRowSerializer[BaseRow] = _

  /** Output for stream records. **/
  @transient private var collector: StreamRecordCollector[BaseRow] = _

  /** The state to store buffer to make it exactly once. **/
  @transient private var bufferState: ListState[BaseRow] = _

  protected def getComparator(gSorter: GeneratedSorter): RecordComparator = {
    val name = gSorter.comparator.name
    val code = gSorter.comparator.code
    LOG.debug(s"Compiling Comparator: $name \n\n Code:\n$code")
    val clazz = CodeGenUtils.compile(
      getRuntimeContext.getUserCodeClassLoader, name, code
    ).asInstanceOf[Class[RecordComparator]]
    val comparator = clazz.newInstance()
    comparator.init(gSorter.serializers, gSorter.comparators)
    comparator
  }

  protected def getComputer(gSorter: GeneratedSorter): NormalizedKeyComputer = {
    val name = gSorter.computer.name
    val code = gSorter.computer.code
    LOG.debug(s"Compiling Computer: $name \n\n Code:\n$code")
    val clazz = CodeGenUtils.compile(
      getRuntimeContext.getUserCodeClassLoader, name, code
    ).asInstanceOf[Class[NormalizedKeyComputer]]
    val computor = clazz.newInstance()
    computor.init(gSorter.serializers, gSorter.comparators)
    computor
  }

  override def open() {
    super.open()

    val recordSerializer = TypeUtils.createSerializer(
      inputRowType).asInstanceOf[TypeSerializer[BaseRow]]

    binarySerializer = new BinaryRowSerializer(
      recordSerializer.asInstanceOf[AbstractRowSerializer[_ <: BaseRow]].getTypes: _*)
    baseRowSerializer = new BaseRowSerializer(
      recordSerializer.asInstanceOf[AbstractRowSerializer[_ <: BaseRow]].getTypes: _*)
    val memManager = getContainingTask.getEnvironment.getMemoryManager
    val memorySegments = memManager
      .allocatePages(getContainingTask, (memorySize / memManager.getPageSize).toInt)
    val comparator = getComparator(gSorter)
    val computer = getComputer(gSorter)
    collector = new StreamRecordCollector[BaseRow](output)
    sortBuffer = BinaryInMemorySortBuffer.createBuffer(
      memManager,
      computer,
      recordSerializer,
      binarySerializer,
      comparator,
      memorySegments,
      0,
      0)
    sorter = new QuickSort()
    inputBuffer = mutable.HashMap()
    // create & restore state
    val stateDesc = new PartitionedListStateDescriptor[BaseRow]("localBufferState",
      inputRowType.createSerializer(getExecutionConfig))
    this.bufferState = getPartitionedState(stateDesc)
    // recover buffer from partition state
    recoverSortBuffer()
  }

  override def processElement(in: StreamRecord[BaseRow]): Unit = {
    val originalInput = in.getValue
    val input = baseRowSerializer.baseRowToBinary(originalInput).copy()
    BaseRowUtil.setAccumulate(input)
    val nowCount : Int = inputBuffer.getOrElse(input, 0)
    if (BaseRowUtil.isAccumulateMsg(originalInput)) {
      inputBuffer += ((input, nowCount + 1))
    } else {
      if (nowCount == 0) {
        throw TableException("BaseRow Not Exist")
      } else if (nowCount == 1) {
        inputBuffer -= input
      } else {
        inputBuffer += ((input, nowCount - 1))
      }
    }
  }

  override def endInput(): Unit = {
    if (inputBuffer.nonEmpty) {
      inputBuffer.keys.foreach { i =>
        val nowCount: Option[Int] = inputBuffer.get(i)
        (1 to nowCount.getOrElse(0)).foreach(_ => sortBuffer.write(i))
      }
      sorter.sort(sortBuffer)
      // emit the sorted inputs
      val outputItor = sortBuffer.getIterator
      var row = binarySerializer.createInstance()
      row = outputItor.next(row)
      while (row != null) {
        collector.collect(row)
        row = outputItor.next(row)
      }
    }
  }

  override def snapshotState(context: StateSnapshotContext): Unit = {
    super.snapshotState(context)
    // clear state first
    bufferState.clear()

    val stateToPut: util.List[BaseRow] = new util.ArrayList[BaseRow](inputBuffer.size)
    inputBuffer.keys.foreach{ i =>
      val nowCount: Option[Int] = inputBuffer.get(i)
      (1 to nowCount.getOrElse(0)).foreach(_ => stateToPut.add(i))
    }

    // batch put
    bufferState.addAll(stateToPut)
  }

  private def recoverSortBuffer(): Unit = {
    val iter = bufferState.iterator
    while (iter.hasNext) {
      val key = iter.next
      val nowCount : Int = inputBuffer.getOrElse(key, 0)
      inputBuffer += ((key, nowCount + 1))
    }
  }

  override def close(): Unit = {
    LOG.info("Closing StreamSortOperator")
    super.close()
  }

  override def requireState = true
}
