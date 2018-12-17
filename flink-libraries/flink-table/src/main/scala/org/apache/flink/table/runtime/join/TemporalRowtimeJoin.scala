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
package org.apache.flink.table.runtime.join

import java.lang.{Long => JLong}
import java.util
import java.util.{Comparator, Optional}
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.collector.HeaderCollector
import org.apache.flink.table.util.Logging

import scala.collection.JavaConversions._

/**
  * This operator works by keeping on the state collection of probe and build records to process
  * on next watermark. The idea is that between watermarks we are collecting those elements
  * and once we are sure that there will be no updates we emit the correct result and clean up the
  * state.
  *
  * Cleaning up the state drops all of the "old" values from the probe side, where "old" is defined
  * as older then the current watermark. Build side is also cleaned up in the similar fashion,
  * however we always keep at least one record - the latest one - even if it's past the last
  * watermark.
  *
  * One more trick is how the emitting results and cleaning up is triggered. It is achieved
  * by registering timers for the keys. We could register a timer for every probe and build
  * side element's event time (when watermark exceeds this timer, that's when we are emitting and/or
  * cleaning up the state). However this would cause huge number of registered timers. For example
  * with following evenTimes of probe records accumulated: {1, 2, 5, 8, 9}, if we
  * had received Watermark(10), it would trigger 5 separate timers for the same key. To avoid that
  * we always keep only one single registered timer for any given key, registered for the minimal
  * value. Upon triggering it, we process all records with event times older then or equal to
  * currentWatermark.
  */
class TemporalRowtimeJoin(
    leftType: TypeInformation[BaseRow],
    rightType: TypeInformation[BaseRow],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    leftTimeAttribute: Int,
    rightTimeAttribute: Int)
  extends AbstractStreamOperator[BaseRow]
  with TwoInputStreamOperator[BaseRow, BaseRow, BaseRow]
  with Triggerable[Any, VoidNamespace]
  with Compiler[FlatJoinFunction[BaseRow, BaseRow, BaseRow]]
  with Logging {

  private val NEXT_LEFT_INDEX_STATE_NAME = "next-index"
  private val LEFT_STATE_NAME = "left"
  private val RIGHT_STATE_NAME = "right"
  private val REGISTERED_TIMER_STATE_NAME = "timer"
  private val TIMERS_STATE_NAME = "timers"

  private val rightRowtimeComparator = new RowtimeComparator(rightTimeAttribute)

  /**
    * Incremental index generator for `leftState`'s keys.
    */
  private var nextLeftIndex: ValueState[JLong] = _

  /**
    * Mapping from artificial row index (generated by `nextLeftIndex`) into the left side `Row`.
    * We can not use List to accumulate Rows, because we need efficient deletes of the oldest rows.
    *
    * TODO: this could be OrderedMultiMap[Jlong, Row] indexed by row's timestamp, to avoid
    * full map traversals (if we have lots of rows on the state that exceed `currentWatermark`).
    */
  private var leftState: MapState[JLong, BaseRow] = _

  /**
    * Mapping from timestamp to right side `Row`.
    *
    * TODO: having `rightState` as an OrderedMapState would allow us to avoid sorting cost
    * once per watermark
    */
  private var rightState: MapState[JLong, BaseRow] = _

  private var registeredTimer: ValueState[JLong] = _ // JLong for correct handling of default null

  protected var headerCollector: HeaderCollector[BaseRow] = _
  private var collector: TimestampedCollector[BaseRow] = _
  private var timerService: InternalTimerService[VoidNamespace] = _

  private var joinFunction: FlatJoinFunction[BaseRow, BaseRow, BaseRow] = _

  override def open(): Unit = {
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)

    joinFunction = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(joinFunction, getRuntimeContext)
    FunctionUtils.openFunction(joinFunction, new Configuration)

    nextLeftIndex = getRuntimeContext.getState(
      new ValueStateDescriptor[JLong](
        NEXT_LEFT_INDEX_STATE_NAME, BasicTypeInfo.LONG_TYPE_INFO))
    leftState = getRuntimeContext.getMapState(
      new MapStateDescriptor[JLong, BaseRow](
        LEFT_STATE_NAME, BasicTypeInfo.LONG_TYPE_INFO, leftType))
    rightState = getRuntimeContext.getMapState(
      new MapStateDescriptor[JLong, BaseRow](
        RIGHT_STATE_NAME, BasicTypeInfo.LONG_TYPE_INFO, rightType))
    registeredTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[JLong](
        REGISTERED_TIMER_STATE_NAME, BasicTypeInfo.LONG_TYPE_INFO))

    collector = new TimestampedCollector[BaseRow](output)
    headerCollector = new HeaderCollector[BaseRow]
    headerCollector.out = collector
    headerCollector.setHeader(BaseRowUtil.ACCUMULATE_MSG)

    timerService = getInternalTimerService(
      TIMERS_STATE_NAME,
      VoidNamespaceSerializer.INSTANCE,
      this)
  }

  override def processElement1(element: StreamRecord[BaseRow]): Unit = {
    val row = element.getValue
    checkNotRetraction(row)

    leftState.put(getNextLeftIndex, row)
    registerSmallestTimer(getLeftTime(row)) // Timer to emit and clean up the state
  }

  override def processElement2(element: StreamRecord[BaseRow]): Unit = {
    val row = element.getValue
    checkNotRetraction(row)

    val rowTime = getRightTime(row)
    rightState.put(rowTime, row)
    registerSmallestTimer(rowTime) // Timer to clean up the state
  }

  private def registerSmallestTimer(timestamp: Long): Unit = {
    val currentRegisteredTimer = registeredTimer.value()
    if (currentRegisteredTimer == null) {
      registerTimer(timestamp)
    }
    else if (currentRegisteredTimer != null && currentRegisteredTimer > timestamp) {
      timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, currentRegisteredTimer)
      registerTimer(timestamp)
    }
  }

  private def registerTimer(timestamp: Long): Unit = {
    registeredTimer.update(timestamp)
    timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp)
  }

  override def onProcessingTime(timer: InternalTimer[Any, VoidNamespace]): Unit = {
    throw new IllegalStateException("This should never happen")
  }

  override def onEventTime(timer: InternalTimer[Any, VoidNamespace]): Unit = {
    registeredTimer.clear()
    val lastUnprocessedTime = emitResultAndCleanUpState(timerService.currentWatermark())
    if (lastUnprocessedTime < Long.MaxValue) {
      registerTimer(lastUnprocessedTime)
    }
  }

  /**
    * @return a row time of the oldest unprocessed probe record or Long.MaxValue, if all records
    *         have been processed.
    */
  private def emitResultAndCleanUpState(timerTimestamp: Long): Long = {
    val rightRowsSorted = getRightRowsSorted(rightRowtimeComparator)
    var lastUnprocessedTime = Long.MaxValue

    val leftIterator = leftState.entries().iterator()
    while (leftIterator.hasNext) {
      val leftEntry = leftIterator.next()
      val leftRow = leftEntry.getValue
      val leftTime = getLeftTime(leftRow)

      if (leftTime <= timerTimestamp) {
        val rightRow = latestRightRowToJoin(rightRowsSorted, leftTime)
        if (rightRow.isPresent) {
          joinFunction.join(leftRow, rightRow.get, headerCollector)
        }
        leftIterator.remove()
      }
      else {
        lastUnprocessedTime = Math.min(lastUnprocessedTime, leftTime)
      }
    }

    cleanUpState(timerTimestamp, rightRowsSorted)
    lastUnprocessedTime
  }

  /**
    * Removes all right entries older then the watermark, except the latest one. For example with:
    * rightState = [1, 5, 9]
    * and
    * watermark = 6
    * we can not remove "5" from rightState, because left elements with rowtime of 7 or 8 could
    * be joined with it later
    */
  private def cleanUpState(timerTimestamp: Long, rightRowsSorted: util.List[BaseRow]) = {
    var i = 0
    val indexToKeep = firstIndexToKeep(timerTimestamp, rightRowsSorted)
    while (i < indexToKeep) {
      val rightTime = getRightTime(rightRowsSorted.get(i))
      rightState.remove(rightTime)
      i += 1
    }
  }

  private def firstIndexToKeep(timerTimestamp: Long, rightRowsSorted: util.List[BaseRow]): Int = {
    val firstIndexNewerThenTimer =
      indexOfFirstElementNewerThanTimer(timerTimestamp, rightRowsSorted)

    if (firstIndexNewerThenTimer < 0) {
      rightRowsSorted.size() - 1
    }
    else {
      firstIndexNewerThenTimer - 1
    }
  }

  private def indexOfFirstElementNewerThanTimer(
      timerTimestamp: Long,
      list: util.List[BaseRow]): Int = {
    val iter = list.listIterator
    while (iter.hasNext) {
      if (getRightTime(iter.next) > timerTimestamp) {
        return iter.previousIndex
      }
    }
    -1
  }

  /**
    * Binary search `rightRowsSorted` to find the latest right row to join with `leftTime`.
    * Latest means a right row with largest time that is still smaller or equal to `leftTime`.
    *
    * @return found element or `Optional.empty` If such row was not found (either `rightRowsSorted`
    *         is empty or all `rightRowsSorted` are are newer).
    */
  private def latestRightRowToJoin(
      rightRowsSorted: util.List[BaseRow],
      leftTime: Long): Optional[BaseRow] = {
    latestRightRowToJoin(rightRowsSorted, 0, rightRowsSorted.size - 1, leftTime)
  }

  private def latestRightRowToJoin(
      rightRowsSorted: util.List[BaseRow],
      low: Int,
      high: Int,
      leftTime: Long): Optional[BaseRow] = {
    if (low > high) {
      // exact value not found, we are returning largest from the values smaller then leftTime
      if (low - 1 < 0) {
        Optional.empty()
      }
      else {
        Optional.of(rightRowsSorted.get(low - 1))
      }
    } else {
      val mid = (low + high) >>> 1
      val midRow = rightRowsSorted.get(mid)
      val midTime = getRightTime(midRow)
      val cmp = midTime.compareTo(leftTime)
      if (cmp < 0) {
        latestRightRowToJoin(rightRowsSorted, mid + 1, high, leftTime)
      }
      else if (cmp > 0) {
        latestRightRowToJoin(rightRowsSorted, low, mid - 1, leftTime)
      }
      else {
        Optional.of(midRow)
      }
    }
  }

  private def getRightRowsSorted(rowtimeComparator: RowtimeComparator): util.List[BaseRow] = {
    val rightRows = new util.ArrayList[BaseRow]()
    for (row <- rightState.values()) {
      rightRows.add(row)
    }
    rightRows.sort(rowtimeComparator)
    rightRows.asInstanceOf[util.List[BaseRow]]
  }

  private def getNextLeftIndex: JLong = {
    var index = nextLeftIndex.value()
    if (index == null) {
      index = 0L
    }
    nextLeftIndex.update(index + 1)
    index
  }

  private def getLeftTime(leftRow: BaseRow): Long = {
    leftRow.getLong(leftTimeAttribute)
  }

  private def getRightTime(rightRow: BaseRow): Long = {
    rightRow.getLong(rightTimeAttribute)
  }

  private def checkNotRetraction(row: BaseRow) = {
    if (BaseRowUtil.isRetractMsg(row)) {
      throw new IllegalStateException(
        s"Retractions are not supported by [${classOf[TemporalRowtimeJoin].getSimpleName}]. " +
          "If this can happen it should be validated during planning!")
    }
  }
}

class RowtimeComparator(timeAttribute: Int) extends Comparator[BaseRow] with Serializable {
  override def compare(o1: BaseRow, o2: BaseRow): Int = {
    val o1Time = o1.getLong(timeAttribute)
    val o2Time = o2.getLong(timeAttribute)
    o1Time.compareTo(o2Time)
  }
}
