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

import java.util
import java.util.{List => JList}

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.collector.HeaderCollector
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.typeutils.TypeCheckUtils.validateEqualsHashCode
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * A CoProcessFunction to support stream join stream, currently just support inner-join
  *
  * @param leftLowerBound
  *        the left stream lower bound, and -leftLowerBound is the right stream upper bound
  * @param leftUpperBound
  *        the left stream upper bound, and -leftUpperBound is the right stream lower bound
  * @param element1Type  the input type of left stream
  * @param element2Type  the input type of right stream
  * @param genJoinFuncName    the function code of other non-equi condition
  * @param genJoinFuncCode    the function name of other non-equi condition
  *
  */
class ProcTimeWindowInnerJoin(
    private val leftLowerBound: Long,
    private val leftUpperBound: Long,
    private val element1Type: BaseRowTypeInfo[BaseRow],
    private val element2Type: BaseRowTypeInfo[BaseRow],
    private val genJoinFuncName: String,
    private val genJoinFuncCode: String)
  extends CoProcessFunction[BaseRow, BaseRow, JoinedRow]
    with Compiler[FlatJoinFunction[BaseRow, BaseRow, JoinedRow]]
    with Logging {

  // check if input types implement proper equals/hashCode
  validateEqualsHashCode("join", element1Type)
  validateEqualsHashCode("join", element2Type)

  private var baseRowCollector: HeaderCollector[JoinedRow] = _

  // other condition function
  private var joinFunction: FlatJoinFunction[BaseRow, BaseRow, JoinedRow] = _

  // tmp list to store expired records
  private var removeList: JList[Long] = _

  // state to hold left stream element
  private var row1MapState: MapState[Long, JList[BaseRow]] = _
  // state to hold right stream element
  private var row2MapState: MapState[Long, JList[BaseRow]] = _

  // state to record last timer of left stream, 0 means no timer
  private var timerState1: ValueState[Long] = _
  // state to record last timer of right stream, 0 means no timer
  private var timerState2: ValueState[Long] = _

  // compute window sizes, i.e., how long to keep rows in state.
  // window size of -1 means rows do not need to be put into state.
  private val leftStreamWinSize: Long = if (leftLowerBound <= 0) -leftLowerBound else -1
  private val rightStreamWinSize: Long = if (leftUpperBound >= 0) leftUpperBound else -1

  override def open(config: Configuration) {
    LOG.debug(s"Compiling JoinFunction: $genJoinFuncName \n\n " +
      s"Code:\n$genJoinFuncCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)
    LOG.debug("Instantiating JoinFunction.")
    joinFunction = clazz.newInstance()

    removeList = new util.ArrayList[Long]()
    baseRowCollector = new HeaderCollector[JoinedRow]()
    baseRowCollector.setAccumulate()

    // initialize row state
    val rowListTypeInfo1: ListTypeInfo[BaseRow] = new ListTypeInfo[BaseRow](element1Type)
    val mapStateDescriptor1: MapStateDescriptor[Long, JList[BaseRow]] =
      new MapStateDescriptor[Long, JList[BaseRow]]("row1mapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo1)
    row1MapState = getRuntimeContext.getMapState(mapStateDescriptor1)

    val rowListTypeInfo2: ListTypeInfo[BaseRow] = new ListTypeInfo[BaseRow](element2Type)
    val mapStateDescriptor2: MapStateDescriptor[Long, JList[BaseRow]] =
      new MapStateDescriptor[Long, JList[BaseRow]]("row2mapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo2)
    row2MapState = getRuntimeContext.getMapState(mapStateDescriptor2)

    // initialize timer state
    val valueStateDescriptor1: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timervaluestate1", classOf[Long])
    timerState1 = getRuntimeContext.getState(valueStateDescriptor1)

    val valueStateDescriptor2: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timervaluestate2", classOf[Long])
    timerState2 = getRuntimeContext.getState(valueStateDescriptor2)
  }

  /**
    * Process left stream records
    *
    * @param row The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement1(
      row: BaseRow,
      ctx: CoProcessFunction[BaseRow, BaseRow, JoinedRow]#Context,
      out: Collector[JoinedRow]): Unit = {

    processElement(
      row,
      ctx,
      out,
      leftStreamWinSize,
      timerState1,
      row1MapState,
      row2MapState,
      -leftUpperBound, // right stream lower
      -leftLowerBound, // right stream upper
      isLeft = true
    )
  }

  /**
    * Process right stream records
    *
    * @param row The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement2(
      row: BaseRow,
      ctx: CoProcessFunction[BaseRow, BaseRow, JoinedRow]#Context,
      out: Collector[JoinedRow]): Unit = {

    processElement(
      row,
      ctx,
      out,
      rightStreamWinSize,
      timerState2,
      row2MapState,
      row1MapState,
      leftLowerBound, // left stream lower
      leftUpperBound, // left stream upper
      isLeft = false
    )
  }

  /**
    * Called when a processing timer trigger.
    * Expire left/right records which earlier than current time - windowsize.
    *
    * @param timestamp The timestamp of the firing timer.
    * @param ctx       The ctx to register timer or get current time
    * @param out       The collector for returning result values.
    */
  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[BaseRow, BaseRow, JoinedRow]#OnTimerContext,
      out: Collector[JoinedRow]): Unit = {

    if (timerState1.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        leftStreamWinSize,
        row1MapState,
        timerState1,
        ctx
      )
    }

    if (timerState2.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        rightStreamWinSize,
        row2MapState,
        timerState2,
        ctx
      )
    }
  }

  /**
    * Puts an element from the input stream into state and search the other state to
    * output records meet the condition, and registers a timer for the current record
    * if there is no timer at present.
    */
  private def processElement(
      row: BaseRow,
      ctx: CoProcessFunction[BaseRow, BaseRow, JoinedRow]#Context,
      out: Collector[JoinedRow],
      winSize: Long,
      timerState: ValueState[Long],
      rowMapState: MapState[Long, JList[BaseRow]],
      otherRowMapState: MapState[Long, JList[BaseRow]],
      otherLowerBound: Long,
      otherUpperBound: Long,
      isLeft: Boolean): Unit = {

    if (LOG.isDebugEnabled) {
      LOG.debug(s"Input Row is: ${row.toString}")
    }
    baseRowCollector.out = out

    val curProcessTime = ctx.timerService.currentProcessingTime
    val otherLowerTime = curProcessTime + otherLowerBound
    val otherUpperTime = curProcessTime + otherUpperBound

    if (winSize >= 0) {
      // put row into state for later joining.
      // (winSize == 0) joins rows received in the same millisecond.
      var rowList = rowMapState.get(curProcessTime)
      if (rowList == null) {
        rowList = new util.ArrayList[BaseRow]()
      }
      rowList.add(row)
      rowMapState.put(curProcessTime, rowList)

      // register a timer to remove the row from state once it is expired
      if (timerState.value == 0) {
        val cleanupTime = curProcessTime + winSize + 1
        ctx.timerService.registerProcessingTimeTimer(cleanupTime)
        timerState.update(cleanupTime)
      }
    }

    // join row with rows received from the other input
    val otherTimeIter = otherRowMapState.keys().iterator()
    if (isLeft) {
      // go over all timestamps in the other input's state
      while (otherTimeIter.hasNext) {
        val otherTimestamp = otherTimeIter.next()
        if (otherTimestamp < otherLowerTime) {
          // other timestamp is expired. Remove it later.
          removeList.add(otherTimestamp)
        } else if (otherTimestamp <= otherUpperTime) {
          // join row with all rows from the other input for this timestamp
          val otherRows = otherRowMapState.get(otherTimestamp)
          var i = 0
          while (i < otherRows.size) {
            joinFunction.join(row, otherRows.get(i), baseRowCollector)
            i += 1
          }
        }
      }
    } else {
      // go over all timestamps in the other input's state
      while (otherTimeIter.hasNext) {
        val otherTimestamp = otherTimeIter.next()
        if (otherTimestamp < otherLowerTime) {
          // other timestamp is expired. Remove it later.
          removeList.add(otherTimestamp)
        } else if (otherTimestamp <= otherUpperTime) {
          // join row with all rows from the other input for this timestamp
          val otherRows = otherRowMapState.get(otherTimestamp)
          var i = 0
          while (i < otherRows.size) {
            joinFunction.join(otherRows.get(i), row, baseRowCollector)
            i += 1
          }
        }
      }
    }

    // remove rows for expired timestamps
    var i = removeList.size - 1
    while (i >= 0) {
      otherRowMapState.remove(removeList.get(i))
      i -= 1
    }
    removeList.clear()
  }

  /**
    * Removes records which are outside the join window from the state.
    * Registers a new timer if the state still holds records after the clean-up.
    */
  private def expireOutTimeRow(
      curTime: Long,
      winSize: Long,
      rowMapState: MapState[Long, JList[BaseRow]],
      timerState: ValueState[Long],
      ctx: CoProcessFunction[BaseRow, BaseRow, JoinedRow]#OnTimerContext): Unit = {

    val expiredTime = curTime - winSize
    val keyIter = rowMapState.keys().iterator()
    var validTimestamp: Boolean = false
    // Search for expired timestamps.
    // If we find a non-expired timestamp, remember the timestamp and leave the loop.
    // This way we find all expired timestamps if they are sorted without doing a full pass.
    while (keyIter.hasNext && !validTimestamp) {
      val recordTime = keyIter.next
      if (recordTime < expiredTime) {
        removeList.add(recordTime)
      } else {
        // we found a timestamp that is still valid
        validTimestamp = true
      }
    }

    // If the state has non-expired timestamps, register a new timer.
    // Otherwise clean the complete state for this input.
    if (validTimestamp) {

      // Remove expired records from state
      var i = removeList.size - 1
      while (i >= 0) {
        rowMapState.remove(removeList.get(i))
        i -= 1
      }
      removeList.clear()

      val cleanupTime = curTime + winSize + 1
      ctx.timerService.registerProcessingTimeTimer(cleanupTime)
      timerState.update(cleanupTime)
    } else {
      timerState.clear()
      rowMapState.clear()
    }
  }
}
