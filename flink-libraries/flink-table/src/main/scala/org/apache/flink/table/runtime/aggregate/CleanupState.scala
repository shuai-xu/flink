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

package org.apache.flink.table.runtime.aggregate

import java.lang.{Long => JLong}

import org.apache.flink.runtime.state.keyed.{KeyedState, KeyedValueState}
import org.apache.flink.streaming.api.TimerService
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.runtime.functions.{CoProcessFunction, ProcessFunction}

/**
  * Base class for clean up state, both for [[ProcessFunction]] and [[CoProcessFunction]].
  */
trait CleanupState {

  def registerProcessingCleanupTimer(
    timeService: TimerService,
    currentTime: Long,
    stateCleaningEnabled: Boolean,
    minRetentionTime: Long,
    maxRetentionTime: Long,
    cleanupTimeState: KeyedValueState[BaseRow, JLong],
    executionContext: ExecutionContext): Unit = {

    if (stateCleaningEnabled) {

      val currentKey = executionContext.currentKey()

      // last registered timer
      val curCleanupTime = cleanupTimeState.get(currentKey)

      // check if a cleanup timer is registered and
      // that the current cleanup timer won't delete state we need to keep
      if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
        // we need to register a new (later) timer
        val cleanupTime = currentTime + maxRetentionTime
        // register timer and remember clean-up time
        timeService.registerProcessingTimeTimer(cleanupTime)
        cleanupTimeState.put(currentKey, cleanupTime)
      }
    }
  }

  def needToCleanupState(
    timestamp: Long,
    stateCleaningEnabled: Boolean,
    cleanupTimeState: KeyedValueState[BaseRow, JLong],
    executionContext: ExecutionContext): Boolean = {

    if (stateCleaningEnabled) {
      val cleanupTime = cleanupTimeState.get(executionContext.currentKey())
      // check that the triggered timer is the last registered processing time timer.
      null != cleanupTime && timestamp == cleanupTime
    } else {
      false
    }
  }

  def cleanupState(
    cleanupTimeState: KeyedValueState[BaseRow, JLong],
    executionContext: ExecutionContext,
    states: KeyedState[BaseRow, _]*): Unit = {

    val currentKey = executionContext.currentKey()
    // clear all state
    states.foreach(_.remove(currentKey))
    cleanupTimeState.remove(currentKey)
  }
}
