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

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.{KeyedState, KeyedValueState}
import org.apache.flink.streaming.api.TimeDomain
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.CoProcessFunction
import org.apache.flink.table.runtime.functions.ProcessFunctionBase.{Context, OnTimerContext}

/**
  * Used to clean up state.
  */
abstract class CoProcessFunctionWithCleanupState[IN1, IN2, OUT](queryConfig: StreamQueryConfig)
  extends CoProcessFunction[IN1, IN2, OUT]
  with CleanupState {

  protected val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  protected val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  protected val stateCleaningEnabled: Boolean = minRetentionTime > 1

  // holds the latest registered cleanup timer
  private var cleanupTimeState: KeyedValueState[BaseRow, JLong] = _

  protected def initCleanupTimeState(stateName: String) {
    if (stateCleaningEnabled) {
      val inputCntDescriptor: ValueStateDescriptor[JLong] =
        new ValueStateDescriptor[JLong](stateName, Types.LONG)
      cleanupTimeState = executionContext.getKeyedValueState(inputCntDescriptor)
    }
  }

  protected def registerProcessingCleanupTimer(ctx: Context, currentTime: Long): Unit = {
    registerProcessingCleanupTimer(
      ctx.timerService(),
      currentTime,
      stateCleaningEnabled,
      minRetentionTime,
      maxRetentionTime,
      cleanupTimeState,
      executionContext)
  }

  protected def isProcessingTimeTimer(ctx: OnTimerContext): Boolean = {
    ctx.timeDomain() == TimeDomain.PROCESSING_TIME
  }

  protected def needToCleanupState(timestamp: Long): Boolean = {
    needToCleanupState(timestamp, stateCleaningEnabled, cleanupTimeState, executionContext)
  }

  protected def cleanupState(states: KeyedState[BaseRow, _]*): Unit = {
    cleanupState(cleanupTimeState, executionContext, states: _*)
  }
}
