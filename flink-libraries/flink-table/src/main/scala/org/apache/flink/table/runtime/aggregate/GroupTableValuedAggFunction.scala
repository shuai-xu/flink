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

import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.GeneratedTableValuedAggHandleFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.ProcessFunctionBase.Context
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.types.DataType
import org.apache.flink.table.util.BaseRowUtil.isAccumulateMsg
import org.apache.flink.util.Collector

/**
  * Class of deterministic table-valued Aggregate Function
  * used for the groupby (without window) aggregate
  */
class GroupTableValuedAggFunction(
    genAggsHandler: GeneratedTableValuedAggHandleFunction,
    accTypes: DataType,
    generateRetraction: Boolean,
    groupWithoutKey: Boolean,
    queryConfig: StreamQueryConfig)
  extends GroupTableValuedAggFunctionBase(
    genAggsHandler,
    accTypes,
    groupWithoutKey: Boolean,
    queryConfig) {

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    LOG.debug(s"Compiling GroupTableValuedAggFunction: ${genAggsHandler.name} \n\n " +
                s"Code:\n${genAggsHandler.code}")
  }

  override def processElement(
    input: BaseRow,
    ctx: Context,
    out: Collector[BaseRow]): Unit = {
    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    val currentKey = executionContext.currentKey()

    // init accumulators before accumualte.
    preAccumulate(function, accState, currentKey, generateRetraction, appendCollector, out)

    var count = getCounter(currentKey, groupedDataCounter).getLong(0)
    // update aggregate result and set to the newRow
    if (isAccumulateMsg(input)) {
      // accumulate input
      function.accumulate(input)
      count += 1
    } else {
      // retract input
      function.retract(input)
      if (count > 0) {
        count -= 1
      }
    }

    // update state and emit results after accumulate
    postAccumulate(function, accState, currentKey, groupedDataCounter, count, appendCollector, out)
  }
}
