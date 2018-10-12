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

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.keyed.KeyedListState
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.GeneratedTableValuedAggHandleFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.{ExecutionContext, ProcessFunction}
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes}
import org.apache.flink.table.util.BaseRowUtil
import org.apache.flink.table.util.BaseRowUtil.isAccumulateMsg
import org.apache.flink.util.Collector

/**
  * Class of non-deterministic table-valued Aggregate Function
  * used for the groupby (without window) aggregate
  *
  * @param genAggsHandler     the generated aggregate handler
  * @param accTypes           the accumulator types
  * @param generateRetraction whether this operator will generate retraction
  */
class NonDeterministicGroupTableValuedAggFunction(
    genAggsHandler: GeneratedTableValuedAggHandleFunction,
    accTypes: DataType,
    outputRowType: BaseRowType,
    generateRetraction: Boolean,
    groupWithoutKey: Boolean,
    queryConfig: StreamQueryConfig)
  extends GroupTableValuedAggFunctionBase(
    genAggsHandler,
    accTypes,
    groupWithoutKey: Boolean,
    queryConfig) {

  // stores last accumulators for retraction
  private var prevResults: KeyedListState[BaseRow, BaseRow] = _

  private var bufferedCollector: BufferedAppendGroupKeyCollector = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)

    val prevResultsDesc = new ListStateDescriptor(
      "prevResultDesc",
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[TypeInformation[BaseRow]])

    prevResults = ctx.getKeyedListState(prevResultsDesc)
    bufferedCollector = new BufferedAppendGroupKeyCollector(prevResults)

    LOG.debug(s"Compiling NonDeterministicGroupTableValuedAggFunction: " +
                s"${genAggsHandler.name} \n\n Code:\n${genAggsHandler.code}")
  }

  override def processElement(
    input: BaseRow,
    ctx: ProcessFunction.Context,
    out: Collector[BaseRow]): Unit = {
    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    val currentKey = executionContext.currentKey()

    var accumulators = accState.get(currentKey)
    if (null == accumulators) {
      firstRow = true
      accumulators = function.createAccumulators()
    } else {
      firstRow = false
    }

    var count = getCounter(currentKey).getLong(0)

    // set accumulators to handler first
    function.setAccumulators(accumulators)

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

    // if this was not the first row and we have to emit retractions
    if (!firstRow) {
      if (generateRetraction) {
        val it = prevResults.get(currentKey).iterator()
        while (it.hasNext) {
          var row = it.next()
          row = BaseRowUtil.setRetract(row)
          out.collect(row)
        }
      }
    }

    // clean prevResults state
    prevResults.remove(currentKey)

    val counter = getCounter(currentKey)
    counter.setLong(0, count)
    groupedDataCounter.put(currentKey, counter)

    if (count != 0) {

      // emit new result and update the
      bufferedCollector.reSet(out, currentKey)
      function.emitValue(bufferedCollector)

      // update the state
      accState.put(currentKey, function.getAccumulators)

    } else { //last data of current key has been retracted
      // clear all state
      accState.remove(currentKey)
      groupedDataCounter.remove(currentKey)
    }
  }
}
