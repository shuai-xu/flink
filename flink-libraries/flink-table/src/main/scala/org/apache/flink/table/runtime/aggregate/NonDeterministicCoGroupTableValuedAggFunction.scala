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
import org.apache.flink.table.codegen.GeneratedCoTableValuedAggHandleFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.ProcessFunctionBase.Context
import org.apache.flink.table.runtime.functions.ExecutionContext
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes}
import org.apache.flink.table.util.BaseRowUtil.isAccumulateMsg
import org.apache.flink.util.Collector

/**
  * Class of non-deterministic co-table-valued Aggregate Function
  * used for the groupby (without window) aggregate
  *
  * @param genAggsHandler     the generated aggregate handler
  * @param accTypes           the accumulator types
  * @param generateRetraction whether this operator will generate retraction
  */
class NonDeterministicCoGroupTableValuedAggFunction(
    genAggsHandler: GeneratedCoTableValuedAggHandleFunction,
    accTypes: DataType,
    outputRowType: BaseRowType,
    generateRetraction: Boolean,
    queryConfig: StreamQueryConfig)
  extends CoGroupTableValuedAggFunctionBase(
    genAggsHandler,
    accTypes,
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

    LOG.debug(s"Compiling NonDeterministicCoGroupTableValuedAggFunction: " +
                s"${genAggsHandler.name} \n\n Code:\n${genAggsHandler.code}")
  }

  override def processElement1(
    input: BaseRow,
    ctx: Context,
    out: Collector[BaseRow]): Unit = {

    processElement(input, true, ctx, out)
  }

  override def processElement2(
    input: BaseRow,
    ctx: Context,
    out: Collector[BaseRow]): Unit = {

    processElement(input, false, ctx, out)
  }

  def processElement(
    input: BaseRow,
    isLeft: Boolean,
    ctx: Context,
    out: Collector[BaseRow]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    val currentKey = executionContext.currentKey()

    preAccumulateForNonDeterministic(function, accState, currentKey, out)

    var count = getCounter(currentKey, groupedDataCounter).getLong(0)
    // update aggregate result and set to the newRow
    if (isAccumulateMsg(input)) {
      // accumulate input
      if (isLeft) {
        function.accumulateLeft(input)
      } else {
        function.accumulateRight(input)
      }
      count += 1
    } else {
      // retract input
      if (isLeft) {
        function.retractLeft(input)
      } else {
        function.retractRight(input)
      }
      if (count > 0) {
        count -= 1
      }
    }

    postAccumulateForNonDeterministic(
      function,
      prevResults,
      accState,
      firstRow,
      generateRetraction,
      currentKey,
      groupedDataCounter,
      count,
      bufferedCollector,
      out)
  }
}

