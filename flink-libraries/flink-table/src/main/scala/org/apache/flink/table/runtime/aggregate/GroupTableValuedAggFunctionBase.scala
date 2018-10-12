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

import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.GeneratedTableValuedAggHandleFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.ProcessFunctionBase.OnTimerContext
import org.apache.flink.table.runtime.functions.{ExecutionContext, TableValuedAggHandleFunction}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.util.{BinaryRowUtil, Logging}
import org.apache.flink.util.Collector

/**
  * Base class of table-valued Aggregate Function used for the groupby (without window) aggregate
  */
abstract class GroupTableValuedAggFunctionBase(
    genAggsHandler: GeneratedTableValuedAggHandleFunction,
    accTypes: DataType,
    groupWithoutKey: Boolean,
    queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](queryConfig)
    with TableValuedAggFunctionBase
    with Logging {

  protected var function: TableValuedAggHandleFunction = _

  // stores the accumulators
  protected var accState: KeyedValueState[BaseRow, BaseRow] = _

  // stores processed row count of each key, instead of inputCounter property in GroupAggFunction
  protected var groupedDataCounter: KeyedValueState[BaseRow, BaseRow] = _

  protected var firstRow: Boolean = _

  protected var appendCollector: AppendGroupKeyCollector = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    val t = initState(accTypes, ctx)
    accState = t._1
    groupedDataCounter = t._2
    appendCollector = new AppendGroupKeyCollector
    initCleanupTimeState("GroupAggregateCleanupTime")
  }

  override def onTimer(
    timestamp: Long,
    ctx: OnTimerContext,
    out: Collector[BaseRow]): Unit = {
    if (needToCleanupState(timestamp)) {
      cleanupState(accState, groupedDataCounter)
      function.cleanup()
    }
  }

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }

  override def endInput(out: Collector[BaseRow]): Unit = {
    // output default value if grouping without key and it's an empty group
    if (groupWithoutKey) {
      executionContext.setCurrentKey(BinaryRowUtil.EMPTY_ROW)
      if (getCounter(executionContext.currentKey(), groupedDataCounter).getLong(0) == 0) {
        function.setAccumulators(function.createAccumulators)
        appendCollector.reSet(out, executionContext.currentKey(), isRetract = false)
        function.emitValue(appendCollector)
      }
    }
  }
}
