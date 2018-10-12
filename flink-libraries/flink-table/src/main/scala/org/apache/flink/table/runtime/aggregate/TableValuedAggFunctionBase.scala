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

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.{KeyedListState, KeyedValueState}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.functions.{ExecutionContext, TableValuedAggHandleFunctionBase}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BaseRowUtil
import org.apache.flink.util.Collector

/**
  * Base class for table valued function. Both for table and co-table, deterministic and
  * non-deterministic.
  */
trait TableValuedAggFunctionBase {

  /**
    * Init accumulate state and count state.
    */
  def initState(accTypes: DataType, ctx: ExecutionContext):
  (KeyedValueState[BaseRow, BaseRow], KeyedValueState[BaseRow, BaseRow]) = {

    // serialize as GenericRow, deserialize as BinaryRow
    val accTypeInfo = new BaseRowTypeInfo(classOf[BaseRow], DataTypes.toTypeInfo(accTypes))
    val accDesc = new ValueStateDescriptor("accState", accTypeInfo)
    val accState = ctx.getKeyedValueState(accDesc)
      .asInstanceOf[KeyedValueState[BaseRow, BaseRow]]

    val counterTypeInfo =
      new BaseRowTypeInfo(classOf[BaseRow], DataTypes.toTypeInfo(DataTypes.LONG))
    val counterDesc = new ValueStateDescriptor("groupedDataCounter", counterTypeInfo)
    val groupedDataCounter = ctx.getKeyedValueState(counterDesc)
      .asInstanceOf[KeyedValueState[BaseRow, BaseRow]]
    (accState, groupedDataCounter)
  }

  /**
    * Get count value for a key.
    */
  def getCounter(key: BaseRow, groupedDataCounter: KeyedValueState[BaseRow, BaseRow]): BaseRow = {
    var counter = groupedDataCounter.get(key)
    if (counter == null) {
      counter = new GenericRow(1)
      counter.setLong(0, 0)
      groupedDataCounter.put(key, counter)
    }
    counter
  }

  /**
    * Init accumulators before accumulate.
    */
  def preAccumulate(
    function: TableValuedAggHandleFunctionBase,
    accState: KeyedValueState[BaseRow, BaseRow],
    currentKey: BaseRow,
    generateRetraction: Boolean,
    appendCollector: AppendGroupKeyCollector,
    out: Collector[BaseRow]): Unit = {

    var accumulators = accState.get(currentKey)
    val firstRow: Boolean = if (null == accumulators) {
      accumulators = function.createAccumulators()
      true
    } else {
      false
    }

    // if this was not the first row and we have to emit retractions
    if (!firstRow) {
      if (generateRetraction) {
        function.setAccumulators(accumulators)
        appendCollector.reSet(out, currentKey, true)
        function.emitValue(appendCollector)
      }
    }

    // set accumulators to handler first
    function.setAccumulators(accumulators)
  }

  /**
    * Init accumulators before accumualte.
    */
  def preAccumulateForNonDeterministic(
    function: TableValuedAggHandleFunctionBase,
    accState: KeyedValueState[BaseRow, BaseRow],
    currentKey: BaseRow,
    out: Collector[BaseRow]): Boolean = {

    var accumulators = accState.get(currentKey)
    val firstRow = if (null == accumulators) {
      accumulators = function.createAccumulators()
      true
    } else {
      false
    }

    // set accumulators to handler first
    function.setAccumulators(accumulators)
    firstRow
  }

  /**
    * Update state and emit results after accumulate.
    */
  def postAccumulate(
    function: TableValuedAggHandleFunctionBase,
    accState: KeyedValueState[BaseRow, BaseRow],
    currentKey: BaseRow,
    groupedDataCounter: KeyedValueState[BaseRow, BaseRow],
    count: Long,
    appendCollector: TableValuedAggCollector,
    out: Collector[BaseRow]): Unit = {

    val counter = getCounter(currentKey, groupedDataCounter)
    counter.setLong(0, count)
    groupedDataCounter.put(currentKey, counter)

    if (count != 0) {
      // emit new result
      appendCollector.reSet(out, currentKey, false)
      function.emitValue(appendCollector)

      // update the state
      accState.put(currentKey, function.getAccumulators)

    } else {
      // clear all state
      function.cleanup()
      accState.remove(currentKey)
      groupedDataCounter.remove(currentKey)
    }
  }

  /**
    * Update state and emit results after accumulate.
    */
  def postAccumulateForNonDeterministic(
    function: TableValuedAggHandleFunctionBase,
    prevResults: KeyedListState[BaseRow, BaseRow],
    accState: KeyedValueState[BaseRow, BaseRow],
    firstRow: Boolean,
    generateRetraction: Boolean,
    currentKey: BaseRow,
    groupedDataCounter: KeyedValueState[BaseRow, BaseRow],
    count: Long,
    bufferedCollector: TableValuedAggCollector,
    out: Collector[BaseRow]): Unit = {

    // if this was not the first row and we have to emit retractions
    if (!firstRow) {
      if (generateRetraction) {
        val tmpList = prevResults.get(currentKey)
        // avoid NPE
        if (tmpList != null) {
          val it = tmpList.iterator()
          while (it.hasNext) {
            var row = it.next()
            row = BaseRowUtil.setRetract(row)
            out.collect(row)
          }
        }
      }
    }

    // clean prevResults state
    prevResults.remove(currentKey)

    postAccumulate(
      function,
      accState,
      currentKey,
      groupedDataCounter,
      count,
      bufferedCollector,
      out)
  }
}
