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

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.codegen.GeneratedTableValuedAggHandleFunction
import org.apache.flink.table.dataformat.{GenericRow, BaseRow}
import org.apache.flink.table.runtime.functions.{ExecutionContext, TableValuedAggHandleFunction}
import org.apache.flink.table.runtime.functions.bundle.BundleFunction
import org.apache.flink.table.types.{DataTypes, DataType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{BinaryRowUtil, Logging}
import org.apache.flink.util.Collector

/**
 * Base class of table-valued Aggregate Function used for the groupby (without window) aggregate
 * in mini batch mode.
 */
abstract class MiniBatchGroupTableValuedAggFunctionBase(
    genAggsHandler: GeneratedTableValuedAggHandleFunction,
    accTypes: DataType,
    generateRetraction: Boolean,
    groupWithoutKey: Boolean)
  extends BundleFunction[BaseRow, JList[BaseRow], BaseRow, BaseRow]
    with Logging {

  protected var function: TableValuedAggHandleFunction = _

  // stores the accumulators
  protected var accState: KeyedValueState[BaseRow, BaseRow] = _

  // stores processed row count of each key, instead of inputCounter property in GroupAggFunction
  protected var groupedDataCounter: KeyedValueState[BaseRow, BaseRow] = _

  protected var appendCollector: AppendGroupKeyCollector = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    // serialize as GenericRow, deserialize as BinaryRow
    val accTypeInfo = new BaseRowTypeInfo(classOf[BaseRow], DataTypes.toTypeInfo(accTypes))
    val accDesc = new ValueStateDescriptor("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(accDesc)

    val counterTypeInfo =
      new BaseRowTypeInfo(classOf[BaseRow], DataTypes.toTypeInfo(DataTypes.LONG))
    val counterDesc = new ValueStateDescriptor("groupedDataCounter", counterTypeInfo)
    groupedDataCounter = ctx.getKeyedValueState(counterDesc)

    appendCollector = new AppendGroupKeyCollector
  }

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }

  override def addInput(value: JList[BaseRow], input: BaseRow): JList[BaseRow] = {
    val acc = if (value == null) {
      new JArrayList[BaseRow]()
    } else {
      value
    }
    acc.add(input)
    acc
  }


  override def endInput(out: Collector[BaseRow]): Unit = {
    // output default value if grouping without key and it's an empty group
    if (groupWithoutKey) {
      ctx.setCurrentKey(BinaryRowUtil.EMPTY_ROW)
      if (getCounter(ctx.currentKey()).getLong(0) == 0) {
        function.setAccumulators(function.createAccumulators)
        appendCollector.reSet(out, ctx.currentKey(), isRetract = false)
        function.emitValue(appendCollector)
      }
    }
  }

  def getCounter(key: BaseRow, map: JMap[BaseRow, BaseRow] = null): BaseRow = {
    if (map == null) {
      var counter = groupedDataCounter.get(key)
      if (counter == null) {
        counter = new GenericRow(1)
        counter.setLong(0, 0)
        groupedDataCounter.put(key, counter)
      }
      counter
    } else {
      var counter = map.get(key)
      if (counter == null) {
        counter = new GenericRow(1)
        counter.setLong(0, 0)
        map.put(key, counter)
      }
      counter
    }
  }
}
