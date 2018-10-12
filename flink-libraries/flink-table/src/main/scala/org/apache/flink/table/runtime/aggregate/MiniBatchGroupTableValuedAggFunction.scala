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

import java.util.{HashMap => JHashMap, List => JList, Map => JMap}


import org.apache.flink.table.codegen.GeneratedTableValuedAggHandleFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.types.DataType
import org.apache.flink.table.util.BaseRowUtil._
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
 * Class of Table-Valued Aggregate Function used for the groupby (without window) aggregate
 * in minibatch mode.
 */
class MiniBatchGroupTableValuedAggFunction(
    genAggsHandler: GeneratedTableValuedAggHandleFunction,
    accTypes: DataType,
    generateRetraction: Boolean,
    groupWithoutKey: Boolean)
  extends MiniBatchGroupTableValuedAggFunctionBase(
    genAggsHandler,
    accTypes,
    generateRetraction,
    groupWithoutKey
  )
  with Logging {

  override def finishBundle(
    buffer: JMap[BaseRow, JList[BaseRow]],
    out: Collector[BaseRow]): Unit = {

    val accMap = accState.getAll(buffer.keySet())
    val accCounter = groupedDataCounter.getAll(buffer.keySet())
    val accResult = new JHashMap[BaseRow, BaseRow]()

    val iter = buffer.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val elements = entry.getValue
      val currentKey = entry.getKey
      // set current key to make dataview know current key
      ctx.setCurrentKey(currentKey)

      var firstRow = false

      // step 1: get the acc for the current key
      var acc = accMap.get(currentKey)
      if (acc == null) {
        acc = function.createAccumulators()
        firstRow = true
      }

      // if this was not the first row and we have to emit retractions
      if (!firstRow) {
        if (generateRetraction) {
          function.setAccumulators(acc)
          appendCollector.reSet(out, currentKey, true)
          function.emitValue(appendCollector)
        }
      }

      // step 2: accumulate
      function.setAccumulators(acc)

      var count = getCounter(currentKey, accCounter).getLong(0)

      val elementIter = elements.iterator()
      while (elementIter.hasNext) {
        val input = elementIter.next()
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
      }

      val counter = getCounter(currentKey)
      counter.setLong(0, count)
      accCounter.put(currentKey, counter)

      if (count != 0) {
        // emit new result
        appendCollector.reSet(out, currentKey, false)
        function.emitValue(appendCollector)

        // update the state
        accResult.put(currentKey, function.getAccumulators)

      } else {
        // clear all state
        function.cleanup()
        accState.remove(currentKey)
        groupedDataCounter.remove(currentKey)
      }

    }

    // batch update to state
    if (!accResult.isEmpty) {
      accState.putAll(accResult)
    }
    if (!accCounter.isEmpty) {
      groupedDataCounter.putAll(accCounter)
    }
  }
}
