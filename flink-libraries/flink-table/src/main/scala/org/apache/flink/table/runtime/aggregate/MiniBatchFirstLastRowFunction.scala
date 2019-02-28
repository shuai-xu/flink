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
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.EqualiserCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.{BundleFunction, ExecutionContext}
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.types.TypeConverters
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

import java.util.{Map => JMap}

/**
  * This function is used to get the first row or last row for every key partition in miniBatch
  * mode.
  *
  * @param rowTypeInfo        the type info of the input row.
  * @param generateRetraction the flag whether to generate retractions in this operator.
  * @param tableConfig        the table config.
  * @param isLastRowMode      if true, keep last row; else keep first row
  */
class MiniBatchFirstLastRowFunction(
    rowTypeInfo: BaseRowTypeInfo,
    generateRetraction: Boolean,
    tableConfig: TableConfig,
    isLastRowMode: Boolean)
  extends BundleFunction[BaseRow, BaseRow, BaseRow, BaseRow]
  with FirstLastRowFunctionBase
  with Logging {

  protected var pkRow: KeyedValueState[BaseRow, BaseRow] = _
  private val ser = rowTypeInfo.createSerializer()

  @transient
  private var equaliser: RecordEqualiser = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    val rowStateDesc = new ValueStateDescriptor("rowState", rowTypeInfo)
    pkRow = ctx.getKeyedValueState(rowStateDesc)

    val generator = new EqualiserCodeGenerator(
      rowTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo))
    val equaliserName = s"${if (isLastRowMode) "LastRow" else "FirstRow"}ValueEqualiser"
    val generatedEqualiser = generator.generateRecordEqualiser(equaliserName)
    equaliser = generatedEqualiser.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
  }

  override def addInput(value: BaseRow, input: BaseRow): BaseRow = {
    if (value == null || isLastRowMode ||
      (!isLastRowMode && isFirstRow(value, input))) {
      // put the input into buffer
      ser.copy(input)
    } else {
      // the input is not last row, ignore it
      value
    }
  }

  override def finishBundle(buffer: JMap[BaseRow, BaseRow], out: Collector[BaseRow]): Unit = {
    val preRowsMap = pkRow.getAll(buffer.keySet())
    val iter = buffer.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val currentKey = entry.getKey
      val currentRow = entry.getValue
      val preRow = preRowsMap.get(currentKey)

      if (isLastRowMode) {
        processLastRow(
          currentKey,
          preRow,
          currentRow,
          generateRetraction,
          stateCleaningEnabled = false,
          pkRow,
          equaliser,
          out)
      } else {
        processFirstRow(
          currentKey,
          preRow,
          currentRow,
          generateRetraction,
          stateCleaningEnabled = false,
          pkRow,
          equaliser,
          out
        )
      }
    }
  }
}
