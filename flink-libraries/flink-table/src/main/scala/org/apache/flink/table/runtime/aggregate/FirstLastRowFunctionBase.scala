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
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.util.{Collector, Preconditions}

trait FirstLastRowFunctionBase {

  def processLastRow(
    currentKey: BaseRow,
    preRow: BaseRow,
    currentRow: BaseRow,
    generateRetraction: Boolean,
    stateCleaningEnabled: Boolean,
    pkRow: KeyedValueState[BaseRow, BaseRow],
    equaliser: RecordEqualiser,
    out: Collector[BaseRow]): Unit = {
    // should be accumulate msg.
    Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow))

    // ignore same record
    if (!stateCleaningEnabled && preRow != null &&
      equaliser.equalsWithoutHeader(preRow, currentRow)) {
      return
    }

    pkRow.put(currentKey, currentRow)
    if (preRow != null && generateRetraction) {
      preRow.setHeader(BaseRowUtil.RETRACT_MSG)
      out.collect(preRow)
    }
    out.collect(currentRow)
  }

  def processFirstRow(
    currentKey: BaseRow,
    preRow: BaseRow,
    currentRow: BaseRow,
    generateRetraction: Boolean,
    stateCleaningEnabled: Boolean,
    pkRow: KeyedValueState[BaseRow, BaseRow],
    equaliser: RecordEqualiser,
    out: Collector[BaseRow]): Unit = {
    // should be accumulate msg.
    Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow))

    // ignore record with timestamp bigger than preRow
    if (!isFirstRow(preRow, currentRow)) {
      return
    }

    pkRow.put(currentKey, currentRow)
    out.collect(currentRow)
  }

  def isFirstRow(preRow: BaseRow, currentRow: BaseRow): Boolean = {
      preRow == null
  }

}
