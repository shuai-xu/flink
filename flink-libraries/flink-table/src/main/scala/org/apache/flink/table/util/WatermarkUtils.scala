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

package org.apache.flink.table.util

import org.apache.calcite.sql.{SqlCall, SqlNode, SqlOperator}
import org.apache.flink.table.api.TableException
import java.util

import org.apache.flink.util.Preconditions

object WatermarkUtils {
  val WITH_OFFSET_FUNC: String = "withOffset"

  def getWithOffsetParameters(
      rowtimeField: String,
      watermarkCall: SqlCall): Long = {

    Preconditions.checkNotNull(watermarkCall, "Watermark call must not be null")
    Preconditions.checkNotNull(rowtimeField, "Rowtime field must not be null")
    val wmOp: SqlOperator = watermarkCall.getOperator
    val funcName: String = wmOp.getName
    if (WITH_OFFSET_FUNC.equalsIgnoreCase(funcName)) {
      val operands: util.List[SqlNode] = watermarkCall.getOperandList
      if (operands.size != 2) {
        throw new TableException("Watermark function " +
            "'withOffset(<rowtime_field>, <offset>)' only accept two arguments.")
      }
      val timeField: String = operands.get(0).toString
      if (!rowtimeField.equals(timeField)) {
        throw new TableException("The first argument of 'withOffset' must be the rowtime field.")
      }
      val offsetStr: String = operands.get(1).toString
      var offset: Long = -1
      try {
        offset = offsetStr.toLong
      } catch {
        case e: NumberFormatException =>
          throw new TableException(
            "The second argument of 'withOffset' must be an integer, but is " + offsetStr, e)
      }
      offset
    }
    else {
      throw new TableException("Unsupported Watermark Function :" + funcName)
    }
  }
}
