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

import org.apache.flink.table.dataformat.{JoinedRow, BaseRow}
import org.apache.flink.table.util.BaseRowUtil
import org.apache.flink.util.Collector

/**
 * Collector which saves group key in collected data.
 */
class AppendGroupKeyCollector extends Collector[BaseRow] with Serializable {

  protected var resultRow: JoinedRow = new JoinedRow()
  protected var currentKey: BaseRow = _
  protected var out: Collector[BaseRow] = _
  protected var isRetract: Boolean = false

  def reSet(out: Collector[BaseRow],
      currentKey: BaseRow,
      isRetract: Boolean): Unit = {
    this.out = out
    this.currentKey = currentKey
    this.isRetract = isRetract
  }

  override def collect(record: BaseRow): Unit = {

    resultRow = resultRow.replace(currentKey, record)

    if (isRetract) {
      resultRow = BaseRowUtil.setRetract(resultRow).asInstanceOf[JoinedRow]
    } else {
      resultRow = BaseRowUtil.setAccumulate(resultRow).asInstanceOf[JoinedRow]
    }
    out.collect(resultRow)
  }

  override def close(): Unit = {
    out.close()
  }
}
