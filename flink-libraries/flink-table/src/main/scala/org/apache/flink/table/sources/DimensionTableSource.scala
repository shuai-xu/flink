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
package org.apache.flink.table.sources

import java.util

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.dataformat.BaseRow

/**
  * Defines an external dimension table and provides access to its data.
  * Param: keyPairs is no longer necessary and will be removed after interface changed for adding
  * constantKeys info.
  * @tparam T The return type of the [[TableSource]].
  */
trait DimensionTableSource[T] extends TableSource {

  /**
    * Defines IndexKey(s) of the table.
    * May include [unique index| index] or both.
    *
    * @return set of index keys, or empty set if no index keys at all
    */
  def getIndexes: util.Collection[IndexKey]

  /**
    * Returns the lookup function by given IndexKey of the table.
    *
    * @return the function for data lookup.
    */
  def getLookupFunction(keys: IndexKey): FlatMapFunction[BaseRow, T]

  /**
    * Returns the asynchronize lookup function by given IndexKey of the table.
    *
    * @return the function for data lookup.
    */
  def getAsyncLookupFunction(keys: IndexKey): AsyncFunction[BaseRow, T]

  /**
    * Returns true if this dimension table is changing by time, false if the table is static
    */
  def isTemporal: Boolean

  /**
    * Returns true if this dimension table could be queried asynchronously
    */
  def isAsync: Boolean

  /**
    * Returns config that defines the runtime behavior of async join table
    */
  def getAsyncConfig: AsyncConfig

}

class AsyncConfig {

  /** Defines async mode, ordered or unordered, default is ordered */
  var orderedMode: Boolean = true

  /** Defines async timeout ms, default is 180s */
  var timeoutMs: Long = 180000L

  /** Defines async buffer capacity, default is 100 */
  var bufferCapacity: Int = 100

  def setOrderedMode(orderedMode: Boolean): Unit = {
    this.orderedMode = orderedMode
  }

  def setTimeoutMs(timeoutMs: Long): Unit = {
    this.timeoutMs = timeoutMs
  }

  def setBufferCapacity(bufferCapacity: Int): Unit = {
    this.bufferCapacity = bufferCapacity
  }
}
