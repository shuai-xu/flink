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
package com.alibaba.blink.table.sources

import java.util

import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.sources.TableSource

trait HBaseDimensionTableSource[T] extends TableSource {

  def getKeyName: String

  def setSelectKey(selectKey: Boolean): Unit

  def isSelectKey: Boolean

  def getRowKeyIndex: Int

  def isStrongConsistency: Boolean

  def isOrderedMode: Boolean

  def partitioning: Boolean = false

  def caching: Boolean = false

  def shuffleEmptyKey: Boolean = true

  def getAsyncTimeoutMs: Long

  def getAsyncBufferCapacity: Int

  /** Returns the number of fields of the table. */
  def getOutputFieldsNumber: Int

  /** Returns the names of the table fields. */
  def getFieldNames: Array[String]

  /** Returns the names of the table fields. */
  def getFieldTypes: Array[TypeInformation[_]]

  def getAsyncFetchFunction[IN, OUT](
      resultType: TypeInformation[OUT],
      joinType: JoinRelType,
      sourceKeyIndex: Int,
      keyType: TypeInformation[_],
      strongConsistency: Boolean,
      objectReuseEnabled: Boolean): AsyncFunction[IN, OUT]

  def getAsyncMultiFetchFunction[IN, OUT](
      resultType: TypeInformation[OUT],
      requiredLeftFieldIdx: Array[Int],
      requiredLeftFieldTypes: Array[TypeInformation[_]],
      chainedSources: util.List[HBaseDimensionTableSource[_]],
      chainedJoinTypes: util.List[JoinRelType],
      chainedLeftJoinKeyIndexes: util.List[Integer],
      chainedLeftJoinKeyTypes: util.List[TypeInformation[_]],
      strongConsistencyOnChain: Boolean,
      objectReuseEnabled: Boolean): AsyncFunction[IN, OUT]

}
