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
package org.apache.flink.table.runtime.join

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.types.InternalType
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

class JoinTableWithCalcAsyncRunner(
    fetcher: AsyncFunction[BaseRow, BaseRow],
    calcFunctionName: String,
    calcFunctionCode: String,
    collectorName: String,
    collectorCode: String,
    objectReuse: Boolean,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
    rightKeysInDefineOrder: List[Int],
    leftKeyIdx2KeyRowIdx: List[(Int, Int)],
    constantKeys: util.Map[Int, Tuple2[InternalType, Object]],
    @transient returnType: BaseRowTypeInfo[_])
  extends JoinTableAsyncRunner(
    fetcher,
    collectorName,
    collectorCode,
    objectReuse,
    leftOuterJoin,
    inputFieldTypes,
    rightKeysInDefineOrder,
    leftKeyIdx2KeyRowIdx,
    constantKeys,
    returnType) {

  // keep Row to keep compatible
  var calcClass: Class[FlatMapFunction[BaseRow, BaseRow]] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    LOG.debug(s"Compiling CalcFunction: $calcFunctionName \n\n Code:\n$calcFunctionCode")
    calcClass = compile(
      getRuntimeContext.getUserCodeClassLoader,
      calcFunctionName,
      calcFunctionCode).asInstanceOf[Class[FlatMapFunction[BaseRow, BaseRow]]]

    LOG.debug("Instantiating CalcFunction.")
    // trying to instantiating
    calcClass.newInstance()
  }

  override protected def getAsyncCollector(
      row: BaseRow,
      asyncCollector: ResultFuture[BaseRow]): ResultFuture[BaseRow] = {
    val collector = super.getAsyncCollector(row, asyncCollector)
    new CalcAsyncCollector(calcClass.newInstance(), collector)
  }

  class CalcAsyncCollector(
      calcFlatMap: FlatMapFunction[BaseRow, BaseRow],
      delegate: ResultFuture[BaseRow])
    extends ResultFuture[BaseRow] {

    override def complete(collection: util.Collection[BaseRow]): Unit = {
      if (collection == null || collection.size() == 0) {
        delegate.complete(collection)
      } else {
        // TODO: currently, collection should only contain one element
        val input = collection.iterator().next()
        val collectionCollector = new CalcCollectionCollector
        calcFlatMap.flatMap(input, collectionCollector)
        delegate.complete(collectionCollector.collection)
      }
    }

    override def completeExceptionally(throwable: Throwable): Unit =
      delegate.completeExceptionally(throwable)
  }

  class CalcCollectionCollector extends Collector[BaseRow] {

    var collection: util.Collection[BaseRow] = Collections.emptyList()

    override def collect(t: BaseRow): Unit = {
      collection = Collections.singleton(t)
    }

    override def close(): Unit = {
    }
  }
}
