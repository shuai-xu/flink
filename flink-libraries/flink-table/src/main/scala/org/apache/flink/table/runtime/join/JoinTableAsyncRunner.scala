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
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.collector.{JoinedRowAsyncCollector, TableAsyncCollector}
import org.apache.flink.table.runtime.conversion.InternalTypeConverters
import org.apache.flink.table.types.{DataTypes, InternalType}
import org.apache.flink.table.typeutils.{BaseRowSerializer, BaseRowTypeInfo}
import org.apache.flink.table.util.Logging

import scala.collection.JavaConverters._

class JoinTableAsyncRunner(
    fetcher: AsyncFunction[BaseRow, BaseRow],
    collectorName: String,
    var collectorCode: String,
    capacity: Int,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
    rightKeysInDefineOrder: List[Int],
    leftKeyIdx2KeyRowIdx: List[(Int, Int)],
    constantKeys: util.Map[Int, Tuple2[InternalType, Object]],
    @transient returnType: BaseRowTypeInfo[_])
  extends RichAsyncFunction[BaseRow, BaseRow]
  with ResultTypeQueryable[BaseRow]
  with Compiler[Any]
  with Logging {

  var collectorQueue: BlockingQueue[JoinedRowAsyncCollector] = _
  var collectorClass: Class[TableAsyncCollector[BaseRow]] = _
  val rightArity: Int = returnType.getArity - inputFieldTypes.length
  var leftKeyTypes: Array[InternalType] = _
  var keySer: BaseRowSerializer[GenericRow] = _
  var leftKeySerializers: Array[TypeSerializer[_]] = _
  var (inRowSrcIdx, keysRowTargetIdx) = prepareIdxHelper()
  private val rightTypes = returnType.getFieldTypes.map(DataTypes.internal)
      .drop(inputFieldTypes.length)

  @transient var keysRow: GenericRow = _

  private def prepareIdxHelper(): (Array[Int], Array[Int]) = {
    val sortedMapping = leftKeyIdx2KeyRowIdx.sortWith(_._2 < _._2)
    (sortedMapping.map(_._1).toArray, sortedMapping.map(_._2).toArray)
  }

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling TableAsyncCollector: $collectorName \n\n Code:\n$collectorCode")
    collectorClass = compile(
      getRuntimeContext.getUserCodeClassLoader,
      collectorName,
      collectorCode).asInstanceOf[Class[TableAsyncCollector[BaseRow]]]
    collectorCode = null

    // add an additional collector in it to avoid blocking on the queue when taking a collector
    collectorQueue = new ArrayBlockingQueue[JoinedRowAsyncCollector](capacity + 1)
    for (_ <- 0 until capacity + 1) {
      val c = new JoinedRowAsyncCollector(
        collectorQueue,
        getDimensionTableCollector,
        rightArity,
        leftOuterJoin,
        rightTypes)
      // throws exception immediately if the queue is full which should never happen
      collectorQueue.add(c)
    }

    FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext)
    FunctionUtils.openFunction(fetcher, parameters)

    LOG.info(s"left key to key row index mapping:$leftKeyIdx2KeyRowIdx")
    leftKeyTypes = inRowSrcIdx.map(inputFieldTypes(_))
    keySer = new BaseRowSerializer[GenericRow](classOf[GenericRow], leftKeyTypes)
    val config = getRuntimeContext.getExecutionConfig
    leftKeySerializers = leftKeyTypes.map(DataTypes.toTypeInfo(_).createSerializer(config))
    LOG.info(s"left keys:$inRowSrcIdx, types:$leftKeyTypes, right keys:$keysRowTargetIdx")
    // row contains all join keys
    keysRow = new GenericRow(rightKeysInDefineOrder.length)
    // fill constant keys
    constantKeys.asScala.foreach {
      key => {
        val targetIdx = rightKeysInDefineOrder.indexOf(key._1)
        val internalVal = InternalTypeConverters.getConverterForType(key._2._1)
          .toInternal(key._2._2)
        keysRow.update(targetIdx, internalVal)
        LOG.info(s"init constant key index[$targetIdx]=[${key._2._2}]")
      }
    }
  }

  override def asyncInvoke(in: BaseRow, asyncCollector: ResultFuture[BaseRow]): Unit = {
    val collector = collectorQueue.take()
    // the input row is copied when object reuse in AsyncWaitOperator
    collector.reset(in, asyncCollector)

    def fillKeyRow(in: BaseRow, keyRow: GenericRow): Unit = {
      for (i <- inRowSrcIdx.indices) {
        val srcIdx = inRowSrcIdx(i)
        val key = if (in.isNullAt(srcIdx)) {
          null
        } else {
          in.get(srcIdx, leftKeyTypes(i))
        }
        keyRow.update(keysRowTargetIdx(i), key)
      }
    }
    // fill left keys to new keyRow instance because reuse it is not definitely safe here
    val thisKeyRow = keySer.copy(keysRow)
    fillKeyRow(in, thisKeyRow)

    fetcher.asyncInvoke(thisKeyRow, collector)
  }

  protected def getDimensionTableCollector: TableAsyncCollector[BaseRow] = {
    collectorClass.newInstance()
  }

  override def getProducedType: BaseRowTypeInfo[BaseRow] =
    returnType.asInstanceOf[BaseRowTypeInfo[BaseRow]]

  override def close(): Unit = {
    FunctionUtils.closeFunction(fetcher)
  }
}
