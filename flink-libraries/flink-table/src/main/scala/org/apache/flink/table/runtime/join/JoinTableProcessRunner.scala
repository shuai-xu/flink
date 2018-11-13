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

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.runtime.conversion.InternalTypeConverters
import org.apache.flink.table.typeutils.{BaseRowSerializer, BaseRowTypeInfo}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class JoinTableProcessRunner(
    fetcher: FlatMapFunction[BaseRow, BaseRow],
    collectorName: String,
    var collectorCode: String,
    objectReuse: Boolean,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
    rightKeysInDefineOrder: List[Int],
    leftKeyIdx2KeyRowIdx: List[(Int, Int)],
    constantKeys: util.Map[Int, Tuple2[InternalType, Object]],
    @transient returnType: BaseRowTypeInfo[_])
  extends ProcessFunction[BaseRow, BaseRow]
  with ResultTypeQueryable[BaseRow]
  with Compiler[Any]
  with Logging {
  var collector: TableFunctionCollector[BaseRow] = _
  var leftKeyTypes: Array[InternalType] = _
  var keySer: BaseRowSerializer[GenericRow] = _
  var leftKeySerializers: Array[TypeSerializer[_]] = _
  var (inRowSrcIdx, keysRowTargetIdx) = prepareIdxHelper()
  val rightArity: Int = returnType.getArity - inputFieldTypes.length

  @transient var nullRow: BaseRow = _
  @transient var outRow: JoinedRow = _
  @transient var keysRow: GenericRow = _

  private def prepareIdxHelper(): (Array[Int], Array[Int]) = {
    val sortedMapping = leftKeyIdx2KeyRowIdx.sortWith(_._2 < _._2)
    (sortedMapping.map(_._1).toArray, sortedMapping.map(_._2).toArray)
  }

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling TableFunctionCollector: $collectorName \n\n Code:\n$collectorCode")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, collectorName, collectorCode)
    collectorCode = null
    LOG.debug("Instantiating TableFunctionCollector.")
    collector = clazz.newInstance().asInstanceOf[TableFunctionCollector[BaseRow]]

    FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext)
    FunctionUtils.openFunction(fetcher, parameters)

    LOG.info(s"left key to key row index mapping:$leftKeyIdx2KeyRowIdx")
    leftKeyTypes = inRowSrcIdx.map(inputFieldTypes(_))
    val config = getRuntimeContext.getExecutionConfig
    leftKeySerializers = leftKeyTypes.map(DataTypes.toTypeInfo(_).createSerializer(config))
    LOG.info(s"left keys:$inRowSrcIdx, types:$leftKeyTypes, right keys:$keysRowTargetIdx")

    nullRow = new GenericRow(rightArity)
    outRow = new JoinedRow()
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

    val keyTypes = new Array[InternalType](inRowSrcIdx.length + constantKeys.size())
    inRowSrcIdx.indices.foreach((i) => keyTypes(keysRowTargetIdx(i)) = leftKeyTypes(i))
    for ((i, (t, _)) <- constantKeys.asScala) keyTypes(rightKeysInDefineOrder.indexOf(i)) = t
    keySer = new BaseRowSerializer[GenericRow](classOf[GenericRow], keyTypes)
  }

  override def processElement(
    in: BaseRow,
    ctx: ProcessFunction[BaseRow, BaseRow]#Context,
    out: Collector[BaseRow]): Unit = {

    collector.setCollector(out)
    collector.setInput(in)
    collector.reset()

    def fillKeyRow(in: BaseRow): Unit = {
      for (i <- inRowSrcIdx.indices) {
        val srcIdx = inRowSrcIdx(i)
        val key = if (in.isNullAt(srcIdx)) {
          null
        } else {
          in.get(srcIdx, leftKeyTypes(i))
        }
        keysRow.update(keysRowTargetIdx(i), key)
      }
    }
    // fill left keys to keyRow
    fillKeyRow(in)

    if (objectReuse) {
      fetcher.flatMap(keySer.copy(keysRow), getFetcherCollector)
    } else {
      fetcher.flatMap(keysRow, getFetcherCollector)
    }

    if (leftOuterJoin && !collector.isCollected) {
      outRow.replace(in, nullRow)
      outRow.setHeader(in.getHeader)
      out.collect(outRow)
    }
  }

  def getFetcherCollector: Collector[BaseRow] = collector

  override def getProducedType: BaseRowTypeInfo[BaseRow] =
    returnType.asInstanceOf[BaseRowTypeInfo[BaseRow]]

  override def close(): Unit = {
    FunctionUtils.closeFunction(fetcher)
  }
}
