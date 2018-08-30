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

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * A CorrelateProcessRunner with [[BaseRow]] input and [[BaseRow]] output.
  */
class CorrelateProcessRunner(
  processName: String,
  processCode: String,
  collectorName: String,
  collectorCode: String,
  @transient var returnType: BaseRowTypeInfo[BaseRow])
  extends ProcessFunction[BaseRow, BaseRow]
    with ResultTypeQueryable[BaseRow]
    with Compiler[Any]
    with Logging {

  private var function: ProcessFunction[BaseRow, BaseRow] = _
  private var collector: TableFunctionCollector[_] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling TableFunctionCollector: $collectorName \n\n Code:\n$collectorCode")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, collectorName, collectorCode)
    LOG.debug("Instantiating TableFunctionCollector.")
    collector = clazz.newInstance().asInstanceOf[TableFunctionCollector[_]]

    LOG.debug(s"Compiling ProcessFunction: $processName \n\n Code:\n$processCode")
    val processClazz = compile(getRuntimeContext.getUserCodeClassLoader, processName, processCode)
    val constructor = processClazz.getConstructor(classOf[TableFunctionCollector[_]])
    LOG.debug("Instantiating ProcessFunction.")
    function = constructor.newInstance(collector).asInstanceOf[ProcessFunction[BaseRow, BaseRow]]
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def processElement(
    in: BaseRow,
    ctx: ProcessFunction[BaseRow, BaseRow]#Context,
    out: Collector[BaseRow])
  : Unit = {

    if (LOG.isDebugEnabled) {
      LOG.debug(s"Input Row is: ${in.toString}")
    }

    collector.setCollector(out)
    collector.setInput(in)
    collector.reset()

    function.processElement(
      in,
      ctx.asInstanceOf[ProcessFunction[BaseRow, BaseRow]#Context],
      out)
  }

  override def getProducedType: BaseRowTypeInfo[BaseRow] = returnType

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}
