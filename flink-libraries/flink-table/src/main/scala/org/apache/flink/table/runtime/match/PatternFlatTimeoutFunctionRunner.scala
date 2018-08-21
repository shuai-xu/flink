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

package org.apache.flink.table.runtime.`match`

import java.util

import org.apache.flink.cep.PatternFlatTimeoutFunction
import org.apache.flink.table.codegen.GeneratedPatternFlatTimeoutFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * PatternFlatTimeoutFunctionRunner with [[BaseRow]] input and [[BaseRow]] output.
  */
class PatternFlatTimeoutFunctionRunner(
    genFunction: GeneratedPatternFlatTimeoutFunction)
  extends PatternFlatTimeoutFunction[BaseRow, BaseRow]
  with Logging{

  private var function: PatternFlatTimeoutFunction[BaseRow, BaseRow] = _

  def init(): Unit = {
    LOG.debug(s"Compiling PatternFlatTimeoutFunction: ${genFunction.name} \n\n " +
                s"Code:\n${genFunction.code}")
    function = genFunction.newInstance(Thread.currentThread().getContextClassLoader)
  }

  override def timeout(
    pattern: util.Map[String, util.List[BaseRow]],
    timeoutTimestamp: Long,
    out: Collector[BaseRow]): Unit = {
    if (function == null) {
      init()
    }

    function.timeout(pattern, timeoutTimestamp, out)
  }
}
