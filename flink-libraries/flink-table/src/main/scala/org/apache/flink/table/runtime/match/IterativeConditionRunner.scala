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

import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.codegen.GeneratedIterativeCondition
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.Logging

/**
  * IterativeConditionRunner with [[BaseRow]] value.
  */
class IterativeConditionRunner(
    genCondition: GeneratedIterativeCondition)
  extends IterativeCondition[BaseRow]
  with Logging {

  // IterativeCondition will be serialized as part of state,
  // so make function as transient to avoid ClassNotFoundException when restore state,
  // see FLINK-6939 for details
  @transient private var function: IterativeCondition[BaseRow] = _

  def init(): Unit = {
    LOG.debug(s"Compiling IterativeCondition: ${genCondition.name} \n\n" +
                s"Code:\n${genCondition.code}")
    function = genCondition.newInstance(Thread.currentThread().getContextClassLoader)
  }

  override def filter(value: BaseRow, ctx: IterativeCondition.Context[BaseRow]): Boolean = {
    if (function == null) {
      init()
    }

    function.filter(value, ctx)
  }
}
