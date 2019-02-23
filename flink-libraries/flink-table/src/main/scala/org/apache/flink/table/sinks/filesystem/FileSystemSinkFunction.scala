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

package org.apache.flink.table.sinks.filesystem

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.InputTypeConfigurable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.Logging
import org.apache.flink.util.FlinkException

/**
  * A function for filesystem sink.
  *
  * <p>We make this class inherit from deprecated OutputFormatSinkFunction cause now JobManager use
  * reflection of class [[OutputFormatSinkFunction]] to decide this is a format output do some
  * pre/after job tasks for whole job lifetime, this expected to be refactored, see
  * [[FileSystemOutputFormat]] for details.
  *
  * @param formatOutput output format with ability of partitioning.
  */
class FileSystemSinkFunction(
    val formatOutput: FileSystemOutputFormat)
  extends OutputFormatSinkFunction[BaseRow](formatOutput)
  with InputTypeConfigurable
  with Logging {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    formatOutput.setConfiguration(parameters)
  }

  override def setRuntimeContext(rc: RuntimeContext): Unit = {
    super.setRuntimeContext(rc)
    formatOutput.setRuntimeContext(rc)
  }

  override def setInputType(
    typeInfo: TypeInformation[_],
    executionConfig: ExecutionConfig): Unit = {
    formatOutput.setInputType(typeInfo)
    formatOutput.setExecutionConfig(executionConfig)
  }

  override def invoke(value: BaseRow): Unit = {
    try {
      formatOutput.writeRecord(value)
    } catch {
      case t: Throwable =>
        formatOutput.abort()
        throw new FlinkException("Task failed while writing rows.", t)
    }
  }
}
