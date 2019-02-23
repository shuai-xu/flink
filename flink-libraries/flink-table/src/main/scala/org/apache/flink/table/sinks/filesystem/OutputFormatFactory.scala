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

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.table.api.types.{DataType, RowType}
import org.apache.flink.table.dataformat.BaseRow

/**
  * A factory that produces [[OutputFormat]]s, A new [[OutputFormatFactory]] is created on
  * JobManager side for each write job issued when writing to a filesystem sink, and then
  * gets serialized to executor side to create actual [[OutputFormat]]s on the fly.
  */
abstract class OutputFormatFactory extends Serializable {

  def configure(options: java.util.Map[String, String]): Unit

  /** Returns the supported data row type for this factory. */
  def supportDataSchema(dataType: DataType): Boolean

  /** Returns the file extension to be used when writing files out. */
  def getFileExtension(taskId: Int): String

  /**
    * When writing to a filesystem sink, this method gets called by each task on TM side to
    * instantiate new [[OutputFormat]]s.
    *
    * @param path Path to write the file.
    * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
    *                   schema if the sink table being written is partitioned.
    * @param taskId The execution task identifier.
    */
  def newOutputFormat(path: String, dataSchema: RowType, taskId: Int): OutputFormat[BaseRow]
}
