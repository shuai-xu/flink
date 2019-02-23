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

import org.apache.flink.table.dataformat.BaseRow

/** OutputFormat that writes data to a single directory (used for non-dynamic-partition writes).
  *
  * Notes that although this format only output to one directory.
  *
  * */
class SingleDirectoryOutput(
    path: String,
    description: OutputJobDescription)
  extends FileSystemOutputFormat(path, description) {
  private val fileNum: Int = 0
  // Initialize current internal OutputFormat
  var firstRecord: Boolean = true

  override def refreshCurrentFormat(partitionKey: Option[BaseRow]): Unit = {
    releaseCurrentFormat()

    val ext = description.outputFormatFactory.getFileExtension(getTaskID)
    val currentPath = newTaskOutputFile(
      getTaskID,
      None,
      f"-c$fileNum%03d" + ext)

    currentFormat = description.outputFormatFactory.newOutputFormat(
      path = currentPath,
      dataSchema = description.dataSchema,
      taskId = getTaskID)
    configCurrentFormat()
  }

  override def writeRecord(record: BaseRow): Unit = {
    if (firstRecord) {
      refreshCurrentFormat(None)
      firstRecord = false
    }

    currentFormat.writeRecord(record)
  }
}
