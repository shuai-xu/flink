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

import org.apache.flink.table.codegen.{CodeGenUtils, GeneratedProjection, Projection}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}

/**
  * OutputFormat writes data using dynamic partition writes, that means this format can write to
  * multiple directories (partitions).
  *
  * @param description Description about how to output records.
  */
class DynamicPartitionOutput(
    path: String,
    description: OutputJobDescription,
    generatedProjectValues: GeneratedProjection)
  extends FileSystemOutputFormat(path, description) {

  @transient private[this] var getPartitionValues: Projection[BaseRow, BinaryRow] = _

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    super.open(taskNumber, numTasks)

    getPartitionValues = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader,
      generatedProjectValues.name,
      generatedProjectValues.code)
      .newInstance.asInstanceOf[Projection[BaseRow, BinaryRow]]
  }

  /** Flag saying whether or not the data to be written out is partitioned. */
  private val hasPartitionKey = description.partitionSchema.nonEmpty

  assert(hasPartitionKey,
    s"""DynamicPartitionOutput should be used for writing out data that's partitioned.
       |OutputJobDescription: $description
     """.stripMargin)

  private val filesNum: Int = 0
  private var currentPartitionKey: Option[BinaryRow] = None

  /**
    * Opens a new OutputWriter given a partition key.
    *
    * @param partitionKey The partition which all tuples being written by this `OutputFormat`
    */
  override def refreshCurrentFormat(partitionKey: Option[BaseRow]): Unit = {
    releaseCurrentFormat()

    val partDir: Option[String] = partitionKey
      .map(PartitionPathUtils.getPartitionPath(_, description.partitionSchema.get,
        description.timeZone))
    partDir.foreach(addedPartitions.add)

    val ext = f".c$filesNum%03d" +
      description.outputFormatFactory.getFileExtension(getTaskID)

    // custom partition location.
    val currentPath = newTaskOutputFile(getTaskID, partDir, ext)

    currentFormat = description.outputFormatFactory.newOutputFormat(
      path = currentPath,
      dataSchema = description.dataSchema,
      taskId = getTaskID)
    configCurrentFormat()
  }

  override def writeRecord(record: BaseRow): Unit = {
    val nextPartitionKey = if (hasPartitionKey) Some(getPartitionValues(record)) else None

    if (currentPartitionKey != nextPartitionKey) {
      // Set a new partition - write a new partition dir.
      if (hasPartitionKey) {
        currentPartitionKey = Some(nextPartitionKey.get.copy())
      }

      refreshCurrentFormat(currentPartitionKey)
    }
    currentFormat.writeRecord(record)
  }
}
