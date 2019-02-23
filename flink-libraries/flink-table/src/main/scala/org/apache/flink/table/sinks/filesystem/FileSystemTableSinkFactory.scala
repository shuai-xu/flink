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
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataType, RowType}
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.factories.BatchTableSinkFactory
import org.apache.flink.table.factories.csv.CsvOptions
import org.apache.flink.table.sinks.filesystem.FileSystemOutputFormat.OutputSpec
import org.apache.flink.table.sinks.{BatchTableSink, TableSinkBase}
import org.apache.flink.table.sinks.filesystem.csv.CSVFormatFactory
import org.apache.flink.table.util.TableProperties

import java.util
import java.util.UUID

import scala.collection.JavaConversions._

/** TableSink for filesystem table sink, e.g. CSV, PARQUET, ORC. */
class FileSystemTableSinkFactory extends BatchTableSinkFactory[BaseRow] {

  override def createBatchTableSink(properties: util.Map[String, String])
    : BatchTableSink[BaseRow] = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)
    val richSchema = tableProperties.readSchemaFromProperties(null)
    val formatFactory = getFormatFactory(tableProperties)
    val path = tableProperties.getString(FileSystemOptions.PATH)
    // Use UUID instead cause Job Id is not available.
    val jobID = UUID.randomUUID().toString

    val inputSchema = richSchema.getResultType
    val partitionSchema = new RowType(
      richSchema.getPartitionDataTypes.toArray(Array[DataType]()),
      richSchema.getPartitionColumns.toArray(Array[String]()))
    val writeJobDescription = FileSystemOutputFormat
      .getOutputJobDescription(
        formatFactory,
        OutputSpec(path, null, inputSchema),
        Some(partitionSchema),
        properties)

    // Check if the format support this row type.
    require(richSchema.getColumnTypes.forall(formatFactory.supportDataSchema(_)),
      "FileFormat sink only support built-in data types now.")

    new BatchTableSink[BaseRow]
      with DefinedDistribution
      with TableSinkBase[BaseRow] {
      override def emitBoundedStream(
        boundedStream: DataStream[BaseRow],
        tableConfig: TableConfig,
        executionConfig: ExecutionConfig): DataStreamSink[_] = {
        val formatWriter = FileSystemOutputFormat.getFileFormatOutput(
          writeJobDescription,
          jobID,
          path)
        val ret = boundedStream
          .addSink(new FileSystemSinkFunction(formatWriter))
          .name(s"${formatFactory.toString}FileFormatSink")
        ret
      }

      override def getOutputType: DataType = {
        new RowType(getFieldTypes.map(_.toInternalType): _*)
      }

      override protected def copy: TableSinkBase[BaseRow] = { this } // dummy func.

      override def getPartitionFields(): Array[String] =
        richSchema.getPartitionColumns.toArray(Array[String]())

      override def sortLocalPartition(): Boolean = {
        // override to let planner decide if needs to sort before output.
        // only support sort mode now.
        true
      }
    }
  }

  // ------------------------------- tool methods -----------------------------------

  private def getFormatFactory(props: TableProperties): OutputFormatFactory = {
    props.getString(FileSystemOptions.FORMAT.key(), "").toLowerCase match {
      case "csv" =>
        new CSVFormatFactory
      case "orc" =>
        null // TODO: support new formats.
      case "parquet" =>
        null
      case _ =>
        throw new RuntimeException("Required attribute fileFormat must not be null!")
    }
  }

  // ------------------------------- factory context validation -----------------------------------
  override def requiredContext(): util.Map[String, String] = {
    val context: util.Map[String, String] = new util.HashMap()
    context.put(CONNECTOR_TYPE, "FILESYSTEM") // FILESYSTEM
    context.put(CONNECTOR_PROPERTY_VERSION, "1") // backwards compatibility
    context
  }

  override def supportedProperties(): util.List[String] = {
    FileSystemOptions.SUPPORTED_KEYS ++ CsvOptions.SUPPORTED_KEYS
  }
}
