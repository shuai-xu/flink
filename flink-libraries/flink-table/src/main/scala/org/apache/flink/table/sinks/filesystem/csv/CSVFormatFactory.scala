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

package org.apache.flink.table.sinks.filesystem.csv

import org.apache.flink.api.common.io.FileOutputFormat.OutputDirectoryMode
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{Path => FPath}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.factories.csv.CsvOptions
import org.apache.flink.table.sinks.csv.BaseRowCsvOutputFormat
import org.apache.flink.table.sinks.filesystem.{FileSystemOptions, OutputFormatFactory}
import org.apache.flink.table.types.{AtomicType, DataType, RowType, TypeInfoWrappedDataType}
import org.apache.flink.table.util.TableProperties
import org.apache.flink.util.FlinkException

import java.util.TimeZone

/** [[OutputFormatFactory]] to create a [[OutputFormat]] of CSV format. */
class CSVFormatFactory extends OutputFormatFactory {
  private[this] var options: java.util.Map[String, String] = _

  override def configure(options: java.util.Map[String, String]): Unit = {
    this.options = options
  }

  override def getFileExtension(taskId: Int): String = {
    ".csv"
  }

  override def newOutputFormat(
      path: String,
      dataSchema: RowType,
      taskId: Int): OutputFormat[BaseRow]= {
    val csvOptions = (new TableProperties).putProperties(options).toKeyLowerCase
    val outputFormat = new BaseRowCsvOutputFormat(
      new FPath(path),
      dataSchema.getFieldInternalTypes)
    outputFormat.setAllowNullValues(csvOptions.getBoolean(CsvOptions.EMPTY_COLUMN_AS_NULL))
    outputFormat.setRecordDelimiter(csvOptions.getString(CsvOptions.OPTIONAL_LINE_DELIM))
    outputFormat.setFieldDelimiter(csvOptions.getString(CsvOptions.OPTIONAL_FIELD_DELIM))
    outputFormat.setQuoteCharacter(csvOptions.getString(CsvOptions.OPTIONAL_QUOTE_CHARACTER))
    outputFormat.setTimezone(TimeZone.getTimeZone(
      csvOptions.getString(CsvOptions.OPTIONAL_TIME_ZONE,
        FileSystemOptions.TIME_ZONE.defaultValue())))
    outputFormat.setWriteMode(getWriteMode(csvOptions))
    val outputFieldNames = csvOptions.getBoolean(CsvOptions.OPTIONAL_FIRST_LINE_AS_HEADER)
    if (outputFieldNames) {
      outputFormat.setOutputFieldName(true)
      outputFormat.setFieldNames(dataSchema.getFieldNames)
    }
    // we never want to suffix the outputPath and only want to output one absolute path.
    outputFormat.setOutputDirectoryMode(OutputDirectoryMode.NEVER)
    outputFormat
  }

  // If no overwrite mode is set, this attribute would be overridden by
  // CoreOptions.FILESYTEM_DEFAULT_OVERRIDE
  private[this] def getWriteMode(props: TableProperties): WriteMode = {
    val writeMode = props.getString(CsvOptions.OPTIONAL_WRITE_MODE)
    writeMode.toLowerCase match {
      case "no_overwrite" => WriteMode.NO_OVERWRITE
      case "overwrite" => WriteMode.OVERWRITE
      case _ => throw new FlinkException("Unknown overwrite mode for Csv sink, " +
        "optional: no_overwrite/overwrite")
    }
  }

  override def supportDataSchema(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case udt: TypeInfoWrappedDataType => supportDataSchema(udt.toInternalType)

    case _ => false
  }

  override def toString: String = "CSV"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[CSVFormatFactory]
}
