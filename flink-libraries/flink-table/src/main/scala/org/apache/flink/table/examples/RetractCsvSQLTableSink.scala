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

package org.apache.flink.table.examples

import java.util.TimeZone
import org.apache.flink.table.sinks.RetractCsvTableSink
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.types.{DataType, DataTypes}

class RetractCsvSQLTableSink(
  path: String,
  fieldTypes: Array[DataType],
  fieldNames: Option[Array[String]],
  fieldDelim: Option[String],
  rowDelim: Option[String],
  quoteCharacter: Option[String],
  numFiles: Option[Int],
  writeMode: Option[WriteMode],
  outputFieldNames: Option[Boolean],
  timezone: Option[TimeZone])
  extends RetractCsvTableSink(path, fieldDelim, rowDelim, quoteCharacter,
    numFiles, writeMode, outputFieldNames, timezone) {

  def this(
    path: String, fieldTypes: Array[DataType], fieldNames: Array[String],
    fieldDelim: String) {
    this(path, fieldTypes, Some(fieldNames),
      Some(fieldDelim), None, None,
      None, Some(WriteMode.OVERWRITE), None, None)
  }

  def this(
    path: String, fieldTypes: Array[DataType], fieldNames: Array[String],
    fieldDelim: String, rowDelim: String, quoteCharacter: String) {
    this(path, fieldTypes, Some(fieldNames),
      Some(fieldDelim), Some(rowDelim), Option(quoteCharacter),
      None, Some(WriteMode.OVERWRITE), None, None)
  }

  def this(
    path: String, fieldTypes: Array[DataType], fieldNames: Array[String],
    fieldDelim: String, timezone: TimeZone) {
    this(path, fieldTypes, Some(fieldNames),
      Some(fieldDelim), None, None,
      None, Some(WriteMode.OVERWRITE), None, Option(timezone))
  }

  def this(
    path: String, fieldTypes: Array[DataType], fieldNames: Array[String],
    fieldDelim: String, rowDelim: String, quoteCharacter: String, timezone: TimeZone) {
    this(path, fieldTypes, Some(fieldNames),
      Some(fieldDelim), Some(rowDelim), Option(quoteCharacter),
      None, Some(WriteMode.OVERWRITE), None, Option(timezone))
  }

  override def getFieldTypes: Array[DataType] = fieldTypes

  override def getFieldNames: Array[String] = fieldNames.getOrElse(super.getFieldNames)

  override def getRecordType: DataType = {
    DataTypes.createRowType(getFieldTypes, getFieldNames)
  }

}
