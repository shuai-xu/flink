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

package org.apache.flink.table.sinks.csv

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.runtime.functions.DateTimeFunctions
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSink, TableSinkBase, UpsertStreamTableSink}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.types.Row

import java.lang.{Boolean => JBool}
import java.util.TimeZone

/**
  * A simple [[org.apache.flink.table.sinks.TableSink]] to emit Upsert data as CSV files.
  *
  * @param path The output path to write the Table to.
  * @param fieldDelim The field delimiter
  * @param recordDelim The record delimiter
  * @param quoteCharacter The quote character
  * @param numFiles The number of files to write to
  * @param writeMode The write mode to specify whether existing files are overwritten or not.
  * @param outputFieldNames Whether output field names.
  */
class UpsertCsvTableSink(
    path: String,
    fieldDelim: Option[String],
    recordDelim: Option[String],
    quoteCharacter: Option[String],
    numFiles: Option[Int],
    writeMode: Option[WriteMode],
    outputFieldNames: Option[Boolean],
    timezone: Option[TimeZone])
  extends TableSinkBase[JTuple2[JBool, Row]]
  with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
  with UpsertStreamTableSink[Row] {

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter, ',' by default.
    */
  def this(path: String, fieldDelim: String = ",") {
    this(path, Some(fieldDelim), None, None, None, None, None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param recordDelim The record delimiter.
    * @param quoteCharacter The quote character.
    */
  def this(path: String, fieldDelim: String, recordDelim: String, quoteCharacter: String) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter), None, None, None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    */
  def this(path: String, fieldDelim: String, numFiles: Int, writeMode: WriteMode) {
    this(path, Some(fieldDelim), None, None, Some(numFiles), Some(writeMode), None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param recordDelim The record delimiter.
    * @param quoteCharacter The quote character.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    */
  def this(
    path: String,
    fieldDelim: String, recordDelim: String, quoteCharacter: String,
    numFiles: Int, writeMode: WriteMode) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      Some(numFiles), Some(writeMode), None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    * @param outputFieldNames Whether to output field names.
    */
  def this(
    path: String, fieldDelim: String, numFiles: Int, writeMode: WriteMode,
    outputFieldNames: Boolean, timezone: TimeZone) {
    this(path, Some(fieldDelim), None, None,
      Some(numFiles), Some(writeMode), Some(outputFieldNames), Option(timezone))
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param recordDelim The record delimiter.
    * @param quoteCharacter The quote character.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    * @param outputFieldNames Whether to output field names.
    */
  def this(
    path: String,
    fieldDelim: String, recordDelim: String, quoteCharacter: String,
    numFiles: Int, writeMode: WriteMode, outputFieldNames: Boolean, timezone: TimeZone) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      Some(numFiles), Some(writeMode), Some(outputFieldNames), Option(timezone))
  }

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  /** Emits the DataStream. */
  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    val csvRows = dataStream.map(
      new UpsertCsvFormatter(fieldDelim.getOrElse(","),
      outputFieldNames.getOrElse(false),
      getFieldNames,
      timezone.getOrElse(TimeZone.getTimeZone("UTC"))))

    if (numFiles.isDefined) {
      csvRows.setParallelism(numFiles.get)
    }

    val sink = writeMode match {
      case None => csvRows.writeAsText(path)
      case Some(wm) => csvRows.writeAsText(path, wm)
    }

    sink.name("UpsertCsvTableSink: " + path)

    if (numFiles.isDefined) {
      sink.setParallelism(numFiles.get)
    }
    sink
  }

  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new UpsertCsvTableSink(path, fieldDelim, recordDelim, quoteCharacter,
      numFiles, writeMode, outputFieldNames, timezone)
  }

  override def getRecordType: DataType = {
    DataTypes.createRowTypeV2(getFieldTypes: _*)
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]])
    : DataStreamSink[_] = {
    // Reuse code cause bounded stream is also kind of DataStream.
    emitDataStream(boundedStream)
  }
}

/**
  * Formats a [[JTuple2[Boolean, Row]]] into a [[String]] with fields separated by the field
  * delimiter.
  *
  * @param fieldDelim The field delimiter.
  */
class UpsertCsvFormatter(fieldDelim: String,
                         outputFieldNames: Boolean,
                         fieldNames: Array[String],
                         timezone: TimeZone)
  extends MapFunction[JTuple2[JBool, Row], String] {
  var outputNames: Boolean = outputFieldNames

  override def map(cRow: JTuple2[JBool, Row]): String = {

    val builder = new StringBuilder
    val row = cRow.f1

    if (outputNames) {
      outputNames = false
      for (i <- fieldNames.indices) {
        builder.append(fieldNames(i))
        if (i < fieldNames.length - 1) {
          builder.append(fieldDelim)
        }
      }
      builder.append("\n")
    }

    if (cRow.f0) {
      builder.append("Add")
    } else {
      builder.append("Delete")
    }

    // write following values
    for (i <- 0 until row.getArity) {
      builder.append(fieldDelim)
      val v = row.getField(i)
      if (v != null) {
        if (v.isInstanceOf[java.sql.Timestamp]) {
          val ts = v.asInstanceOf[java.sql.Timestamp]
          builder.append(DateTimeFunctions.dateFormatTz(ts.getTime,
            "yyyy-MM-dd HH:mm:ss.SSS", timezone.getID))
        }
        else if (v.isInstanceOf[java.sql.Date]) {
          val ts = v.asInstanceOf[java.sql.Date].getTime
          val offset = timezone.getOffset(ts)
          builder.append(DateTimeFunctions.dateFormatTz(ts - offset, "yyyy-MM-dd",
            timezone.getID))
        }
        else if (v.isInstanceOf[java.sql.Time]) {
          val ts = v.asInstanceOf[java.sql.Time].getTime
          val offset = timezone.getOffset(ts)
          builder.append(DateTimeFunctions.dateFormatTz(ts - offset, "HH:mm:ss",
            timezone.getID))
        }
        else {
          builder.append(v.toString)
        }
      }
    }
    builder.mkString
  }
}
