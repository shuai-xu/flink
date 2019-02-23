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

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType, RowType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.DateTimeFunctions
import org.apache.hadoop.util.Shell

import java.util.TimeZone

object PartitionPathUtils {

  // This duplicates default value of Hive `ConfVars.DEFAULTPARTITIONNAME`
  val DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__"

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // The following string escaping code is mainly copied from Hive (o.a.h.h.common.FileUtils).
  //////////////////////////////////////////////////////////////////////////////////////////////////

  val charToEscape = {
    val bitSet = new java.util.BitSet(128)

    /**
      * ASCII 01-1F are HTTP control characters that need to be escaped.
      * \u000A and \u000D are \n and \r, respectively.
      */
    val clist = Array(
      '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
      '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
      '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
      '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
      '{', '[', ']', '^')

    clist.foreach(bitSet.set(_))

    if (Shell.WINDOWS) {
      Array(' ', '<', '>', '|').foreach(bitSet.set(_))
    }

    bitSet
  }

  private[this] def getColumnAsString(
      row: BaseRow,
      fieldIndex: Int,
      fieldType: DataType,
      timeZone: TimeZone): String = {

    if (row.isNullAt(fieldIndex)) {
      return null
    }

    fieldType.toInternalType match {
      case DataTypes.BYTE_ARRAY =>
        new String(row.getByteArray(fieldIndex), "UTF-8")
      case DataTypes.STRING =>
        row.getString(fieldIndex)
      case DataTypes.BYTE =>
        java.lang.Byte.toString(row.getByte(fieldIndex))
      case DataTypes.SHORT =>
        java.lang.Short.toString(row.getShort(fieldIndex))
      case DataTypes.INT =>
        java.lang.Integer.toString(row.getInt(fieldIndex))
      case DataTypes.LONG =>
        java.lang.Long.toString(row.getLong(fieldIndex))
      case DataTypes.FLOAT =>
        java.lang.Float.toString(row.getFloat(fieldIndex))
      case DataTypes.DOUBLE =>
        java.lang.Double.toString(row.getDouble(fieldIndex))
      case DataTypes.BOOLEAN =>
        java.lang.Boolean.toString(row.getBoolean(fieldIndex))
      case dt: DecimalType =>
        row.getDecimal(fieldIndex, dt.precision(), dt.scale()).toString
      case DataTypes.TIMESTAMP =>
        DateTimeFunctions.timestampToStringPrecision(
          row.getLong(fieldIndex), 3, timeZone)
      case DataTypes.DATE =>
        DateTimeUtils.unixDateToString(row.getInt(fieldIndex))
      case DataTypes.TIME =>
        DateTimeUtils.unixTimeToString(row.getInt(fieldIndex))
      case _ => throw new IllegalArgumentException("Unsupported data type: " + fieldType.toString)
    }
  }

  def getPartitionPath(row: BaseRow, partitionSchema: RowType, timeZone: TimeZone): String = {
    (partitionSchema.getFieldTypes zip partitionSchema.getFieldNames).zipWithIndex.map {
      case ((fieldType, fieldName), index) =>
        getPartitionDirStr(fieldName, getColumnAsString(row, index, fieldType, timeZone))
    }.mkString(Path.SEPARATOR)
  }

  def getPartitionDirStr(col: String, value: String): String = {
    val partitionString = if (value == null || value.isEmpty) {
      DEFAULT_PARTITION_NAME
    } else {
      escapePathName(value)
    }
    escapePathName(col) + "=" + partitionString
  }

  def needsEscaping(c: Char): Boolean = {
    c >= 0 && c < charToEscape.size() && charToEscape.get(c)
  }

  def escapePathName(path: String): String = {
    val builder = new StringBuilder()
    path.foreach { c =>
      if (needsEscaping(c)) {
        builder.append('%')
        builder.append(f"${c.asInstanceOf[Int]}%02X")
      } else {
        builder.append(c)
      }
    }

    builder.toString()
  }
}
