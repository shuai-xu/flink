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

package org.apache.flink.table.sources.parquet

import java.util

import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.ColumnarRow
import org.apache.flink.table.sources.vector.VectorizedColumnBatch
import org.apache.flink.table.types.{BaseRowType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

/**
  * Creates a TableSource to scan an Parquet table based
  * [[VectorizedColumnBatch]],
  * and return [[ColumnarRow]].
  */
class ParquetVectorizedColumnRowTableSource(
    filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean],
    enumerateNestedFiles: Boolean,
    numTimes: Int = 1,
    sourceName: String = "",
    uniqueKeySet: util.Set[util.Set[String]] = null)
  extends ParquetTableSource[ColumnarRow](
    filePath,
    fieldTypes,
    fieldNames,
    fieldNullables,
    enumerateNestedFiles) {

  def this(filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    enumerateNestedFiles: Boolean) = {
    this(
      filePath,
      fieldTypes,
      fieldNames,
      fieldTypes.map(!FlinkTypeFactory.isTimeIndicatorType(_)),
      enumerateNestedFiles)
  }

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment):
    DataStreamSource[ColumnarRow] = {
    val inputFormat = new VectorizedColumnRowInputParquetFormat(
      filePath, fieldTypes, fieldNames, limit)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    streamEnv.createInput(inputFormat, getPhysicalType,
      s"ParquetVectorizedColumnRowTableSource: ${filePath.getName}")
  }

  override def getReturnType: BaseRowType = new BaseRowType(
    classOf[ColumnarRow], this.fieldTypes, this.fieldNames)

  def getPhysicalType: BaseRowTypeInfo[ColumnarRow] =
    DataTypes.toTypeInfo(getReturnType).asInstanceOf[BaseRowTypeInfo[ColumnarRow]]

  override protected def createTableSource(
      fieldTypes: Array[InternalType],
      fieldNames: Array[String],
      fieldNullables: Array[Boolean]): ParquetTableSource[ColumnarRow] = {
    val tableSource = new ParquetVectorizedColumnRowTableSource(
      filePath, fieldTypes, fieldNames, fieldNullables, enumerateNestedFiles, numTimes, sourceName)
    tableSource.setFilterPredicate(filterPredicate)
    tableSource.setFilterPushedDown(filterPushedDown)
    tableSource.setLimit(limit)
    tableSource.setLimitPushedDown(limitPushedDown)
    tableSource
  }

  override def explainSource(): String = {
    val limitStr = if (isLimitPushedDown && limit < Long.MaxValue) s"; limit=$limit" else ""
    val predicate = if (filterPredicate == null) "" else filterPredicate.toString
    s"ParquetVectorizedColumnRowTableSource -> " +
      s"selectedFields=[${fieldNames.mkString(", ")}];" +
      s"filterPredicates=[$predicate]$limitStr"
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[ColumnarRow] = {
    val inputFormat = new VectorizedColumnRowInputParquetFormat(
      filePath, fieldTypes, fieldNames, limit)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    execEnv.createInput(inputFormat, getPhysicalType, numTimes, sourceName)
  }

  override def getUniqueKeys(): util.Set[util.Set[String]] = uniqueKeySet
}
