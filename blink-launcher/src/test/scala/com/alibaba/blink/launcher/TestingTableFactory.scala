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
package com.alibaba.blink.launcher

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{RichTableSchema, TableFactory, TableProperties, TableSchema}
import org.apache.flink.table.sinks.{AppendStreamTableSink, TableSink, TableSinkBase}
import org.apache.flink.table.sources.{DimensionTableSource, StreamTableSource, TableSource}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

class TestingTableFactory extends TableFactory {

  override def createTableSource(
    tableName: String,
    schema: RichTableSchema,
    properties: TableProperties): TableSource = {

    TestingTableSource(tableName, schema, properties)
  }

  override def createDimensionTableSource(
    tableName: String,
    schema: RichTableSchema,
    properties: TableProperties): DimensionTableSource[_] = ???

  override def createTableSink(
    tableName: String,
    schema: RichTableSchema,
    properties: TableProperties): TableSink[_] = {
    TestingTableSink(tableName, schema, properties)
  }
}

case class TestingTableSource(
    tableName: String,
    schema: RichTableSchema,
    properties: TableProperties) extends StreamTableSource[Row] {
  override def getReturnType: DataType = schema.getResultRowType

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  /** Returns the table schema of the table source */
  override def getTableSchema = TableSchema.fromDataType(getReturnType)

  override def explainSource(): String = ""
}

case class TestingTableSink(
  tableName: String,
  schema: RichTableSchema,
  properties: TableProperties) extends AppendStreamTableSink[Row] with TableSinkBase[Row] {
  override def getOutputType: DataType = schema.getResultRowType

  override def getFieldNames: Array[String] = schema.getColumnNames

  override def getFieldTypes: Array[DataType] = schema.getColumnTypes.asInstanceOf[Array[DataType]]

  override def emitDataStream(dataStream: DataStream[Row]): Unit = ???

  override protected def copy: TableSinkBase[Row] = TestingTableSink(tableName, schema, properties)
}
