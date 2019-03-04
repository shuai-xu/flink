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

package org.apache.flink.table.temptable

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableSchema}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
import org.apache.flink.table.temptable.io.TableServiceSourceFunction
import org.apache.flink.table.temptable.util.TableServiceUtil
import org.apache.flink.table.types.{DataType, RowType, TypeConverters}
import org.apache.flink.table.util.{TableProperties, TableSchemaUtil}

class FlinkTableServiceSource(
  tableEnv: TableEnvironment,
  tableProperties: TableProperties,
  tableName: String,
  resultType: RowType) extends BatchTableSource[BaseRow] with StreamTableSource[BaseRow] {

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    TableServiceUtil.injectTableServiceInstances(
      tableEnv.tableServiceManager.getTableServiceInstance(), tableProperties)
    streamEnv.addSource(
      new TableServiceSourceFunction(
        tableProperties,
        tableName,
        resultType)
    ).returns(
      TypeConverters.toBaseRowTypeInfo(resultType)
    )
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    TableServiceUtil.injectTableServiceInstances(
      tableEnv.tableServiceManager.getTableServiceInstance(), tableProperties)
    execEnv.addSource(new TableServiceSourceFunction(
      tableProperties,
      tableName,
      resultType)
    ).returns(
      TypeConverters.toBaseRowTypeInfo(resultType)
    )
  }

  override def getReturnType: DataType = resultType

  override def getTableSchema: TableSchema = TableSchemaUtil.fromDataType(resultType)
}
