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

import java.util.Collections
import org.apache.flink.table.api.types.{RowType, DataType, DataTypes}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory}
import org.apache.flink.table.sinks.BatchTableSink
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.temptable.rpc.TableServiceClient
import org.apache.flink.table.util.TableProperties

class FlinkTableServiceFactory extends BatchTableSinkFactory[BaseRow]
  with BatchTableSourceFactory[BaseRow] {

  override def createBatchTableSink(
     properties: java.util.Map[String, String]): BatchTableSink[BaseRow] = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)
    val tableName = tableProperties.readTableNameFromProperties()
    val schema = tableProperties
      .readSchemaFromProperties(FlinkTableServiceFactory.getClass.getClassLoader)
    new FlinkTableServiceSink(
      FlinkTableServiceFactory.CLIENT_NAME,
      tableProperties,
      tableName,
      new RowType(
        classOf[BaseRow], schema.getColumnTypes.toArray[DataType], schema.getColumnNames, true)
    )
  }

  override def createBatchTableSource(
    properties: java.util.Map[String, String]): BatchTableSource[BaseRow] = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)
    val tableName = tableProperties.readTableNameFromProperties()
    val schema = tableProperties
      .readSchemaFromProperties(FlinkTableServiceFactory.getClass.getClassLoader)
    new FlinkTableServiceSource(
      FlinkTableServiceFactory.CLIENT_NAME,
      tableProperties,
      tableName,
      DataTypes.createBaseRowType(schema.getColumnTypes, schema.getColumnNames)
    )
  }

  override def requiredContext(): java.util.Map[String, String] =
    Collections.emptyMap()

  override def supportedProperties(): java.util.List[String] =
    Collections.emptyList()
}

object FlinkTableServiceFactory {
  val CLIENT_NAME = classOf[TableServiceClient].getCanonicalName
  val TABLE_SERVICE_READY_RETRY_TIMES: String = "flink.service.tableservice.ready.retry.times"
  val TABLE_SERVICE_READY_RETRY_BACKOFF_MS: String = "flink.service.tableservice.ready.backoff.ms"
  val TABLE_SERVICE_ID: String = "flink.service.tableservice.id"
  val TABLE_SERVICE_DEFAULT_READY_RETRYTIMES_VALUE: Int = 3
  val TABLE_SERVICE_DEFAULT_READY_GAP_MS_VALUE: Long = 10000L
}
