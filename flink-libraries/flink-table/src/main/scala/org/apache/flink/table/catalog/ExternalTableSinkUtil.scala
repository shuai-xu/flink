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

package org.apache.flink.table.catalog

import org.apache.flink.factories.FlinkTableFactory
import org.apache.flink.table.api.{RichTableSchema, TableProperties}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.plan.schema.TableSinkTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.TableSink

/**
  * The utility class is used to convert ExternalCatalogTable to TableSinkTable.
  */
object ExternalTableSinkUtil {

  private def convertTableSchemaToRichTableSchema(tableSchema: TableSchema): RichTableSchema = {

    val colNames = tableSchema.getColumnNames
    val colTypes = tableSchema.getTypes
    val colNullables = tableSchema.getNullables
    val richTableSchema = new RichTableSchema(
      colNames, colTypes, colNullables)
    val primaryKeys = tableSchema.getPrimaryKeys
    richTableSchema.setPrimaryKey(primaryKeys: _*)
    // TODO unique keys of RichTableSchema
    // TODO indexes of RichTableSchema
    // TODO header fields of RichTableSchema
    richTableSchema
  }

  def convertExternalCatalogTableToSinkTable(
      tableName: String,
      externalTable: ExternalCatalogTable): TableSinkTable[_] = {

    val tableFactory = FlinkTableFactory.INSTANCE
    val tableProperties = new TableProperties()
    tableProperties.addAll(externalTable.properties)

    val tableSink = tableFactory.createTableSink(
      tableName,
      convertTableSchemaToRichTableSchema(externalTable.schema),
      tableProperties)
    new TableSinkTable(
      tableSink.asInstanceOf[TableSink[_]],
      new FlinkStatistic(Some(externalTable.stats)))
  }
}
