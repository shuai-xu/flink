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
import org.apache.flink.table.api.{RichTableSchema, TableProperties, TableSchema, TableSourceParser}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.Logging

/**
  * The utility class is used to convert ExternalCatalogTable to TableSinkTable.
  */
object ExternalTableUtil extends Logging {

  private def convertTableSchemaToRichTableSchema(
      tableSchema: TableSchema): RichTableSchema = {

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

  /**
   * Converts table source parser from the given ExternalCatalogTable.
   *
   * @param name the name of the table
   * @param table the [[ExternalCatalogTable]] instance which to convert
   * @param isStreaming Is in streaming mode or not
   * @return the extracted parser
   */
  def toParser(
      name: String, table: ExternalCatalogTable, isStreaming: Boolean): TableSourceParser = {
    ExternalTableSourceUtil.toTableSource(table) match {
      case Some(_: TableSource) =>
        null
      case None =>
        val tableFactory = FlinkTableFactory.INSTANCE
        val tableProperties = generateTableProperties(table, isStreaming)
        tableFactory.createParser(
          name, table.richTableSchema, tableProperties)
    }
  }

  /**
   * Converts an [[ExternalCatalogTable]] instance to a [[TableSource]] instance
   *
   * @param name the name of the table source
   * @param externalCatalogTable the [[ExternalCatalogTable]] instance which to convert
   * @param isStreaming is streaming source expected.
   * @param isDim is dimension table source epxected.
   * @return converted [[TableSource]] instance from the input catalog table
   */
  def toTableSource(
      name: String,
      externalCatalogTable: ExternalCatalogTable,
      isStreaming: Boolean,
      isDim: Boolean): TableSource = {
    ExternalTableSourceUtil.toTableSource(externalCatalogTable) match {
      case Some(tableSource: TableSource) =>
        tableSource
      case None =>
        val tableFactory = FlinkTableFactory.INSTANCE
        val tableProperties = generateTableProperties(externalCatalogTable, isStreaming)
        isDim match {
          case true =>
            tableFactory.createDimensionTableSource(
              name,
              externalCatalogTable.richTableSchema,
              tableProperties).asInstanceOf[TableSource]
          case false =>
            tableFactory.createTableSource(
              name,
              externalCatalogTable.richTableSchema,
              tableProperties)
        }
    }
  }

  /**
   * Converts an [[ExternalCatalogTable]] instance to a [[TableSink]] instance
   * @param name          name of the table
   * @param externalTable the [[ExternalCatalogTable]] instance to convert
   * @param isStreaming   is in streaming mode or not.
   * @return
   */
  def toTableSink(
      name: String,
      externalTable: ExternalCatalogTable,
      isStreaming: Boolean): TableSink[Any] = {

    val tableFactory = FlinkTableFactory.INSTANCE
    val tableProperties: TableProperties = generateTableProperties(externalTable, isStreaming)

    tableFactory.createTableSink(
      name,
      externalTable.richTableSchema,
      tableProperties).asInstanceOf[TableSink[Any]]
  }

  def generateTableProperties(
      externalTable: ExternalCatalogTable, isStream: Boolean): TableProperties = {

    val tableProperties = new TableProperties()
    tableProperties.addAll(externalTable.properties)
    isStream match {
      case true =>
        tableProperties.setString(
          TableProperties.BLINK_ENVIRONMENT_TYPE_KEY,
          TableProperties.BLINK_ENVIRONMENT_STREAM_VALUE)
      case false =>
        tableProperties.setString(
          TableProperties.BLINK_ENVIRONMENT_TYPE_KEY,
          TableProperties.BLINK_ENVIRONMENT_BATCHEXEC_VALUE)
    }
    tableProperties.setString("type", externalTable.tableType)
    tableProperties
  }
}
