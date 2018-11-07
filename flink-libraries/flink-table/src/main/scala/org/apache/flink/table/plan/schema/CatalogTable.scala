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

package org.apache.flink.table.plan.schema

import java.util

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.catalog.{ExternalCatalogTable, ExternalTableUtil}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.schema.{ConfigurableTable, TemporalTable}
import org.apache.flink.table.api.TableSourceParser
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.{BatchTableSource, DimensionTableSource, StreamTableSource}

import scala.collection.mutable.ArrayBuffer

/**
 * CatalogTable represents an ExternalCatalogTable in Calcite.
 * 1. The isStreaming flag indicates the execution environment of the job which is used to
 * determine the schema of the table and which TableSink can be create from catalog table.
 * 2. CatalogTable will be transferred to a TableSource in TableScan and a TableSink in DML query
 * and a DimensionTableSource in TemporalTableScan by CatalogTableRules.
 * 3. The schema of CatalogTable can be different in different execution environment since there is
 * no TimeIndicator in batch table and dimension table.
 * 4. The computed columns of ExternalCatalogTable will registered as virtual columns in calcite
 * which cannot be update in dml query.
 *
 */
class CatalogTable(val name:String, val table: ExternalCatalogTable, val isStreaming: Boolean)
    extends FlinkTable with ConfigurableTable with TemporalTable {

  /**
   * Creates a copy of this table, changing statistic.
   *
   * @param statistic A new FlinkStatistic.
   * @return Copy of this table, substituting statistic.
   */
  override def copy(statistic: FlinkStatistic) = new CatalogTable(name, table, isStreaming)

  override def getRowType(typeFactory: RelDataTypeFactory) =
    typeFactory.asInstanceOf[FlinkTypeFactory].buildLogicalRowType(table.schema, isStreaming)

  override def config(dynamicParameters: util.Map[String, String]) = {
    val newProperties = new util.HashMap[String, String]()
    newProperties.putAll(table.properties)
    newProperties.putAll(dynamicParameters)
    val newTable = ExternalCatalogTable(
      table.tableType,
      table.schema,
      newProperties,
      table.richTableSchema,
      table.stats,
      table.comment,
      table.partitionColumnNames,
      table.isPartitioned,
      table.computedColumns,
      table.rowTimeField,
      table.watermarkOffset,
      table.createTime,
      table.lastAccessTime)
    new CatalogTable(name, newTable, isStreaming)
  }

  override def getStatistic(): FlinkStatistic = {
    val primaryKeys = table.schema.getPrimaryKeys
    val uniqueKeys = table.schema.getUniqueKeys
    if (primaryKeys.isEmpty && uniqueKeys.isEmpty) {
      FlinkStatistic.of(table.stats)
    } else {
      val keyBuffer = new ArrayBuffer[util.Set[String]]()
      if (!primaryKeys.isEmpty) {
        keyBuffer.append(ImmutableSet.copyOf(primaryKeys))
      }
      uniqueKeys.foreach {
        case uniqueKey: Array[String] => keyBuffer.append(ImmutableSet.copyOf(uniqueKey))
      }

      FlinkStatistic.of(table.stats, ImmutableSet.copyOf(keyBuffer.toArray), null)
    }
  }

  /**
   * Create table sink.
   * @return table sink
   */
  def tableSink: TableSink[Any] = {
    isStreaming match {
      case true => streamTableSink
      case false => batchTableSink
    }
  }

  /**
   * Create a stream table sink from table.
   * @return table sink
   */
  private def streamTableSink: TableSink[Any] =
    ExternalTableUtil.toTableSink(name, table, true)
        .asInstanceOf[TableSink[Any]]

  /**
   * Create a batch table sink from table.
   * @return table sink
   */
  private def batchTableSink: TableSink[Any] =
    ExternalTableUtil.toTableSink(name, table, false)
      .asInstanceOf[TableSink[Any]]

  /**
   * Create a streaming table source from external catalog table.
   * @return the stream table source
   */
  def streamTableSource: StreamTableSource[Any] =
    if (!isStreaming) {
      null
    } else {
      ExternalTableUtil.toTableSource(name, table, true, false) match {
        case t: StreamTableSource[Any] => t
        case _ => null
      }
    }

  /**
   * Create a table parser for external catalog table.
   * @return
   */
  def tableSourceParser: TableSourceParser = ExternalTableUtil.toParser(name, table, isStreaming)

  /**
   * Create a batch table source from external catalog table.
   * @return the batch table source
   */
  def batchTableSource: BatchTableSource[Any] =
    if (isStreaming) {
      null
    } else {
      ExternalTableUtil.toTableSource(name, table, false, false) match {
        case t: BatchTableSource[Any] => t
        case _ => null
      }
    }

  /**
   * Create a dimension table source from external catalog table.
   * @return the dimension table source
   */
  def dimTableSource: DimensionTableSource[Any] =
    ExternalTableUtil.toTableSource(name, table, isStreaming, true) match {
      case t: DimensionTableSource[Any] => t
      case _ => null
    }

  override def getSysStartFieldName: String = "sys_start"

  override def getSysEndFieldName: String = "sys_end"

}
