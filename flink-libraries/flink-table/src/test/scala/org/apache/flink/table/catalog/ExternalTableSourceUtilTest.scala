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

import java.util.{Collections => JCollections, Set => JSet}

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.plan.schema.StreamTableSourceTable
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.{Before, Test}

class ExternalTableSourceUtilTest {

  @Before
  def setUp(): Unit = {
    ExternalTableSourceUtil.injectTableSourceConverter("mock", classOf[MockTableSourceConverter])
  }

  @Test
  def testExternalStreamTable() = {
    val schema = new TableSchema(Array("foo"), Array(DataTypes.INT))
    val table = ExternalCatalogTable("mock", schema)
    val tableSource = ExternalTableSourceUtil.fromExternalCatalogTable(table)
    assertTrue(tableSource.isInstanceOf[StreamTableSourceTable[_]])
  }

  @Test
  def testNotExistedExternalTable() = {
    val schema = new TableSchema(Array("foo"), Array(DataTypes.INT))
    val table = ExternalCatalogTable("mock1", schema)
    val tableSource = ExternalTableSourceUtil.toTableSource(table)
    assertFalse(tableSource.isDefined)
  }

}

class MockTableSourceConverter extends TableSourceConverter[StreamTableSource[Row]] {
  override def requiredProperties: JSet[String] = JCollections.emptySet()

  override def fromExternalCatalogTable(externalCatalogTable: ExternalCatalogTable)
  : StreamTableSource[Row] = {
    new StreamTableSource[Row] {
      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] =
        throw new UnsupportedOperationException

      override def getReturnType: DataType = {
        val schema = externalCatalogTable.schema
        DataTypes.createRowType(
          schema.getTypes.asInstanceOf[Array[DataType]],
          schema.getColumnNames)
      }

      /** Returns the table schema of the table source */
      override def getTableSchema = TableSchema.fromDataType(getReturnType)
    }
  }
}
