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

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.{Column, ComputedColumn, TableSchema, Watermark}
import org.apache.flink.table.types.{DataTypes, IntType, LongType, StringType}
import org.apache.flink.table.sinks.parquet.ParquetTableSink
import org.apache.flink.table.sinks.UpdateDeleteTableSink
import org.apache.flink.table.sources.parquet.ParquetVectorizedColumnRowTableSource
import org.apache.flink.table.util.TableTestBase

import org.junit.{Before, Test}

/**
  * Test for delete sql.
  */
class DeleteSqlTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    val columns = Array[Column](
      new Column("a", DataTypes.INT, false),
      new Column("b", DataTypes.LONG, false),
      new Column("c", DataTypes.STRING, false),
      new Column("d", DataTypes.STRING, false)
    )

    val tableSchema = new TableSchema(columns,
      Array[String]("b", "c"),
      Array[Array[String]](),
      Array[Array[String]](),
      Array[ComputedColumn](),
      Array[Watermark]())
    val tableSource = new ParquetVectorizedColumnRowTableSource(new Path("/tmp/source"),
      tableSchema.getFieldTypes,
      tableSchema.getFieldNames,
      tableSchema.getFieldNullables,
      true)
    util.tableEnv.registerTableSource("SmallTable", tableSource)
    val tableSink = new TestDeleteBatchTableSink()
    tableSink.setPrimaryKeys(Array("c", "b"))
    util.tableEnv.registerTableSink("SmallTable",
      tableSchema.getFieldNames,
      Array(IntType.INSTANCE,
        LongType.INSTANCE,
        StringType.INSTANCE,
        StringType.INSTANCE), tableSink)
  }

  @Test
  def testDeleteSql(): Unit = {
    val sqlQuery = "DELETE FROM SmallTable WHERE a > 10"
    util.tableEnv.sqlUpdate(sqlQuery)
    util.verifyExplain()
  }

  @Test
  def testSubQueryDeleteSql(): Unit = {
    val sqlQuery = "DELETE FROM SmallTable WHERE a > (Select max(b) From SmallTable)"
    util.tableEnv.sqlUpdate(sqlQuery)
    util.verifyExplain()
  }
}

/**
  * Test for delete sql.
  */
class TestDeleteBatchTableSink extends ParquetTableSink("/tmp") with UpdateDeleteTableSink {

  override protected def copy: TestDeleteBatchTableSink = {
    val sink = new TestDeleteBatchTableSink
    sink.setPrimaryKeys(primaryKeys)
    sink
  }
}
