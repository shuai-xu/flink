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

package org.apache.flink.table.api.batch.sql.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.util.{MemoryTableSinkUtil, TableTestBatchExecBase}
import org.junit._

class InsertIntoValidationTest extends TableTestBatchExecBase {

  @Test(expected = classOf[ValidationException])
  def testInconsistentLengthInsert(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[DataType] = Array(DataTypes.INT, DataTypes.LONG)
    val sink = new MemoryTableSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"

    // must fail because table sink schema has too few fields
    util.tableEnv.sqlUpdate(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testUnmatchedTypesInsert(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes: Array[DataType] = Array(DataTypes.STRING, DataTypes.INT, DataTypes.LONG)
    val sink = new MemoryTableSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"

    // must fail because types of table sink do not match query result
    util.tableEnv.sqlUpdate(sql)
  }

  @Test
  def testPartialInsert(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = util.tableEnv.scan("sourceTable")
        .getSchema.getTypes.asInstanceOf[Array[DataType]]
    val sink = new MemoryTableSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

    val sql = "INSERT INTO targetTable (d, f) SELECT a, c FROM sourceTable"

    // must fail because partial insert is not supported yet.
    util.tableEnv.sqlUpdate(sql, util.tableEnv.queryConfig)
  }
}
