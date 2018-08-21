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

package org.apache.flink.table.api

import com.alibaba.blink.errcode.ErrorFactory
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.util.MemoryTableSinkUtil.UnsafeMemoryAppendTableSink
import org.apache.flink.table.util.{MockTableEnvironment, TableTestBase}
import org.junit.Test

class TableEnvironmentTest extends TableTestBase {

  val tEnv = new MockTableEnvironment

  @Test
  def testInsertInto(): Unit = {
    val util = streamTestUtil()

    val t: Table = util.addTable[(Int, Double)]("table_1", 'id, 'text)

    val sinkFieldNames = Array("a", "b")

    val sinkFieldTypes: Array[DataType] = Array(DataTypes.INT, DataTypes.LONG)

    util.tableEnv.registerTableSink("sink_1", sinkFieldNames,
                  sinkFieldTypes, new UnsafeMemoryAppendTableSink)

    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      ErrorFactory.prettyPrint(
        "Insert into: Query result and target table 'sink_1' field type(s) not match."))
    t.insertInto("sink_1")
  }

  @Test
  def testSqlUpdate(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Double, String)]("MyTable", 'a, 'b, 'c, 'proctime.proctime)
    val sinkFieldNames = Array("a", "proctime", "c")
    val sinkFieldTypes: Array[DataType] = Array(
      DataTypes.INT,
      DataTypes.TIMESTAMP,
      DataTypes.STRING)
    util.tableEnv.getConfig.setSubsectionOptimization(true)
    util.tableEnv.registerTableSink(
      "sink_1",
      sinkFieldNames,
      sinkFieldTypes,
      new UnsafeMemoryAppendTableSink)

    val sql = "INSERT INTO sink_1 SELECT a, proctime, c FROM MyTable ORDER BY proctime, c"

    util.tableEnv.sqlUpdate(sql)
    val node = util.tableEnv.sinkNodes.head
    val resultTable = new Table(util.tableEnv, node.children.head)

    util.verifyPlan(resultTable)
  }
}

case class CClass(cf1: Int, cf2: String, cf3: Double)

class PojoClass(var pf1: Int, var pf2: String, var pf3: Double) {
  def this() = this(0, "", 0.0)
}
