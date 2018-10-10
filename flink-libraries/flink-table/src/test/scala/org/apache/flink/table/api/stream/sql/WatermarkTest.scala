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

package org.apache.flink.table.api.stream.sql

import org.apache.calcite.sql.{SqlCall, SqlNode, SqlOperator}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.WatermarkUtils
import org.apache.flink.table.util.TableTestBase
import org.junit.Test
import org.mockito.Mockito

class WatermarkTest extends TableTestBase {

  @Test
  def testWatermarkAssigner(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val mockSqlCall = Mockito.mock(classOf[SqlCall])
    val mockOperator = Mockito.mock(classOf[SqlOperator])
    Mockito.when(mockSqlCall.getOperator).thenReturn(mockOperator)
    Mockito.when(mockOperator.getName).thenReturn(WatermarkUtils.WITH_OFFSET_FUNC)
    val operandList = new java.util.LinkedList[SqlNode]()
    operandList.add(Mockito.mock(classOf[SqlNode]))
    operandList.add(Mockito.mock(classOf[SqlNode]))
    Mockito.when(operandList.get(0).toString).thenReturn("a")
    Mockito.when(operandList.get(1).toString).thenReturn("1")
    Mockito.when(mockSqlCall.getOperandList).thenReturn(operandList)
    util.tableEnv.registerTableWithWatermark(
      "test",
      table,
      "a",
      WatermarkUtils.getWithOffsetParameters("a", mockSqlCall))
    util.verifyPlan("select * from test")
  }

  @Test(expected = classOf[TableException])
  def testInvalidRowtimeType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val mockSqlCall = Mockito.mock(classOf[SqlCall])
    val mockOperator = Mockito.mock(classOf[SqlOperator])
    Mockito.when(mockSqlCall.getOperator).thenReturn(mockOperator)
    Mockito.when(mockOperator.getName).thenReturn(WatermarkUtils.WITH_OFFSET_FUNC)
    val operandList = new java.util.LinkedList[SqlNode]()
    operandList.add(Mockito.mock(classOf[SqlNode]))
    operandList.add(Mockito.mock(classOf[SqlNode]))
    Mockito.when(operandList.get(0).toString).thenReturn("b")
    Mockito.when(operandList.get(1).toString).thenReturn("1")
    Mockito.when(mockSqlCall.getOperandList).thenReturn(operandList)
    util.tableEnv.registerTableWithWatermark(
      "test",
      table,
      "a",
      WatermarkUtils.getWithOffsetParameters("a", mockSqlCall))
    util.verifyPlan("select * from test")
  }

  @Test(expected = classOf[TableException])
  def testInvalidFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val mockSqlCall = Mockito.mock(classOf[SqlCall])
    val mockOperator = Mockito.mock(classOf[SqlOperator])
    Mockito.when(mockSqlCall.getOperator).thenReturn(mockOperator)
    Mockito.when(mockOperator.getName).thenReturn(WatermarkUtils.WITH_OFFSET_FUNC + "_")
    val operandList = new java.util.LinkedList[SqlNode]()
    operandList.add(Mockito.mock(classOf[SqlNode]))
    operandList.add(Mockito.mock(classOf[SqlNode]))
    Mockito.when(operandList.get(0).toString).thenReturn("b")
    Mockito.when(operandList.get(1).toString).thenReturn("1")
    Mockito.when(mockSqlCall.getOperandList).thenReturn(operandList)
    util.tableEnv.registerTableWithWatermark(
      "test",
      table,
      "a",
      WatermarkUtils.getWithOffsetParameters("a", mockSqlCall))
    util.verifyPlan("select * from test")
  }

  @Test(expected = classOf[TableException])
  def testInvalidOffset(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val mockSqlCall = Mockito.mock(classOf[SqlCall])
    val mockOperator = Mockito.mock(classOf[SqlOperator])
    Mockito.when(mockSqlCall.getOperator).thenReturn(mockOperator)
    Mockito.when(mockOperator.getName).thenReturn(WatermarkUtils.WITH_OFFSET_FUNC)
    val operandList = new java.util.LinkedList[SqlNode]()
    operandList.add(Mockito.mock(classOf[SqlNode]))
    operandList.add(Mockito.mock(classOf[SqlNode]))
    Mockito.when(operandList.get(0).toString).thenReturn("a")
    Mockito.when(operandList.get(1).toString).thenReturn("1 a")
    Mockito.when(mockSqlCall.getOperandList).thenReturn(operandList)
    util.tableEnv.registerTableWithWatermark(
      "test",
      table,
      "a",
      WatermarkUtils.getWithOffsetParameters("a", mockSqlCall))
    util.verifyPlan("select * from test")
  }
}
