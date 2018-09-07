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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.utils.{StreamTestData, TestingAppendBaseRowSink, TestingAppendRowSink, TestingAppendSink}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BaseRowUtil
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class CalcITCase {

  @Test
  def testGenericRowAndBaseRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRow = new GenericRow(3)
    rowData.setInt(0, 1)
    rowData.setInt(1, 1)
    rowData.setLong(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRow] =
      new BaseRowTypeInfo(
        classOf[GenericRow],
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO).asInstanceOf[TypeInformation[GenericRow]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val outputType = new BaseRowTypeInfo(
      classOf[BaseRow],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[BaseRow]
    val sink = new TestingAppendBaseRowSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("0|1,1,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowAndBaseRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlQuery = "SELECT * FROM MyTableRow WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val outputType = new BaseRowTypeInfo(
      classOf[BaseRow],
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[BaseRow]
    val sink = new TestingAppendBaseRowSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("0|Hello,Worlds,1","0|Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGenericRowAndRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRow = new GenericRow(3)
    rowData.setInt(0, 1)
    rowData.setInt(1, 1)
    rowData.setLong(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRow] =
      new BaseRowTypeInfo(
        classOf[GenericRow],
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO).asInstanceOf[TypeInformation[GenericRow]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,1,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowAndRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlQuery = "SELECT * FROM MyTableRow WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Hello,Worlds,1","Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMapType(): Unit = {
    def testPrimitiveType(): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tEnv = TableEnvironment.getTableEnvironment(env)

      val sqlQuery = "SELECT MAP[b, 30, 10, a] FROM MyTableRow"

      val t = env.fromCollection(StreamTestData.getSmall3TupleData)
        .toTable(tEnv).as('a, 'b, 'c)
      tEnv.registerTable("MyTableRow", t)

      val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      val sink = new TestingAppendSink
      result.addSink(sink)
      env.execute()

      val expected = List(
        "{1=30, 10=1}",
        "{2=30, 10=2}",
        "{2=30, 10=3}")
      assertEquals(expected.sorted, sink.getAppendResults.sorted)
    }

    def testNonPrimitiveType(): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val tEnv = TableEnvironment.getTableEnvironment(env)

      val sqlQuery = "SELECT MAP[a, c] FROM MyTableRow"

      val t = env.fromCollection(StreamTestData.getSmall3TupleData)
        .toTable(tEnv).as('a, 'b, 'c)
      tEnv.registerTable("MyTableRow", t)

      val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      val sink = new TestingAppendSink
      result.addSink(sink)
      env.execute()

      val expected = List(
        "{1=Hi}",
        "{2=Hello}",
        "{3=Hello world}")
      assertEquals(expected.sorted, sink.getAppendResults.sorted)
    }

    testPrimitiveType()
    testNonPrimitiveType()
  }

  def baseRowToGenericRowResults(
      testBaseRowResults: mutable.MutableList[BaseRow],
      rowTypeInfo: BaseRowTypeInfo[_]): mutable.MutableList[BaseRow] = {
    val config = new ExecutionConfig
    val fieldTypes = rowTypeInfo.getFieldTypes
    val fieldSerializers = fieldTypes.map(_.createSerializer(config))
    testBaseRowResults.map { r =>
      BaseRowUtil.toGenericRow(r, fieldTypes, fieldSerializers)
    }
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {

    val sqlQuery = "SELECT * FROM MyTable"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val table = tEnv.fromDataStream(env.fromCollection(Seq(
      ((0, 0), "0"),
      ((1, 1), "1"),
      ((2, 2), "2")
    )))
    tEnv.registerTable("MyTable", table)

    val result = tEnv.sqlQuery(sqlQuery)

    val sink = new TestingAppendRowSink
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    sink.localResults.zipWithIndex.foreach {
      case (row, i) =>
        val baseRow = row.getField(0).asInstanceOf[BaseRow]
        assertEquals(i, baseRow.getInt(0))
        assertEquals(i, baseRow.getInt(1))
        assertEquals(i.toString, row.getField(1))
    }
  }
}
