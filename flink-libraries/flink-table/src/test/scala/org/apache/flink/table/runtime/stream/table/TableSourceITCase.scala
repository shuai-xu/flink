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

package org.apache.flink.table.runtime.stream.table

import java.lang.{Integer => JInt, Long => JLong}
import java.sql.Timestamp

import org.apache.calcite.runtime.SqlFunctions.{internalToTimestamp => toTimestamp}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.runtime.utils.{CommonTestData, TestingAppendSink}
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.util.{TestFilterableTableSource, TestPartitionableTableSource, TestTableSourceWithNames, TestTableSourceWithTime}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TableSourceITCase extends AbstractTestBase {

  @Test
  def testCsvTableSource(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val sink = new TestingAppendSink

    tEnv.registerTableSource("csvTable", csvTable)
    tEnv.scan("csvTable")
      .where('id > 4)
      .select('last, 'score * 2)
      .toAppendStream[Row]
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "Williams,69.0",
      "Miller,13.56",
      "Smith,180.2",
      "Williams,4.68")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCsvTableSourceWithFilterable(): Unit = {
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val sink = new TestingAppendSink

    tEnv.registerTableSource(tableName, new TestFilterableTableSource)
    tEnv.scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeTableSource(): Unit = {
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "rtime", "amount"))

    tEnv.registerTableSource(tableName, new TestTableSourceWithTime(data, rowType, "rtime", null))

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProctimeTableSource(): Unit = {
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "rtime", "amount"))

    tEnv.registerTableSource(tableName, new TestTableSourceWithTime(data, rowType, null, "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .where('ptime.cast(DataTypes.LONG) > 0L)
      .select('name, 'amount)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,10",
      "Bob,20",
      "Mary,30",
      "Liz,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeProctimeTableSource(): Unit = {
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = Seq(
      Row.of("Mary", new JLong(1L), new JInt(10)),
      Row.of("Bob", new JLong(2L), new JInt(20)),
      Row.of("Mary", new JLong(2L), new JInt(30)),
      Row.of("Liz", new JLong(2001L), new JInt(40)))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.LONG, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "rtime", "amount"))

    tEnv.registerTableSource(
      tableName,
      new TestTableSourceWithTime(data, rowType, "rtime", "ptime"))

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeAsTimestampTableSource(): Unit = {
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = Seq(
      Row.of("Mary", toTimestamp(1L), new JInt(10)),
      Row.of("Bob", toTimestamp(2L), new JInt(20)),
      Row.of("Mary", toTimestamp(2L), new JInt(30)),
      Row.of("Liz", toTimestamp(2001L), new JInt(40)))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, Types.SQL_TIMESTAMP, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "rtime", "amount"))

    tEnv.registerTableSource(tableName, new TestTableSourceWithTime(data, rowType, "rtime", null))

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .window(Tumble over 1.second on 'rtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.start, 'amount.sum)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:00.0,40",
      "Bob,1970-01-01 00:00:00.0,20",
      "Liz,1970-01-01 00:00:02.0,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testPartitionableTableSourceWithPartitionFields(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val sink = new TestingAppendSink
    tEnv.scan("partitionable_table")
      .where('part === "2" || 'part === "1" && 'id > 2)
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList("3,John,2,part=1#part=2", "4,nosharp,2,part=1#part=2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionPruningRuleNotAppliedWithoutPartitionFields(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val sink = new TestingAppendSink
    tEnv.scan("partitionable_table")
      .where('name === "Lucy")
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList("6,Lucy,3,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDefinedFieldNames(): Unit = {
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // StringSink is using UTC.
    val data = Seq(
      ("Mary", new Timestamp(1L), new JInt(10)),
      ("Bob", new Timestamp(2L), new JInt(20)),
      ("Mary", new Timestamp(2L), new JInt(30)),
      ("Liz", new Timestamp(2001L), new JInt(40)))

    tEnv.registerTableSource(tableName, new TestTableSourceWithNames(
      data,
      Array("name", "amount", "rtime"),
      Array(0, 2, 1)))

    val sink = new TestingAppendSink
    tEnv.scan(tableName)
      .select('name, 'rtime, 'amount)
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Mary,1970-01-01 00:00:00.001,10",
      "Bob,1970-01-01 00:00:00.002,20",
      "Mary,1970-01-01 00:00:00.002,30",
      "Liz,1970-01-01 00:00:02.001,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
