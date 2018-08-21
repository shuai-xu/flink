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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.utils.TestingAppendSink
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class CepITCase {

  @Test
  def testSimpleCEP() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
        |SELECT T.aid, T.bid, T.cid
        |FROM MyTable
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    A.id AS aid,
        |    B.id AS bid,
        |    C.id AS cid
        |  PATTERN (A B C)
        |  DEFINE
        |    A AS A.name = 'a',
        |    B AS B.name = 'b',
        |    C AS C.name = 'c'
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("6,7,8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllRowsPerMatch() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM MyTable
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    A.id AS aid,
        |    B.id AS bid,
        |    C.id AS cid
        |  ALL ROWS PER MATCH
        |  PATTERN (A B C)
        |  DEFINE
        |    A AS A.name = 'a',
        |    B AS B.name = 'b',
        |    C AS C.name = 'c'
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("6,a,6,null,null", "7,b,6,7,null", "8,c,6,7,8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFinalFirst() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 15, 4))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 24, 6))
    data.+=(("ACME", 7L, 25, 7))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    FIRST(DOWN.tstamp) AS bottom_tstamp,
        |    FIRST(UP.tstamp) AS end_tstamp,
        |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
        |    FIRST(UP.price + UP.tax) AS end_total
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,3,4,17,19", "2,3,4,17,19", "2,3,4,17,19", "2,3,4,17,19")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFinalLast() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 3))
    data.+=(("ACME", 4L, 15, 4))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 24, 6))
    data.+=(("ACME", 7L, 25, 7))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    LAST(DOWN.tstamp) AS bottom_tstamp,
        |    LAST(UP.tstamp) AS end_tstamp,
        |    LAST(DOWN.price + DOWN.tax) AS bottom_total,
        |    LAST(UP.price + UP.tax + 1) AS end_total
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,3,4,16,20", "2,3,5,16,26", "2,3,6,16,31", "2,3,7,16,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPrev() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = new mutable.MutableList[(String, Long, Int)]
    data.+=(("ACME", 1L, 12))
    data.+=(("ACME", 2L, 17))
    data.+=(("ACME", 3L, 13))
    data.+=(("ACME", 4L, 11))
    data.+=(("ACME", 5L, 14))
    data.+=(("ACME", 6L, 12))
    data.+=(("ACME", 7L, 13))
    data.+=(("ACME", 8L, 19))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    LAST(DOWN.tstamp) AS up_days,
        |    LAST(UP.tstamp) AS total_days
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price, 2)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,4,5", "2,4,6", "3,4,5", "3,4,6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRunningFirst() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 4))
    data.+=(("ACME", 4L, 11, 3))
    data.+=(("ACME", 5L, 20, 5))
    data.+=(("ACME", 6L, 24, 4))
    data.+=(("ACME", 7L, 25, 3))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    LAST(DOWN.tstamp) AS bottom_tstamp,
        |    LAST(UP.tstamp) AS end_tstamp
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price) AND UP.tax > FIRST(DOWN.tax)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,4,5", "3,4,5", "3,4,6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRunningLast() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(String, Long, Int, Int)]
    data.+=(("ACME", 1L, 12, 1))
    data.+=(("ACME", 2L, 17, 2))
    data.+=(("ACME", 3L, 13, 4))
    data.+=(("ACME", 4L, 11, 3))
    data.+=(("ACME", 5L, 20, 4))
    data.+=(("ACME", 6L, 24, 4))
    data.+=(("ACME", 7L, 25, 3))
    data.+=(("ACME", 8L, 19, 8))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'tax)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    LAST(DOWN.tstamp) AS bottom_tstamp,
        |    LAST(UP.tstamp) AS end_tstamp
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price) AND UP.tax > LAST(DOWN.tax)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,4,5", "2,4,6", "3,4,5", "3,4,6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionByOrderByEventTime() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3L, ("ACME", 17, 2))))
    data.+=(Left((1L, ("ACME", 12, 1))))
    data.+=(Left((2L, ("BCME", 12, 1))))
    data.+=(Left((4L, ("BCME", 17, 2))))
    data.+=(Right(4L))
    data.+=(Left((5L, ("ACME", 13, 3))))
    data.+=(Left((7L, ("ACME", 15, 4))))
    data.+=(Left((8L, ("BCME", 15, 4))))
    data.+=(Left((6L, ("BCME", 13, 3))))
    data.+=(Right(8L))
    data.+=(Left((10L, ("BCME", 20, 5))))
    data.+=(Left((9L, ("ACME", 20, 5))))
    data.+=(Right(13L))
    data.+=(Left((15L, ("ACME", 19, 8))))
    data.+=(Left((16L, ("BCME", 19, 8))))
    data.+=(Right(16L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)

    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  PARTITION BY symbol
        |  ORDER BY tstamp
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    FIRST(DOWN.tstamp) AS bottom_tstamp,
        |    FIRST(UP.tstamp) AS end_tstamp,
        |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
        |    FIRST(UP.price + UP.tax) AS end_total
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price) AND
        |      DATEDIFF(DOWN.tstamp, '1970-01-01 00:00:00') >= 0,
        |    UP AS UP.price > PREV(UP.price)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:00.003,1970-01-01 00:00:00.005,1970-01-01 00:00:00.007,17,19",
      "ACME,1970-01-01 00:00:00.003,1970-01-01 00:00:00.005,1970-01-01 00:00:00.007,17,19",
      "BCME,1970-01-01 00:00:00.004,1970-01-01 00:00:00.006,1970-01-01 00:00:00.008,17,19",
      "BCME,1970-01-01 00:00:00.004,1970-01-01 00:00:00.006,1970-01-01 00:00:00.008,17,19")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartitionByOrderByEventTimeAllRowsPerMatch() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3L, ("ACME", 17, 2))))
    data.+=(Left((1L, ("ACME", 12, 1))))
    data.+=(Left((2L, ("BCME", 12, 1))))
    data.+=(Left((4L, ("BCME", 17, 2))))
    data.+=(Right(4L))
    data.+=(Left((5L, ("ACME", 13, 3))))
    data.+=(Left((7L, ("ACME", 15, 4))))
    data.+=(Left((8L, ("BCME", 15, 4))))
    data.+=(Left((6L, ("BCME", 13, 3))))
    data.+=(Right(8L))
    data.+=(Left((10L, ("BCME", 20, 5))))
    data.+=(Left((9L, ("ACME", 20, 5))))
    data.+=(Right(13L))
    data.+=(Left((15L, ("ACME", 19, 8))))
    data.+=(Left((16L, ("BCME", 19, 8))))
    data.+=(Right(16L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)

    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    FIRST(UP.price + UP.tax) AS end_total
         |  ALL ROWS PER MATCH
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("ACME,1970-01-01 00:00:00.003,17,2,null,null",
                        "ACME,1970-01-01 00:00:00.003,17,2,null,null",
                        "ACME,1970-01-01 00:00:00.005,13,3,17,null",
                        "ACME,1970-01-01 00:00:00.005,13,3,17,null",
                        "ACME,1970-01-01 00:00:00.007,15,4,17,19",
                        "ACME,1970-01-01 00:00:00.007,15,4,17,19",
                        "ACME,1970-01-01 00:00:00.009,20,5,17,19",
                        "BCME,1970-01-01 00:00:00.004,17,2,null,null",
                        "BCME,1970-01-01 00:00:00.004,17,2,null,null",
                        "BCME,1970-01-01 00:00:00.006,13,3,17,null",
                        "BCME,1970-01-01 00:00:00.006,13,3,17,null",
                        "BCME,1970-01-01 00:00:00.008,15,4,17,19",
                        "BCME,1970-01-01 00:00:00.008,15,4,17,19",
                        "BCME,1970-01-01 00:00:00.01,20,5,17,19")
     assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllRowsPerMatchWithAggregateInMeasures() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(String, Long, Int)]
    data.+=(("ACME", 1L, 12))
    data.+=(("ACME", 2L, 17))
    data.+=(("ACME", 3L, 13))
    data.+=(("ACME", 4L, 15))
    data.+=(("ACME", 5L, 20))
    data.+=(("ACME", 6L, 19))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    FINAL COUNT(UP.tstamp) AS up_days,
        |    FINAL COUNT(tstamp) AS total_days,
        |    MAX(price) AS max_price,
        |    MIN(price) AS min_price,
        |    AVG(price) AS avg_price,
        |    RUNNING COUNT(tstamp) AS cnt_days,
        |    price - STRT.price AS price_dif
        |  ALL ROWS PER MATCH
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("ACME,2,17,2,1,3,17,17,17.0,1,0", "ACME,3,13,2,1,3,17,13,15.0,2,-4",
                        "ACME,4,15,2,1,3,17,13,15.0,3,-2", "ACME,2,17,2,2,4,17,17,17.0,1,0",
                        "ACME,3,13,2,2,4,17,13,15.0,2,-4", "ACME,4,15,2,2,4,17,13,15.0,3,-2",
                        "ACME,5,20,2,2,4,20,13,16.25,4,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAggregateInMeasuresAndDefine() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(String, Long, Int)]
    data.+=(("ACME", 1L, 12))
    data.+=(("ACME", 2L, 17))
    data.+=(("ACME", 3L, 13))
    data.+=(("ACME", 4L, 11))
    data.+=(("ACME", 5L, 20))
    data.+=(("ACME", 6L, 15))
    data.+=(("ACME", 7L, 10))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    LAST(UP.tstamp) AS end_tstamp,
        |    AVG(DOWN.price) AS down_avg_price
        |  PATTERN (STRT DOWN+ UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > AVG(DOWN.price)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,5,12.0", "2,6,12.0", "3,5,11.0", "3,6,11.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testStringAggregateInMeasuresAndDefine() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(String, Long, Int, String)]
    data.+=(("ACME", 1L, 12, "a"))
    data.+=(("ACME", 2L, 17, "b"))
    data.+=(("ACME", 3L, 13, "c"))
    data.+=(("ACME", 4L, 11, "d"))
    data.+=(("ACME", 5L, 20, "e"))
    data.+=(("ACME", 6L, 15, "f"))
    data.+=(("ACME", 7L, 10, "g"))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price, 'name)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    LAST(UP.tstamp) AS end_tstamp,
         |    MAX(DOWN.name) AS concat_name
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > AVG(DOWN.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,5,d", "2,6,d", "3,5,d", "3,6,d")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testClassifierWithAllRowsPerMatch() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM MyTable
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    CLASSIFIER() AS cls
        |  ALL ROWS PER MATCH
        |  PATTERN (A B C)
        |  DEFINE
        |    A AS A.name = 'a',
        |    B AS B.name = 'b',
        |    C AS C.name = 'c'
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("6,a,A", "7,b,B", "8,c,C")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testClassifierWithOneRowPerMatch() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(String, Long, Int)]
    data.+=(("ACME", 1L, 12))
    data.+=(("ACME", 2L, 17))
    data.+=(("ACME", 3L, 13))
    data.+=(("ACME", 4L, 21))
    data.+=(("ACME", 5L, 22))
    data.+=(("ACME", 6L, 19))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    LAST(UP.tstamp) AS end_tstamp,
        |    CLASSIFIER() AS cls
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN?? UP+)
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS CASE
        |            WHEN PREV(CLASSIFIER()) = 'STRT'
        |              THEN UP.price > 15
        |            ELSE
        |              UP.price > 20
        |          END
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,2,UP", "2,4,UP", "2,5,UP", "3,4,UP", "3,5,UP", "4,5,UP", "5,6,UP")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWithinEventTime() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2))))
    data.+=(Left((1000L, ("ACME", 12, 1))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3))))
    data.+=(Left((7000L, ("ACME", 15, 4))))
    data.+=(Right(8000L))
    data.+=(Left((9000L, ("ACME", 20, 5))))
    data.+=(Right(13000L))
    data.+=(Left((15000L, ("ACME", 19, 8))))
    data.+=(Right(16000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
        |SELECT *
        |FROM Ticker
        |MATCH_RECOGNIZE (
        |  PARTITION BY symbol
        |  ORDER BY tstamp
        |  MEASURES
        |    STRT.tstamp AS start_tstamp,
        |    FIRST(DOWN.tstamp) AS bottom_tstamp,
        |    FIRST(UP.tstamp) AS end_tstamp,
        |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
        |    FIRST(UP.price + UP.tax) AS end_total
        |  ONE ROW PER MATCH
        |  PATTERN (STRT DOWN+ UP+) within interval '5' second
        |  DEFINE
        |    DOWN AS DOWN.price < PREV(DOWN.price),
        |    UP AS UP.price > PREV(UP.price)
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0,1970-01-01 00:00:07.0,17,19")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDynamicWithinEventTime() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int, Long)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2, 5000L))))
    data.+=(Left((1000L, ("ACME", 12, 1, 5000L))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3, 5000L))))
    data.+=(Left((7000L, ("ACME", 15, 4, 5000L))))
    data.+=(Right(8000L))
    data.+=(Left((9000L, ("ACME", 20, 5, 5000L))))
    data.+=(Right(13000L))
    data.+=(Left((15000L, ("ACME", 19, 8, 5000L))))
    data.+=(Right(16000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int, Long)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'windowTime, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    FIRST(DOWN.tstamp) AS bottom_tstamp,
         |    FIRST(UP.tstamp) AS end_tstamp,
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    FIRST(UP.price + UP.tax) AS end_total
         |  ONE ROW PER MATCH
         |  PATTERN (STRT DOWN+ UP+) within interval STRT.windowTime
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0,1970-01-01 00:00:07.0,17,19")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFolloweBy() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
        |SELECT T.aid, T.bid, T.cid
        |FROM MyTable
        |MATCH_RECOGNIZE (
        |  MEASURES
        |    A.id AS aid,
        |    B.id AS bid,
        |    C.id AS cid
        |  PATTERN (A -> B C)
        |  DEFINE
        |    A AS A.name = 'a',
        |    B AS B.name = 'b',
        |    C AS C.name = 'c'
        |) AS T
        |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,3,4","6,7,8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testOneRowPerMatchTimeout() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2))))
    data.+=(Left((1000L, ("ACME", 12, 1))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3))))
    data.+=(Left((7000L, ("ACME", 15, 4))))
    data.+=(Right(8000L))
    data.+=(Left((9000L, ("ACME", 20, 5))))
    data.+=(Right(13000L))
    data.+=(Left((15000L, ("ACME", 19, 8))))
    data.+=(Right(16000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    FIRST(DOWN.tstamp) AS bottom_tstamp,
         |    FIRST(UP.tstamp) AS end_tstamp,
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    UP.price + UP.tax AS end_total
         |  ONE ROW PER MATCH WITH TIMEOUT ROWS
         |  PATTERN (STRT DOWN+ UP) within interval '5' second
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0,1970-01-01 00:00:07.0,17,19",
      "ACME,1970-01-01 00:00:09.0,null,null,null,null",
      "ACME,1970-01-01 00:00:15.0,null,null,null,null"
    )

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllRowsPerMatchTimeout() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2))))
    data.+=(Left((1000L, ("ACME", 12, 1))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3))))
    data.+=(Left((7000L, ("ACME", 15, 4))))
    data.+=(Right(8000L))
    data.+=(Left((9000L, ("ACME", 20, 5))))
    data.+=(Right(13000L))
    data.+=(Left((15000L, ("ACME", 19, 8))))
    data.+=(Right(16000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    FIRST(DOWN.tstamp) AS bottom_tstamp,
         |    FIRST(UP.tstamp) AS end_tstamp,
         |    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,
         |    UP.price + UP.tax AS end_total
         |  ALL ROWS PER MATCH WITH TIMEOUT ROWS
         |  PATTERN (STRT DOWN+ UP) within interval '5' second
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,17,2,1970-01-01 00:00:03.0,null,null,null,null",
      "ACME,1970-01-01 00:00:05.0,13,3,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0," +
        "null,17,null",
      "ACME,1970-01-01 00:00:07.0,15,4,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0," +
        "1970-01-01 00:00:07.0,17,19",
      "ACME,1970-01-01 00:00:09.0,20,5,1970-01-01 00:00:09.0,null,null,null,null",
      "ACME,1970-01-01 00:00:15.0,19,8,1970-01-01 00:00:15.0,null,null,null,null"
    )

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGreedy() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "b"))
    data.+=((9, "c"))
    data.+=((10, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bid, T.cid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.id AS aid,
         |    last(B.id) AS bid,
         |    C.id AS cid
         |  PATTERN (A B+ -> C)
         |  DEFINE
         |    A AS A.name = 'a',
         |    B AS B.name = 'b',
         |    C AS C.name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("6,8,9")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGreedy2() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "b"))
    data.+=((9, "c"))
    data.+=((10, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bid, T.cid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.id AS aid,
         |    last(B.id) AS bid,
         |    C.id AS cid
         |  PATTERN (A B+? -> C)
         |  DEFINE
         |    A AS A.name = 'a',
         |    B AS B.name = 'b',
         |    C AS C.name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("6,7,9", "6,8,9")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAfterMatchSkipPastLastRow() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "a"))
    data.+=((3, "a"))
    data.+=((4, "a"))
    data.+=((5, "a"))
    data.+=((6, "a"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.id AS aid
         |  AFTER MATCH SKIP PAST LAST ROW
         |  PATTERN (A{3})
         |  DEFINE
         |    A AS A.name = 'a'
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("3", "6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAfterMatchSkipToFirst() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "a"))
    data.+=((3, "a"))
    data.+=((4, "a"))
    data.+=((5, "a"))
    data.+=((6, "a"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.id AS aid,
         |    last(B.id) AS bid
         |  AFTER MATCH SKIP TO FIRST B
         |  PATTERN (A{2}? -> B{2}?)
         |  DEFINE
         |    A AS A.name = 'a',
         |    B AS B.name = 'a'
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("2,4", "4,6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEmit() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3))))
    data.+=(Left((7000L, ("ACME", 15, 4))))
    data.+=(Left((9000L, ("ACME", 20, 5))))
    data.+=(Right(17000L))
    data.+=(Left((18000L, ("ACME", 19, 8))))
    data.+=(Right(19000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    DOWN.tstamp AS bottom_tstamp,
         |    UP.tstamp AS end_tstamp
         |  ONE ROW PER MATCH WITH TIMEOUT ROWS
         |  PATTERN (STRT DOWN UP) WITHIN INTERVAL '15' SECOND
         |  EMIT TIMEOUT (INTERVAL '5' SECOND, INTERVAL '10' SECOND)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0,1970-01-01 00:00:07.0",
      "ACME,1970-01-01 00:00:09.0,1970-01-01 00:00:18.0,null",
      "ACME,1970-01-01 00:00:09.0,1970-01-01 00:00:18.0,null",
      "ACME,1970-01-01 00:00:09.0,null,null",
      "ACME,1970-01-01 00:00:18.0,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEmit2() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val data = new mutable.MutableList[Either[(Long, (String, Int, Int)), Long]]
    data.+=(Left((3000L, ("ACME", 17, 2))))
    data.+=(Right(4000L))
    data.+=(Left((5000L, ("ACME", 13, 3))))
    data.+=(Left((7000L, ("ACME", 15, 4))))
    data.+=(Left((9000L, ("ACME", 20, 5))))
    data.+=(Right(17000L))
    data.+=(Left((18000L, ("ACME", 19, 8))))
    data.+=(Right(19000L))

    val t = env.addSource(new EventTimeSourceFunction[(String, Int, Int)](data))
      .toTable(tEnv, 'symbol, 'price, 'tax, 'tstamp.rowtime)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY tstamp
         |  MEASURES
         |    STRT.tstamp AS start_tstamp,
         |    DOWN.tstamp AS bottom_tstamp,
         |    UP.tstamp AS end_tstamp
         |  ONE ROW PER MATCH WITH TIMEOUT ROWS
         |  PATTERN (STRT DOWN UP) WITHIN INTERVAL '15' SECOND
         |  EMIT TIMEOUT EVERY INTERVAL '5' SECOND
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) AS T
         |""".stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "ACME,1970-01-01 00:00:03.0,1970-01-01 00:00:05.0,1970-01-01 00:00:07.0",
      "ACME,1970-01-01 00:00:09.0,1970-01-01 00:00:18.0,null",
      "ACME,1970-01-01 00:00:09.0,1970-01-01 00:00:18.0,null",
      "ACME,1970-01-01 00:00:09.0,null,null",
      "ACME,1970-01-01 00:00:18.0,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
