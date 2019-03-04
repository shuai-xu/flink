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

import java.math.BigDecimal
import java.util.TimeZone

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.TimeTestUtil.{EventTimeSourceFunction, TimestampAndWatermarkWithOffset}
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamingWithMiniBatchTestBase, TestingAppendSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
  * Integrate tests for group window aggregate in SQL API.
  *
  * NOTE: All the cases in this file should support minibatch window, if not, please move to
  * [[GroupWindowITCase]].
  *
  * For the requirements which windows support minibatch, please see
  * [[org.apache.flink.table.plan.nodes.physical.stream.StreamExecGroupWindowAggregate]].
  */
@RunWith(classOf[Parameterized])
class MiniBatchGroupWindowITCase(miniBatch: MiniBatchMode, mode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, mode) {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi", "a"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo", "a"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello", "a"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello", "b"),
    (6L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world", "a"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world", "b"),
    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String], null.asInstanceOf[String]))

  @Test
  def testDistinctAggOnRowTimeTumbleWindow(): Unit = {

    val t = failingDataSource(StreamTestData.get5TupleData).assignAscendingTimestamps(x => x._2)
            .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      """
        |SELECT a,
        |   SUM(DISTINCT e),
        |   MIN(DISTINCT e),
        |   COUNT(DISTINCT e)
        |FROM MyTable
        |GROUP BY a, TUMBLE(rowtime, INTERVAL '5' SECOND)
      """.stripMargin

    val results = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = List(
      "1,1,1,1",
      "2,3,1,2",
      "3,5,2,2",
      "4,3,1,2",
      "5,6,1,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testHopStartEndWithHaving(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sqlQueryHopStartEndWithHaving =
      """
        |SELECT
        |  c AS k,
        |  COUNT(a) AS v,
        |  HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowStart,
        |  HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowEnd
        |FROM T1
        |GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE), c
        |HAVING
        |  SUM(b) > 1 AND
        |    QUARTER(HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE)) = 1
      """.stripMargin
    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Right(14000010L),
      Left(8640000000L, (4, 1L, "Hello")), // data for the quarter to validate having filter
      Left(8640000001L, (4, 1L, "Hello")),
      Right(8640000010L)
    )
    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("T1", t1)
    val resultHopStartEndWithHaving = tEnv
      .sqlQuery(sqlQueryHopStartEndWithHaving)
      .toAppendStream[Row]
    val sink = new TestingAppendSink
    resultHopStartEndWithHaving.addSink(sink)
    env.execute()
    val expected = List(
      "Hello,2,1970-01-01 03:53:00.0,1970-01-01 03:54:00.0"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeSlidingWindowWithTimeZone(): Unit = {
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](0L))
    val table = stream.toTable(tEnv, 'ts.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)
    tEnv.getConfig.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val sql =
      """
        |SELECT
        |  string,
        |  HOP_START(ts, INTERVAL '12' HOUR, INTERVAL '1' DAY),
        |  HOP_ROWTIME(ts, INTERVAL '12' HOUR, INTERVAL '1' DAY),
        |  COUNT(`int`),
        |  COUNT(DISTINCT `float`)
        |FROM T1
        |GROUP BY string, HOP(ts, INTERVAL '12' HOUR, INTERVAL '1' DAY)
      """.stripMargin

    val sink = new TestingAppendSink(tEnv.getConfig.getTimeZone)
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()
    val expected = Seq(
      "Hallo,1969-12-31 12:00:00.0,1970-01-01 11:59:59.999,1,1",
      "Hallo,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,1,1",
      "Hello world,1969-12-31 12:00:00.0,1970-01-01 11:59:59.999,2,2",
      "Hello world,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,2,2",
      "Hello,1969-12-31 12:00:00.0,1970-01-01 11:59:59.999,4,3",
      "Hello,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,4,3",
      "Hi,1969-12-31 12:00:00.0,1970-01-01 11:59:59.999,1,1",
      "Hi,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,1,1",
      "null,1969-12-31 12:00:00.0,1970-01-01 11:59:59.999,1,1",
      "null,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,1,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindowWithTimeZone(): Unit = {
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
          [(Long, Int, Double, Float, BigDecimal, String, String)](0L))
    val table = stream.toTable(tEnv, 'ts.rowtime, 'int, 'double, 'float, 'bigdec, 'string, 'name)
    tEnv.registerTable("T1", table)
    tEnv.getConfig.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val sql =
      """
        |SELECT
        |  string,
        |  TUMBLE_START(ts, INTERVAL '1' DAY),
        |  TUMBLE_ROWTIME(ts, INTERVAL '1' DAY),
        |  COUNT(`int`),
        |  COUNT(DISTINCT `float`)
        |FROM T1
        |GROUP BY string, TUMBLE(ts, INTERVAL '1' DAY)
      """.stripMargin

    val sink = new TestingAppendSink(tEnv.getConfig.getTimeZone)
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()
    val expected = Seq(
      "Hallo,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,1,1",
      "Hello world,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,2,2",
      "Hello,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,4,3",
      "Hi,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,1,1",
      "null,1970-01-01 00:00:00.0,1970-01-01 23:59:59.999,1,1")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }
}
