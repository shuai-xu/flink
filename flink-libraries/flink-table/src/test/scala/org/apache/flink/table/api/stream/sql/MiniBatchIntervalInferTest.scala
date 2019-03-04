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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class MiniBatchIntervalInferTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable1", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  streamUtil.addTable[(Int, String, Long)](
    "MyTable2", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  streamUtil.tableEnv.getConfig.getConf
  .setBoolean(TableConfigOptions.SQL_EXEC_MINI_BATCH_WINDOW_ENABLED, true)

  @Test
  def testMiniBatchOnly(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable1 GROUP BY b"
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRedundantWatermarkDefinition(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    streamUtil.registerTableWithWatermark(
      "MyTable3", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    val sql = "SELECT b, COUNT(DISTINCT a), MAX(b), SUM(c) FROM MyTable3 GROUP BY b"
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testWindowWithoutMinibatch(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setBoolean(TableConfigOptions.SQL_EXEC_MINI_BATCH_WINDOW_ENABLED, false)
    streamUtil.registerTableWithWatermark(
      "MyTable3", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    val sql =
      """
        | SELECT b, SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     TUMBLE_START(rowtime, INTERVAL '5' SECOND) as w_start,
        |     TUMBLE_END(rowtime, INTERVAL '5' SECOND) as w_end
        |   FROM MyTable3
        |   GROUP BY b, TUMBLE(rowtime, INTERVAL '5' SECOND)
        | )
        | GROUP BY b
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testWindowWithMinibatch(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 500L)
    streamUtil.registerTableWithWatermark(
      "MyTable3", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    val sql =
      """
        | SELECT b, SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_start,
        |     HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_end
        |   FROM MyTable3
        |   GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        | )
        | GROUP BY b
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testWindowWithEarlyFire(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    streamUtil.tableEnv.getConfig.withEarlyFireInterval(Time.milliseconds(500))
    streamUtil.registerTableWithWatermark(
      "MyTable3", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    val sql =
      """
        | SELECT b, SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     HOP_START(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_start,
        |     HOP_END(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as w_end
        |   FROM MyTable3
        |   GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        | )
        | GROUP BY b
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testWindowCascade(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 3000L)
    streamUtil.registerTableWithWatermark(
      "MyTable3", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    val sql =
      """
        | SELECT b,
        |   SUM(cnt)
        | FROM (
        |   SELECT b,
        |     COUNT(a) as cnt,
        |     TUMBLE_ROWTIME(rowtime, INTERVAL '10' SECOND) as rt
        |   FROM MyTable3
        |   GROUP BY b, TUMBLE(rowtime, INTERVAL '10' SECOND)
        | )
        | GROUP BY b, TUMBLE(rt, INTERVAL '5' SECOND)
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testWindowJoinWithMiniBatch(): Unit = {
    streamUtil.registerTableWithWatermark(
      "LeftT", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    streamUtil.registerTableWithWatermark(
      "RightT", streamUtil.tableEnv.scan("MyTable2"), "rowtime", 0)
    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    val sql =
      """
        | SELECT b, COUNT(a)
        | FROM (
        |   SELECT t1.a as a, t1.b as b
        |   FROM
        |     LeftT as t1 JOIN RightT as t2
        |   ON
        |     t1.a = t2.a AND t1.rowtime BETWEEN t2.rowtime - INTERVAL '5' SECOND AND
        |     t2.rowtime + INTERVAL '10' SECOND
        | )
        | GROUP BY b
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowtimeRowsOverWithMiniBatch(): Unit = {
    streamUtil.registerTableWithWatermark(
      "MyTable3", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    val sql =
      """
        | SELECT cnt, COUNT(c)
        | FROM (
        |   SELECT c, COUNT(a)
        |   OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND CURRENT ROW) as cnt
        |   FROM MyTable3
        | )
        | GROUP BY cnt
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testTemporalTableFunctionJoinWithMiniBatch(): Unit = {
    streamUtil.registerTableWithWatermark(
      "Orders", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    streamUtil.registerTableWithWatermark(
      "RatesHistory", streamUtil.tableEnv.scan("MyTable2"), "rowtime", 0)

    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)

    streamUtil.addFunction(
      "Rates",
      streamUtil.tableEnv.scan("RatesHistory")
        .createTemporalTableFunction('rowtime, 'b))

    val sqlQuery =
      """
        | SELECT r_a, COUNT(o_a)
        | FROM (
        |   SELECT o.a as o_a, r.a as r_a
        |   FROM Orders As o,
        |   LATERAL TABLE (Rates(o.rowtime)) as r
        |   WHERE o.b = r.b
        | )
        | GROUP BY r_a
      """.stripMargin

    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiOperatorNeedsWatermark(): Unit = {
    streamUtil.registerTableWithWatermark(
      "LeftT", streamUtil.tableEnv.scan("MyTable1"), "rowtime", 0)
    streamUtil.registerTableWithWatermark(
      "RightT", streamUtil.tableEnv.scan("MyTable2"), "rowtime", 0)
    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 6000L)

    val sql =
      """
        | SELECT b, COUNT(a)
        | OVER (PARTITION BY b ORDER BY rt ROWS BETWEEN 5 preceding AND CURRENT ROW)
        | FROM (
        |  SELECT t1.a as a, t1.b as b, t1.rt as rt
        |  FROM
        |  (
        |    SELECT b,
        |     COUNT(a) as a,
        |     TUMBLE_ROWTIME(rowtime, INTERVAL '5' SECOND) as rt
        |    FROM LeftT
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '5' SECOND)
        |  ) as t1
        |  JOIN
        |  (
        |    SELECT b,
        |     COUNT(a) as a,
        |     HOP_ROWTIME(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND) as rt
        |    FROM RightT
        |    GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '6' SECOND)
        |  ) as t2
        |  ON
        |    t1.a = t2.a AND t1.rt BETWEEN t2.rt - INTERVAL '5' SECOND AND
        |    t2.rt + INTERVAL '10' SECOND
        | )
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }
}
