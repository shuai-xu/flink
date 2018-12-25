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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class RankITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testOrderByLimit(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql = "SELECT * FROM T ORDER BY num DESC LIMIT 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "fruit,3,44",
      "fruit,4,33")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLimitWithoutOrderBy(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql = "SELECT * FROM T LIMIT 4"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "book,1,12",
      "book,2,19",
      "book,4,11",
      "fruit,4,33")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test @Ignore
  def testLimitWithSubQueryWithoutOrderBy(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql = "SELECT * FROM (SELECT * FROM T WHERE shopId > 1 LIMIT 4) LIMIT 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "book,2,19",
      "book,4,11",
      "fruit,4,33")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTopN(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM T)
        |WHERE rank_num <= 2
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "book,2,19,1",
      "book,1,12,2",
      "fruit,3,44,1",
      "fruit,4,33,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTopNth(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM T)
        |WHERE rank_num = 2
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "book,1,12,2",
      "fruit,4,33,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTopNWithUpsertSink(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num ASC) as rank_num
        |  FROM T)
        |WHERE rank_num <= 2
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(sink)
    env.execute()

    val expected = List(
      "book,4,11,1",
      "book,1,12,2",
      "fruit,5,22,1",
      "fruit,4,33,2")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithApprox(): Unit = {
    val data = List(
      ("book", 11, 100),
      ("book", 11, 200),
      ("book", 12, 400),
      ("book", 12, 500),
      ("book", 10, 600),
      ("book", 10, 700),
      ("book", 9, 800),
      ("book", 9, 900),
      ("book", 10, 500),
      ("book", 8, 110),
      ("book", 8, 120),
      ("book", 7, 1800),
      ("book", 9, 300),
      ("book", 6, 1900),
      ("book", 7, 50),
      ("book", 11, 1800),
      ("book", 7, 50),
      ("book", 8, 2000),
      ("book", 6, 700),
      ("book", 5, 800),
      ("book", 4, 910),
      ("book", 3, 1000),
      ("book", 2, 1100),
      ("book", 1, 1200)
    )

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num ASC, cnt DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num, count(num) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

      tEnv.getConfig.getConf.setBoolean(TableConfigOptions.BLINK_TOPN_APPROXIMATE_ENABLED, true)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, 20)

    val sink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(sink)
    env.execute()

    val updatedExpected = List(
      "book,5,800,1",
      "book,12,900,2",
      "book,4,910,3")

    assertEquals(updatedExpected.sorted, sink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithUnary(): Unit = {
    val data = List(
      ("book", 11, 100),
      ("book", 11, 200),
      ("book", 12, 400),
      ("book", 12, 500),
      ("book", 10, 600),
      ("book", 10, 700),
      ("book", 9, 800),
      ("book", 9, 900),
      ("book", 10, 500),
      ("book", 8, 110),
      ("book", 8, 120),
      ("book", 7, 1800),
      ("book", 9, 300),
      ("book", 6, 1900),
      ("book", 7, 50),
      ("book", 11, 1800),
      ("book", 7, 50),
      ("book", 8, 2000),
      ("book", 6, 700),
      ("book", 5, 800),
      ("book", 4, 910),
      ("book", 3, 1000),
      ("book", 2, 1100),
      ("book", 1, 1200)
    )

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(sink)
    env.execute()

    val updatedExpected = List(
      "book,5,800,1",
      "book,12,900,2",
      "book,4,910,3")

    assertEquals(updatedExpected.sorted, sink.getUpsertResults.sorted)
  }

  @Ignore("Enable when state support SortedMapState")
  @Test
  def testUnarySortTopNOnString(): Unit = {
    val data = List(
      ("book", 11, "100"),
      ("book", 11, "200"),
      ("book", 12, "400"),
      ("book", 12, "600"),
      ("book", 10, "600"),
      ("book", 10, "700"),
      ("book", 9, "800"),
      ("book", 9, "900"),
      ("book", 10, "500"),
      ("book", 8, "110"),
      ("book", 8, "120"),
      ("book", 7, "812"),
      ("book", 9, "300"),
      ("book", 6, "900"),
      ("book", 7, "50"),
      ("book", 11, "800"),
      ("book", 7, "50"),
      ("book", 8, "200"),
      ("book", 6, "700"),
      ("book", 5, "800"),
      ("book", 4, "910"),
      ("book", 3, "110"),
      ("book", 2, "900"),
      ("book", 1, "700")
    )

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'price)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, max_price,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_price ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, max(price) as max_price
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(sink)
    env.execute()

    val updatedExpected = List(
      "book,3,110,1",
      "book,8,200,2",
      "book,12,600,3")

    assertEquals(updatedExpected.sorted, sink.getUpsertResults.sorted)
  }


  @Test
  def testTopNWithGroupBy(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),
      ("fruit", 4, 33),
      ("fruit", 5, 12),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 2
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(sink)
    env.execute()

    val updatedExpected = List(
      "book,1,22,1",
      "book,2,19,2",
      "fruit,3,44,1",
      "fruit,5,34,2")
    assertEquals(updatedExpected.sorted, sink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithSumAndCondition(): Unit = {
    val data = List(
      Row.of("book", Int.box(11), Double.box(100)),
      Row.of("book", Int.box(11), Double.box(200)),
      Row.of("book", Int.box(12), Double.box(400)),
      Row.of("book", Int.box(12), Double.box(500)),
      Row.of("book", Int.box(10), Double.box(600)),
      Row.of("book", Int.box(10), Double.box(700)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO) // tpe is automatically

    val ds = env.fromCollection(data)
    val t = ds.toTable(tEnv).as('category, 'shopId, 'num)
    tEnv.registerTable("T", t)

    val subquery =
      """
        |SELECT category, shopId, sum(num) as sum_num
        |FROM T
        |WHERE num >= cast(1.1 as double)
        |GROUP BY category, shopId
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, sum_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sum_num DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 2
      """.stripMargin

    val sink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(sink)
    env.execute()

    val updatedExpected = List(
      "book,10,1300.0,1",
      "book,12,900.0,2")

    assertEquals(updatedExpected.sorted, sink.getUpsertResults.sorted)
  }

  @Test
  def testTopNthWithGroupBy(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),
      ("fruit", 4, 33),
      ("fruit", 5, 12),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num = 2
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val updatedExpected = List(
      "book,2,19,2",
      "fruit,5,34,2")

    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithGroupByAndRetract(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),
      ("fruit", 4, 33),
      ("fruit", 5, 12),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num, cnt,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC, cnt ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num, count(num) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 2
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "book,1,22,2,1",
      "book,2,19,1,2",
      "fruit,3,44,1,1",
      "fruit,5,34,2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTopNWithGroupByAndApprox(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),
      ("fruit", 4, 33),
      ("fruit", 5, 12),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num, cnt,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC, cnt ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num, count(num) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 2
      """.stripMargin

    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.BLINK_TOPN_APPROXIMATE_ENABLED, true)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, 20)

    val tableSink = new TestingUpsertTableSink(Array(0, 4))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val updatedExpected = List(
      "book,1,22,2,1",
      "book,2,19,1,2",
      "fruit,3,44,1,1",
      "fruit,5,34,2,2")
    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNthWithGroupByAndRetract(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),
      ("fruit", 4, 33),
      ("fruit", 5, 12),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num, cnt,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC, cnt ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num, count(num) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num = 2
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "book,2,19,1,2",
      "fruit,5,34,2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTopNthWithGroupByAndApprox(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),
      ("book", 4, 1),
      ("book", 4, 8),
      ("book", 1, 0),
      ("book", 1, 0),
      ("book", 2, 3),
      ("fruit", 4, 33),
      ("fruit", 5, 12),
      ("fruit", 3, 44),
      ("fruit", 5, 22),
      ("fruit", 4, 1),
      ("fruit", 5, 0)
    )

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num, cnt,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC, cnt ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num, count(num) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num = 2
      """.stripMargin

     tEnv.getConfig.getConf.setBoolean(TableConfigOptions.BLINK_TOPN_APPROXIMATE_ENABLED, true)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, 20)

    val tableSink = new TestingUpsertTableSink(Array(0, 4))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val updatedExpected = List(
      "book,4,22,3,2",
      "fruit,4,34,2,2")
    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithGroupByCount(): Unit = {
    val data = List(
      ("book", 1, 1001),
      ("book", 2, 1002),
      ("book", 4, 1003),
      ("book", 1, 1004),
      ("book", 1, 1005),
      ("book", 3, 1006),
      ("book", 2, 1007),
      ("book", 4, 1008),
      ("book", 1, 1009),
      ("book", 4, 1010),
      ("book", 4, 1012),
      ("book", 4, 1012),
      ("fruit", 4, 1013),
      ("fruit", 5, 1014),
      ("fruit", 3, 1015),
      ("fruit", 4, 1017),
      ("fruit", 5, 1018),
      ("fruit", 5, 1016))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, rank_num, sells, shopId
        |FROM (
        |  SELECT category, shopId, sells,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sells DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, count(sellId) as sells
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 4
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "book,1,5,4",
      "book,2,4,1",
      "book,3,2,2",
      "book,4,1,3",
      "fruit,1,3,5",
      "fruit,2,2,4",
      "fruit,3,1,3")
    assertEquals(expected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNthWithGroupByCount(): Unit = {
    val data = List(
      ("book", 1, 1001),
      ("book", 2, 1002),
      ("book", 4, 1003),
      ("book", 1, 1004),
      ("book", 1, 1005),
      ("book", 3, 1006),
      ("book", 2, 1007),
      ("book", 4, 1008),
      ("book", 1, 1009),
      ("book", 4, 1010),
      ("book", 4, 1012),
      ("book", 4, 1012),
      ("fruit", 4, 1013),
      ("fruit", 5, 1014),
      ("fruit", 3, 1015),
      ("fruit", 4, 1017),
      ("fruit", 5, 1018),
      ("fruit", 5, 1016))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, rank_num, sells, shopId
        |FROM (
        |  SELECT category, shopId, sells,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sells DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, count(sellId) as sells
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num = 3
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "book,3,2,2",
      "fruit,3,1,3")
    assertEquals(expected.sorted, tableSink.getUpsertResults.sorted)
  }


  @Test
  def testNestedTopN(): Unit = {
    val data = List(
      ("book", "a", 1),
      ("book", "b", 1),
      ("book", "c", 1),
      ("fruit", "a", 2),
      ("book", "a", 1),
      ("book", "d", 0),
      ("book", "b", 3),
      ("fruit", "b", 6),
      ("book", "c", 1),
      ("book", "e", 5),
      ("book", "d", 4))

    env.setParallelism(1)
    val ds = failingDataSource(data).toTable(tEnv, 'cate, 'shopId, 'sells)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT rank_num, cate, shopId, sells, cnt
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY cate ORDER BY sells DESC) as rank_num
        |  FROM (
        |     SELECT cate, shopId, count(*) as cnt, max(sells) as sells
        |     FROM T
        |     GROUP BY cate, shopId
        |  ))
        |WHERE rank_num <= 4
      """.stripMargin


    val sql2 =
      s"""
         |SELECT rank_num, cate, shopId, sells, cnt
         |FROM (
         |  SELECT cate, shopId, sells, cnt,
         |     ROW_NUMBER() OVER (ORDER BY sells DESC) as rank_num
         |  FROM ($sql)
         |)
         |WHERE rank_num <= 4
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0))
    tEnv.sqlQuery(sql2).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,1,book,a,1,1)", "(true,2,book,b,1,1)", "(true,3,book,c,1,1)",
      "(true,1,fruit,a,2,1)", "(true,2,book,a,1,1)", "(true,3,book,b,1,1)", "(true,4,book,c,1,1)",
      "(true,2,book,a,1,2)",
      "(true,1,book,b,3,2)", "(true,2,fruit,a,2,1)", "(true,3,book,a,1,2)",
      "(true,3,book,a,1,2)",
      "(true,1,fruit,b,6,1)", "(true,2,book,b,3,2)", "(true,3,fruit,a,2,1)", "(true,4,book,a,1,2)",
      "(true,3,fruit,a,2,1)",
      "(true,2,book,e,5,1)",
      "(true,3,book,b,3,2)", "(true,4,fruit,a,2,1)",
      "(true,3,book,b,3,2)",
      "(true,3,book,d,4,2)",
      "(true,4,book,b,3,2)",
      "(true,4,book,b,3,2)")
    assertEquals(expected.mkString("\n"), tableSink.getRawResults.mkString("\n"))

    val expected2 = List("1,fruit,b,6,1", "2,book,e,5,1", "3,book,d,4,2", "4,book,b,3,2")
    assertEquals(expected2, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithoutDeduplicate(): Unit = {
    val data = List(
      ("book", "a", 1),
      ("book", "b", 1),
      ("book", "c", 1),
      ("fruit", "a", 2),
      ("book", "a", 1),
      ("book", "d", 0),
      ("book", "b", 3),
      ("fruit", "b", 6),
      ("book", "c", 1),
      ("book", "e", 5),
      ("book", "d", 4))

    env.setParallelism(1)
    val ds = failingDataSource(data).toTable(tEnv, 'cate, 'shopId, 'sells)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT rank_num, cate, shopId, sells, cnt
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY cate ORDER BY sells DESC) as rank_num
        |  FROM (
        |     SELECT cate, shopId, count(*) as cnt, max(sells) as sells
        |     FROM T
        |     GROUP BY cate, shopId
        |  ))
        |WHERE rank_num <= 4
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0))
    tEnv.getConfig.withTopNCacheSize(1)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,1,book,a,1,1)",
      "(true,2,book,b,1,1)",
      "(true,3,book,c,1,1)",
      "(true,1,fruit,a,2,1)",
      "(true,1,book,a,1,2)",
      "(true,4,book,d,0,1)",
      "(true,1,book,b,3,2)",
      "(true,2,book,a,1,2)",
      "(true,1,fruit,b,6,1)",
      "(true,2,fruit,a,2,1)",
      "(true,3,book,c,1,2)",
      "(true,1,book,e,5,1)",
      "(true,2,book,b,3,2)",
      "(true,3,book,a,1,2)",
      "(true,4,book,c,1,2)",
      "(true,2,book,d,4,2)",
      "(true,3,book,b,3,2)",
      "(true,4,book,a,1,2)")

    assertEquals(expected, tableSink.getRawResults)
  }

  @Test
  def testTopNWithVariableTopSize(): Unit = {
    val data = List(
      ("book", 1, 1001, 4),
      ("book", 2, 1002, 4),
      ("book", 4, 1003, 4),
      ("book", 1, 1004, 4),
      ("book", 1, 1005, 4),
      ("book", 3, 1006, 4),
      ("book", 2, 1007, 4),
      ("book", 4, 1008, 4),
      ("book", 1, 1009, 4),
      ("book", 4, 1010, 4),
      ("book", 4, 1012, 4),
      ("book", 4, 1012, 4),
      ("fruit", 4, 1013, 2),
      ("fruit", 5, 1014, 2),
      ("fruit", 3, 1015, 2),
      ("fruit", 4, 1017, 2),
      ("fruit", 5, 1018, 2),
      ("fruit", 5, 1016, 2))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId, 'topSize)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, rank_num, sells, shopId
        |FROM (
        |  SELECT category, shopId, sells, topSize,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sells DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, count(sellId) as sells, max(topSize) as topSize
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= topSize
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "book,1,5,4",
      "book,2,4,1",
      "book,3,2,2",
      "book,4,1,3",
      "fruit,1,3,5",
      "fruit,2,2,4")
    assertEquals(expected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNApproxComplexScenario(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 22),
      ("book", 3, 33),
      ("book", 4, 44),
      ("book", 4, -24),  // k1, k2, k3, k4  =forward update=> k1, k4, k2, k3
      ("book", 1, 12),   // k1, k4, k2, k3  =backward update=> k4, k2, k1, k3
      ("book", 5, 26),   // k4, k2, k1, k3  =insert=> k4,k2,k1,k5 | k3
      ("book", 6, 66),
      ("book", 7, 77),
      ("book", 1, 35),   // k4,k2,k1,k5 | k3,k6,k7  =backward update=> k4,k2,k5,k3 | k1,k6,k7
      ("book", 6, -42),  // k4,k2,k5,k3 | k1,k6,k7  =forward update=> k4,k2,k6,k5 | k3,k1,k7
      ("book", 8, 88),
      ("book", 9, 23),   // k4,k2,k6,k5 | k3,k1,k7,k8  =insert=> k4,k2,k9,k6 | k5,k3,k1,k7
      ("book", 10, 100)  // insert out of range while buffer's full
    )

    env.setParallelism(1)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, sellSums, cnt,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sellSums ASC, cnt DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, SUM(sellId) as sellSums, COUNT(sellId) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 4
      """.stripMargin

    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.BLINK_TOPN_APPROXIMATE_ENABLED, true)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, 0)

    val tableSink = new TestingUpsertTableSink(Array(0, 4))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,book,1,11,1,1)",
      "(true,book,2,22,1,2)",
      "(true,book,3,33,1,3)",
      "(true,book,4,44,1,4)",

      "(true,book,4,20,2,2)",  // forward update of k4
      "(true,book,2,22,1,3)",
      "(true,book,3,33,1,4)",

      "(true,book,4,20,2,1)",  // backward update of k1
      "(true,book,2,22,1,2)",
      "(true,book,1,23,2,3)",

      "(true,book,5,26,1,4)",  // insert k5

      "(true,book,5,26,1,3)",  // backward update of k1
      "(true,book,3,33,1,4)",

      "(true,book,6,24,2,3)",  // forward update of k6
      "(true,book,5,26,1,4)",

      "(true,book,9,23,1,3)",  // insert k9
      "(true,book,6,24,2,4)")

    assertEquals(expected, tableSink.getRawResults)

    val updatedExpected = List(
      "book,4,20,2,1",
      "book,2,22,1,2",
      "book,9,23,1,3",
      "book,6,24,2,4")

    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNUnaryComplexScenario(): Unit = {
    val data = List(
      ("book", 1, 11),
      ("book", 2, 19),
      ("book", 4, 13),
      ("book", 1, 11),    // backward update in heap
      ("book", 3, 23),    // elems exceed topn size after insert
      ("book", 5, 19),    // sort map shirk out some elem after insert
      ("book", 7, 10),    // sort map keeps a little more than topn size elems after insert
      ("book", 8, 13),    // sort map now can shrink out-of-range elems after another insert
      ("book", 10, 13),   // Once again, sort map keeps a little more elems after insert
      ("book", 8, 6),     // backward update from heap to state
      ("book", 10, 6),    // backward update from heap to state, and sort map load more data
      ("book", 5, 3),     // backward update from heap to state
      ("book", 10, 1),    // backward update in heap, and then sort map shrink some data
      ("book", 5, 1),     // backward update in state
      ("book", 5, -3),    // forward update in state
      ("book", 2, -10),   // forward update in heap, and then sort map shrink some data
      ("book", 10, -7),   // forward update from state to heap
      ("book", 11, 13),   // insert into heap
      ("book", 12, 10),   // insert into heap, and sort map shrink some data
      ("book", 15, 14)    // insert into state
    )

    env.setParallelism(1)

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, sum(num) as num
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(sql).writeToSink(tableSink)

    env.execute()

    val expected = List(
      "(true,book,1,11,1)",
      "(true,book,2,19,2)",
      "(true,book,4,13,2)",
      "(true,book,2,19,3)",

      "(true,book,4,13,1)",
      "(true,book,2,19,2)",
      "(true,book,1,22,3)",

      "(true,book,5,19,3)",

      "(true,book,7,10,1)",
      "(true,book,4,13,2)",
      "(true,book,2,19,3)",

      "(true,book,8,13,3)",

      "(true,book,10,13,3)",

      "(true,book,2,19,3)",

      "(true,book,2,9,1)",
      "(true,book,7,10,2)",
      "(true,book,4,13,3)",

      "(true,book,12,10,3)")

    assertEquals(expected.mkString("\n"), tableSink.getRawResults.mkString("\n"))

    val updatedExpected = List(
      "book,2,9,1",
      "book,7,10,2",
      "book,12,10,3")

    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithTwoSortFieldsWithoutRowNumber(): Unit = {
    val data = List(
      ("book", 1, 100),
      ("book", 3, 110),
      ("book", 4, 120),
      ("book", 1, 200),
      ("book", 1, 200),
      ("book", 2, 300),
      ("book", 2, 400),
      ("book", 4, 500),
      ("book", 1, 400),
      ("fruit", 5, 100))

    env.setParallelism(1)

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, shopId, avgSellId, cnt
        |FROM (
        |  SELECT category, shopId, avgSellId, cnt,
        |    ROW_NUMBER() OVER (PARTITION BY category ORDER BY avgSellId DESC, cnt ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, AVG(sellId) as avgSellId, COUNT(sellId) as cnt
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.BLINK_TOPN_APPROXIMATE_ENABLED, true)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, 0)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,book,1,100.0,1)",
      "(true,book,3,110.0,1)",
      "(true,book,4,120.0,1)",
      "(true,book,1,150.0,2)",
      "(true,book,1,166.66666666666666,3)",
      "(true,book,2,300.0,1)",
      "(false,book,3,110.0,1)",
      "(true,book,2,350.0,2)",
      "(true,book,4,310.0,2)",
      "(true,book,1,225.0,4)",
      "(true,fruit,5,100.0,1)")

    assertEquals(expected, tableSink.getRawResults)

    val updatedExpected = List(
      "book,1,225.0,4",
      "book,4,310.0,2",
      "book,2,350.0,2",
      "fruit,5,100.0,1")

    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithGroupByAvgWithoutRowNumber(): Unit = {
    val data = List(
      ("book", 1, 100),
      ("book", 3, 110),
      ("book", 4, 120),
      ("book", 1, 200),
      ("book", 1, 200),
      ("book", 2, 300),
      ("book", 2, 400),
      ("book", 4, 500),
      ("book", 1, 400),
      ("fruit", 5, 100))

    env.setParallelism(1)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, shopId, avgSellId
        |FROM (
        |  SELECT category, shopId, avgSellId,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY avgSellId DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, AVG(sellId) as avgSellId
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,book,1,100.0)",
      "(true,book,3,110.0)",
      "(true,book,4,120.0)",
      "(true,book,1,150.0)",
      "(true,book,1,166.66666666666666)",
      "(true,book,2,300.0)",
      "(false,book,3,110.0)",
      "(true,book,2,350.0)",
      "(true,book,4,310.0)",
      "(true,book,1,225.0)",
      "(true,fruit,5,100.0)")

    assertEquals(expected, tableSink.getRawResults)

    val updatedExpected = List(
      "book,1,225.0",
      "book,2,350.0",
      "book,4,310.0",
      "fruit,5,100.0")

    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithGroupByCountWithoutRowNumber(): Unit = {
    val data = List(
      ("book", 1, 1001),
      ("book", 3, 1006),
      ("book", 4, 1003),
      ("book", 1, 1004),
      ("book", 1, 1005),
      ("book", 2, 1002),
      ("book", 2, 1007),
      ("book", 4, 1008),
      ("book", 1, 1009),
      ("book", 4, 1010),
      ("book", 4, 1012),
      ("book", 4, 1012),
      ("fruit", 4, 1013),
      ("fruit", 5, 1014),
      ("fruit", 3, 1015),
      ("fruit", 4, 1017),
      ("fruit", 5, 1018),
      ("fruit", 5, 1016))

    env.setParallelism(1)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'sellId)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, shopId, sells
        |FROM (
        |  SELECT category, shopId, sells,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sells DESC) as rank_num
        |  FROM (
        |     SELECT category, shopId, count(sellId) as sells
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,book,1,1)",
      "(true,book,3,1)",
      "(true,book,4,1)",
      "(true,book,1,2)",
      "(true,book,1,3)",
      "(true,book,2,2)",
      "(false,book,4,1)",
      "(true,book,4,2)",
      "(false,book,3,1)",
      "(true,book,1,4)",
      "(true,book,4,3)",
      "(true,book,4,4)",
      "(true,book,4,5)",
      "(true,fruit,4,1)",
      "(true,fruit,5,1)",
      "(true,fruit,3,1)",
      "(true,fruit,4,2)",
      "(true,fruit,5,2)",
      "(true,fruit,5,3)")
    assertEquals(expected, tableSink.getRawResults)

    val updatedExpected = List(
      "book,4,5",
      "book,1,4",
      "book,2,2",
      "fruit,5,3",
      "fruit,4,2",
      "fruit,3,1")
    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testTopNWithoutRowNumber(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("book", 5, 20),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22),
      ("fruit", 1, 40))

    env.setParallelism(1)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql =
      """
        |SELECT category, num, shopId
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM T)
        |WHERE rank_num <= 2
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 2))
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    val expected = List(
      "(true,book,12,1)",
      "(true,book,19,2)",
      "(true,book,20,5)",
      "(false,book,12,1)",
      "(true,fruit,33,4)",
      "(true,fruit,44,3)",
      "(true,fruit,40,1)",
      "(false,fruit,33,4)")
    assertEquals(expected, tableSink.getRawResults)

    val updatedExpected = List(
      "book,19,2",
      "book,20,5",
      "fruit,40,1",
      "fruit,44,3")
    assertEquals(updatedExpected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testMultipleRetractTopNAfterAgg(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 1, 13),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    env.setParallelism(1)
    tEnv.config.setSubsectionOptimization(true)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val subquery =
      s"""
         |SELECT category, shopId, SUM(num) as sum_num, MAX(num) as max_num,
         | AVG(num) as avg_num, COUNT(num) as cnt
         |FROM T
         |GROUP BY category, shopId
         |""".stripMargin

    val t1 = tEnv.sqlQuery(subquery)
    tEnv.registerTable("MyView", t1)

    val sink1 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, sum_num, avg_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sum_num DESC, avg_num ASC
         |       ) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink1)

    val sink2 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, max_num, cnt,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_num DESC, cnt ASC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink2)

    tEnv.execute()

    val expected1 = List(
      "book,1,25,12.5,1",
      "book,2,19,19.0,2",
      "fruit,3,44,44.0,1",
      "fruit,4,33,33.0,2")
    assertEquals(expected1.sorted, sink1.getRetractResults.sorted)

    val expected2 = List(
      "book,2,19,1,1",
      "book,1,13,2,2",
      "fruit,3,44,1,1",
      "fruit,4,33,1,2")
    assertEquals(expected2.sorted, sink2.getRetractResults.sorted)
  }

  @Test
  def testMultipleUnaryTopNAfterAgg(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 1, 13),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    env.setParallelism(1)
    tEnv.config.setSubsectionOptimization(true)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val subquery =
      s"""
         |SELECT category, shopId, SUM(num) as sum_num, MAX(num) as max_num
         |FROM T
         |GROUP BY category, shopId
         |""".stripMargin

    val t1 = tEnv.sqlQuery(subquery)
    tEnv.registerTable("MyView", t1)

    val sink1 = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, sum_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY sum_num DESC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink1)

    val sink2 = new TestingUpsertTableSink(Array(0, 3))
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, max_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_num DESC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink2)

    tEnv.execute()

    val expected1 = List(
      "book,1,25,1",
      "book,2,19,2",
      "fruit,3,44,1",
      "fruit,4,33,2")
    assertEquals(expected1.sorted, sink1.getUpsertResults.sorted)

    val expected2 = List(
      "book,2,19,1",
      "book,1,13,2",
      "fruit,3,44,1",
      "fruit,4,33,2")
    assertEquals(expected2.sorted, sink2.getUpsertResults.sorted)
  }

  @Test
  def testMultipleUpdateTopNAfterAgg(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 1, 13),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    env.setParallelism(1)
    tEnv.config.setSubsectionOptimization(true)
    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val subquery =
      s"""
         |SELECT category, shopId, COUNT(num) as cnt_num, MAX(num) as max_num
         |FROM T
         |GROUP BY category, shopId
         |""".stripMargin

    val t1 = tEnv.sqlQuery(subquery)
    tEnv.registerTable("MyView", t1)

    val sink1 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, cnt_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY cnt_num DESC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink1)

    val sink2 = new TestingRetractTableSink
    tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM (
         |  SELECT category, shopId, max_num,
         |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_num DESC) as rank_num
         |  FROM MyView)
         |WHERE rank_num <= 2
         |""".stripMargin).writeToSink(sink2)

    tEnv.execute()

    val expected1 = List(
      "book,1,2,1",
      "book,2,1,2",
      "fruit,4,1,1",
      "fruit,3,1,2")
    assertEquals(expected1.sorted, sink1.getRetractResults.sorted)

    val expected2 = List(
      "book,2,19,1",
      "book,1,13,2",
      "fruit,3,44,1",
      "fruit,4,33,2")
    assertEquals(expected2.sorted, sink2.getRetractResults.sorted)
  }

  @Test
  def testUpdateRank(): Unit = {
    val data = List(
      (1, 1), (1, 2), (1, 3),
      (2, 2), (2, 3), (2, 4),
      (3, 3), (3, 4), (3, 5))

    val ds = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", ds)

    // We use max here to ensure the usage of update rank
    val sql = "SELECT a, max(b) FROM T GROUP BY a ORDER BY a LIMIT 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("1,3", "2,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUpdateRankWithOffset(): Unit = {
    val data = List(
      (1, 1), (1, 2), (1, 3),
      (2, 2), (2, 3), (2, 4),
      (3, 3), (3, 4), (3, 5))

    val ds = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", ds)

    // We use max here to ensure the usage of update rank
    val sql = "SELECT a, max(b) FROM T GROUP BY a ORDER BY a LIMIT 2 OFFSET 1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("2,4", "3,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
