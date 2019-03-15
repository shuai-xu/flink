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

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.Func0
import org.apache.flink.table.runtime.utils.StreamingWithAggTestBase.{AggMode, LocalGlobalOff, LocalGlobalOn}
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class StateReuseTest(
    aggMode: AggMode,
    miniBatch: MiniBatchMode) extends StreamPlanTestBase(aggMode, miniBatch) {

  @Before
  override def before(): Unit = {
    super.before()
    streamUtil.addTable[(Long, Int, Long, Long, Long)](
      "MyTable", 'a, 'b, 'c, 'd, 'e, 'proctime.proctime, 'rowtime.rowtime)
    streamUtil.addTable[(Long, Int, Long, Long, Long)](
      "MyTable2", 'a, 'b, 'c, 'd, 'e, 'proctime.proctime, 'rowtime.rowtime)
    val tableConfig = streamUtil.tableEnv.getConfig
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG, true)
  }

  @Test
  def testGroupAggregate(): Unit = {
    streamUtil.tableEnv.registerFunction("func", Func0)
    val sql = "SELECT a, sum(func(b)), count(distinct c) FROM MyTable WHERE c > 100 GROUP BY a"
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testWindowAggregate(): Unit = {
    val sql =
      s"""
         |SELECT SUM(a),
         |  HOP_START(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND),
         |  HOP_END(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
         |FROM MyTable
         |GROUP BY HOP(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
       """.stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testOver(): Unit = {
    val sql =
      s"""
         |SELECT a,
         |  SUM(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
         |  MIN(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
         |FROM MyTable
         |WHERE b > 100
       """.stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testInnerJoin(): Unit = {
    streamUtil.tableEnv.registerFunction("func", Func0)
    val sql =
      s"""
         |SELECT a1, a2, b1, b2
         |FROM A JOIN B
         |  ON a1 = b1 AND a1 > 100 AND func(a1) > func(b1)
       """.stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testOuterJoin(): Unit = {
    streamUtil.tableEnv.registerFunction("func", Func0)
    val sql =
      s"""
         |SELECT a1, a2, b1, b2
         |FROM A RIGHT OUTER JOIN B
         |  ON a1 = b1 AND a1 > 100 AND func(a1) > func(b1)
       """.stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testWindowJoin(): Unit = {
    val sql =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM MyTable as t1 join MyTable2 as t2 ON
        |  t1.a = t2.a AND t1.b > t2.b AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '5' SECOND AND
        |    t2.proctime + INTERVAL '5' SECOND
        |""".stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testRank(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) as c
        |  FROM MyTable)
        |WHERE c <= 2
      """.stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testSubplanReuse(): Unit = {
    streamUtil.tableEnv.registerFunction("func", Func0)
    streamUtil.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    val subquery =
      s"""
         |SELECT
         |  a, sum(func(b)) as b, count(distinct c) as c
         |FROM MyTable
         |WHERE c > 100
         |GROUP BY a
       """.stripMargin
    streamUtil.tableEnv.registerTable("myView", streamUtil.tableEnv.sqlQuery(subquery))

    val sql =
      s"""
         |(
         |  SELECT
         |    b as a, c as b, sum(c) as c
         |  FROM
         |    myView
         |  GROUP BY b, c
         |)
         |UNION ALL
         |(
         |  SELECT a, c as b, count(b) as c
         |  FROM
         |    myView
         |  GROUP BY a, c
         |)
       """.stripMargin
    streamUtil.verifyStateDigest(sql)
  }

  @Test
  def testGroupAggregateStateReusableWithFilter(): Unit = {
    streamUtil.tableEnv.registerFunction("func", Func0)
    val oldSql = "SELECT a, sum(func(b)), count(distinct c) FROM MyTable GROUP BY a"
    // add filter clause
    val newSql = "SELECT a, sum(func(b)), count(distinct c) FROM MyTable WHERE c > 100 GROUP BY a"
    streamUtil.verifyUidIdentical(oldSql, newSql)
  }

  @Test
  def testGroupAggregateStateNonReusableWithAggChanged(): Unit = {
    expectedException.expect(classOf[AssertionError])

    val oldSql = "SELECT a, sum(b) FROM MyTable GROUP BY a"
    // change aggregate function: sum -> count
    val newSql = "SELECT a, count(b) FROM MyTable GROUP BY a"
    streamUtil.verifyUidIdentical(oldSql, newSql)
  }

  @Test
  def testGroupAggregateStateReusableAddingAnotherGroupAggregate(): Unit = {
    val oldSql = "SELECT a, sum(b) FROM MyTable GROUP BY a"
    // add another GROUP BY clause
    val newSql =
      s"""
         |(
         |  SELECT a, sum(b)
         |  FROM MyTable
         |  GROUP BY a
         |)
         |UNION ALL
         |(
         |  SELECT a, count(b)
         |  FROM MyTable
         |  WHERE c > 100
         |  GROUP BY a
         |)
       """.stripMargin
    streamUtil.verifyUidIdentical(oldSql, newSql)
  }

  @Test
  def testGroupWindowStateReusableWithFilter(): Unit = {
    val oldSql =
      s"""
         |SELECT SUM(a),
         |  HOP_START(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND),
         |  HOP_END(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
         |FROM MyTable
         |GROUP BY HOP(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
       """.stripMargin

    // add filter clause
    val newSql =
      s"""
         |SELECT SUM(a),
         |  HOP_START(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND),
         |  HOP_END(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
         |FROM MyTable
         |WHERE c > 100
         |GROUP BY HOP(rowtime, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
       """.stripMargin

    streamUtil.verifyUidIdentical(oldSql, newSql)
  }

  @Test
  def testOverWindowStateReusableWithFilter(): Unit = {
    val oldSql =
      s"""
         |SELECT a,
         |  SUM(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
         |  MIN(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
         |FROM MyTable
       """.stripMargin

    // add filter clause
    val newSql =
      s"""
         |SELECT a,
         |  SUM(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
         |  MIN(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
         |FROM MyTable
         |WHERE b > 100
       """.stripMargin

    streamUtil.verifyUidIdentical(oldSql, newSql)
  }

  @Test
  def testJoinStateReusableWithFilter(): Unit = {
    val oldSql = "SELECT a1, a2, b1, b2 FROM A RIGHT OUTER JOIN B ON a1 = b1"
    val newSql = "SELECT a1, a2, b1, b2 FROM A RIGHT OUTER JOIN B ON a1 = b1 AND a1 > 100"
    streamUtil.verifyUidIdentical(oldSql, newSql)
  }
}

object StateReuseTest {

  @Parameterized.Parameters(name = "LocalGlobal={0}, {1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(LocalGlobalOff, MiniBatchOff),
      Array(LocalGlobalOff, MiniBatchOn),
      Array(LocalGlobalOn, MiniBatchOn))
  }
}
