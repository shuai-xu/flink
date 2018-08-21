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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class SplitAggregateTest() extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Long, Int, String)](
    "MyTable", 'a, 'b, 'c)
  streamUtil.tableEnv.queryConfig.enableMiniBatch.enablePartialAgg

  @Test
  def testSingleDistinctAgg(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiCountDistinctAgg(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg(): Unit = {
    val sqlQuery = "SELECT SUM(b), COUNT(DISTINCT c), AVG(b) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    val sqlQuery = "SELECT a, COUNT(DISTINCT c) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithAndNonDistinctAggOnSameColumn(): Unit = {
    val sqlQuery = "SELECT a, COUNT(DISTINCT b), MAX(b), MIN(b) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSomeColumnsBothInDistinctAggAndGroupBy(): Unit = {
    val sqlQuery = "SELECT a, COUNT(DISTINCT a), COUNT(b) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  a,
         |  COUNT(DISTINCT b) filter (where not b = 2),
         |  MAX(b) filter (where not b = 5),
         |  MIN(b) filter (where not b = 2)
         |FROM MyTable
         |GROUP BY a
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testMinMaxWithRetraction(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  c, MIN(b), MAX(b), COUNT(DISTINCT a)
         |FROM(
         |  SELECT
         |    a, COUNT(DISTINCT b) as b, MAX(b) as c
         |  FROM MyTable
         |  GROUP BY a
         |) GROUP BY c
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testFirstValueLastValueWithRetraction(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  b, FIRST_VALUE(c, a), LAST_VALUE(c, a), FIRST_VALUE(c), LAST_VALUE(c), COUNT(DISTINCT c)
         |FROM(
         |  SELECT
         |    a, COUNT(DISTINCT b) as b, MAX(b) as c
         |  FROM MyTable
         |  GROUP BY a
         |) GROUP BY b
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testAggWithJoin(): Unit = {
    val sqlQuery =
      s"""
         |SELECT *
         |FROM(
         |  SELECT
         |    c, MIN(b) as b, MAX(b) as d, COUNT(DISTINCT a) as a
         |  FROM(
         |    SELECT
         |      a, COUNT(DISTINCT b) as b, MAX(b) as c, MIN(b) as d
         |    FROM MyTable
         |    GROUP BY a
         |  ) GROUP BY c
         |) as MyTable1 JOIN MyTable ON MyTable1.b + 2 = MyTable.a
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }
}
