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
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.program.{FlinkBatchPrograms, FlinkStreamPrograms}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBase

import scala.collection.JavaConversions._
import org.junit.{Before, Test}

class CalcRankTransposeRuleTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    var startRemove = false
    val programs = FlinkBatchPrograms.buildPrograms(util.tableEnv.getConfig.getConf)
    programs.getProgramNames.foreach { name =>
      if (name.equals(FlinkStreamPrograms.PHYSICAL)) {
        startRemove = true
      }
      if (startRemove) {
        programs.remove(name)
      }
      val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
                          .replaceBatchPrograms(programs).build()
      util.tableEnv.getConfig.setCalciteConfig(calciteConfig)
    }

    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
  }

  @Test
  def testPruneOrderKeys(): Unit = {
    // Push Calc into Rank, project column (a, rowtime), prune column (b, c)
    val sql =
      """
        |SELECT a
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testPrunePartitionKeys(): Unit = {
    // Push Calc into Rank, project column (a, rowtime), prune column (b, c)
    val sql =
      """
        |SELECT rowtime
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testPruneUniqueKeys(): Unit = {
    // Push Calc into Rank, project column (category, shopId, max_price), prune column (min_price)
    util.addTable[(String, Int, String)]("T", 'category, 'shopId, 'price)
    val sql =
      """
        |SELECT category, max_price, rank_num
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_price ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, max(price) as max_price, min(price) as min_price
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    util.verifyPlanAndTrait(sql)
  }

  @Test
  def testNotTranspose(): Unit = {
    // Not transpose calc into Rank because there is no columns to prune
    util.addTable[(String, Int, String)]("T", 'category, 'shopId, 'price)
    val sql =
      """
        |SELECT category, max_price, rank_num
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_price ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, max(price) as max_price
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testLastRowOnRowtime(): Unit = {
    // Push Calc into LastRow, project column (a, rowtime), prune column (b, c)
    val sql =
      """
        |SELECT a, rowtime
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testPruneRankFunc(): Unit = {
    // Push Calc into Rank, project column (a, rowtime), prune column (b, c)
    val sql =
      """
        |SELECT a, rowtime
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE  rank_num <= 2
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testProjectRankFunc(): Unit = {
    // Push Calc into Rank, project column (a, rowtime), prune column (b, c)
    // Need a New Calc on top of Rank to keep equivalency
    val sql =
      """
        |SELECT rank_num, rowtime, a, rank_num, a, rank_num
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE  rank_num <= 2
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testTrivialCalcIsRemoved(): Unit = {
    // Push Calc into Rank, project column (a, rowtime), prune column (b, c)
    // Does not need a New Calc on top of Rank because it is trivial
    val sql =
      """
        |SELECT a, rowtime, rank_num
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE  rank_num <= 2
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testPushCalcWithConditionIntoRank(): Unit = {
    // Push Calc into Rank even if it has filter condition, project column(rowtime, c, a), prune(b)
    val sql =
      """
        |SELECT rowtime, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE  rank_num <= 2 AND a > 10
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testPruneUnusedProject(): Unit = {
    // Push Calc into Rank, project(category, shopId, max_price), prune (min_price)
    util.addTable[(String, Int, String)]("T", 'category, 'shopId, 'price)
    val sql =
      """
        |SELECT category, shopId, max_price, rank_num
        |FROM (
        |  SELECT category, shopId, max_price,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY max_price ASC) as rank_num
        |  FROM (
        |     SELECT category, shopId, max(price) as max_price, min(price) as min_price
        |     FROM T
        |     GROUP BY category, shopId
        |  ))
        |WHERE rank_num <= 3
      """.stripMargin

    util.verifyPlan(sql)
  }
}
