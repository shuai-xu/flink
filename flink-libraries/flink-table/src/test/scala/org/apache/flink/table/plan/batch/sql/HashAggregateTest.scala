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

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.{Before, Test}

/**
  * Test for testing aggregate plans.
  */
class HashAggregateTest extends TableTestBatchExecBase {

  private val util = batchExecTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "SortAgg")
  }

  @Test
  def testAggregate(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithFilter(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithFilterOnNestedFields(): Unit = {
    util.addTable[(Int, Long, (Int, Long))]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT avg(a), sum(b), count(c), sum(c._1) FROM MyTable WHERE a = 1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupAggregate(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1 GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSets(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g FROM MyTable " +
      "GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCube(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " +
      "FROM MyTable " +
      "GROUP BY CUBE (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollup(): Unit = {
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " + " FROM MyTable " +
      "GROUP BY ROLLUP (b, c)"
    util.verifyPlan(sqlQuery)
  }
}
