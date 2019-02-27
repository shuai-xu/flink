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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

class FirstLastRowTest extends TableTestBase {

  var util: StreamTableTestUtil = _

  @Before
  def setUp(): Unit = {
    util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  }

  @Test
  def testInvalidRowNumberConditionOnProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 2""".stripMargin

    // the rank condition is not 1, so it will not be translate to LastRow, but Rank
    util.verifyPlan(sql)
  }

  @Test
  def testInvalidRowNumberConditionOnRowtime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 3""".stripMargin

    // the rank condition is not 1, so it will not be translate to LastRow, but Rank
    util.verifyPlan(sql)
  }

  @Test
  def testSimpleLastRowOnProctime(): Unit = {
    // TODO: merge the bottom two calc in the future.
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleLastRowOnRowtime(): Unit = {
    // FirstLastRow does not support sort on rowtime now, so it is translated to Rank currently
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleFirstRowOnProctime(): Unit = {
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testSimpleFirstRowOnRowtime(): Unit = {
    // FirstLastRow does not support sort on rowtime now, so it is translated to Rank currently
    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime ASC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 1
      """.stripMargin

    util.verifyPlan(sql)
  }
}
