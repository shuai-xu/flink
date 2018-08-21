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

package org.apache.flink.table.plan

import org.apache.calcite.rel.{RelCollation, RelDistribution}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.{Before, Ignore, Test}

class RemoveCollationTest extends TableTestBatchExecBase {

  private val util = batchExecTestUtil()

  @Before
  def before(): Unit = {
    // clear parameters
    util.tableEnv.getConfig.setParameters(new Configuration)
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("d", "e", "f")))
    util.tableEnv.alterTableStats("x", Some(TableStats(100L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(100L)))
  }

  @Test
  def testRemoveCollation_OverWindowAgg(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin,HashAgg")
    val sqlQuery =
      """
        | SELECT
        |   SUM(b) sum_b,
        |   AVG(SUM(b)) OVER (PARTITION BY a order by a) avg_b,
        |   RANK() OVER (PARTITION BY a ORDER BY a) rn
        | FROM x
        | GROUP BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Aggregate(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,NestedLoopJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Aggregate_1(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,NestedLoopJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by d
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Sort(): Unit = {
    util.tableEnv.getConfig.getParameters.setBoolean(TableConfig.SQL_EXEC_SORT_ENABLE_RANGE, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, COUNT(c) AS cnt FROM x GROUP BY a, b)
        |SELECT * FROM r ORDER BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Aggregate_3(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashAgg")
    util.tableEnv.getConfig.getParameters.setBoolean(TableConfig.SQL_EXEC_SORT_ENABLE_RANGE, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x ORDER BY a, b)
        |SELECT a, b, COUNT(c) AS cnt FROM r GROUP BY a, b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
