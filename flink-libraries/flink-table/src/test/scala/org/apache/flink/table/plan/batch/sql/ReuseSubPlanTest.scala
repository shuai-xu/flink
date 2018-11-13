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

import java.util.Random

import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.functions.aggfunctions.{IntFirstValueAggFunction, LongLastValueAggFunction}
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms
import org.apache.flink.table.plan.rules.logical.PushLimitIntoTableSourceScanRule
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.{Before, Ignore, Test}

class ReuseSubPlanTest extends TableTestBatchExecBase {

  private val util = batchExecTestUtil()

  @Before
  def before(): Unit = {
    // For #testReuseSubPlan_Limit
    util.tableEnv.getConfig.getCalciteConfig
      .getBatchPrograms
      .getFlinkRuleSetProgram(FlinkBatchPrograms.LOGICAL)
      .get.remove(RuleSets.ofList(PushLimitIntoTableSourceScanRule.INSTANCE))

    // clear parameters
    util.tableEnv.getConfig.setParameters(new Configuration)
    util.tableEnv.getConfig.setSubPlanReuse(true)
    util.tableEnv.getConfig.setTableSourceReuse(false)
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("d", "e", "f")))
    util.tableEnv.alterTableStats("x", Some(TableStats(100L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(100L)))
    BatchExecRel.resetReuseIdCounter()
  }

  @Test
  def testDisableReuseSubPlan(): Unit = {
    util.tableEnv.getConfig.setSubPlanReuse(false)
    val sqlQuery =
      """
        |WITH r AS (
        | SELECT a, SUM(b) as b, SUM(e) as e FROM x, y WHERE a = d AND c > 100 GROUP BY a
        |)
        |SELECT r1.a, r1.b, r2.e FROM r r1, r r2 WHERE r1.b > 10 AND r2.e < 20 AND r1.a = r2.a
      """.stripMargin
    util.verifySqlNotExpected(sqlQuery, "Reused")
  }

  @Test @Ignore // FIXME: BLINK-16477898
  def testReuseSubPlan_DifferentRowType(): Unit = {
    util.tableEnv.getConfig.setTableSourceReuse(false)
    // can not reuse because of different row-type
    val sqlQuery =
      """
        |WITH t1 AS (SELECT CAST(a as BIGINT) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as BIGINT)),
        |     t2 AS (SELECT CAST(a as DOUBLE) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as DOUBLE))
        |SELECT t1.*, t2.* FROM t1, t2 WHERE t1.b = t2.b
      """.stripMargin
    util.verifySqlNotExpected(sqlQuery, "Reused")
  }

  @Test
  def testReuseSubPlan_Calc(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c FROM x WHERE c LIKE 'test%')
        |(SELECT r.a, LOWER(c) AS c, y.e FROM r, y WHERE r.a = y.d)
        |UNION ALL
        |(SELECT r.a, LOWER(c) AS c, y.e FROM r, y WHERE r.a = y.d)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_Correlate(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c, v FROM x, LATERAL TABLE(STRING_SPLIT(c, '-')) AS T(v))
        |SELECT * FROM r r1, r r2 WHERE r1.v = r2.v
      """.stripMargin
    // TODO the sub-plan of Correlate should be reused,
    // however the digests of Correlates are different
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_Exchange(): Unit = {
    util.tableEnv.alterTableStats("x", Some(TableStats(100000000L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(1000000000L)))
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c FROM x WHERE c LIKE 'test%')
        |SELECT * FROM r, y WHERE a = d AND e > 10
        |UNION ALL
        |SELECT * FROM r, y WHERE a = d AND f <> ''
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_OverWindow(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, RANK() OVER (ORDER BY c DESC) FROM x)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b < 100 AND r2.b > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_ScanTable(): Unit = {
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    val sqlQuery =
      """
        |(SELECT a FROM t WHERE a > 10)
        |UNION ALL
        |(SELECT a FROM t WHERE b > 10)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_HashAggregate(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "SortAgg")
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_SortAggregate(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashAgg")
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_Sort(): Unit = {
    util.tableEnv.getConfig.getParameters.setBoolean(TableConfig.SQL_EXEC_SORT_ENABLE_RANGE, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c ORDER BY a, b DESC)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r1.a > 1 AND r2.b < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_Source_With_Limit(): Unit = {
    util.tableEnv.getConfig.setTableSourceReuse(true)
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b FROM x LIMIT 10)
        |SELECT r1.a, r1.b, r2.a FROM r r1, r r2 WHERE r1.a = r2.b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_Limit(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b FROM x LIMIT 10)
        |SELECT r1.a, r1.b, r2.a FROM r r1, r r2 WHERE r1.a = r2.b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_SortLimit(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c ORDER BY a, b DESC LIMIT 10)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r1.a > 1 AND r2.b < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_SortMergeJoin(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,NestedLoopJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_HashJoin(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReuseSubPlan_Union(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT a, c FROM x WHERE b > 10 UNION ALL SELECT d, f FROM y WHERE e < 100)
        |SELECT r1.a, r1.c, r2.c FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testBreakupDeadlock_HashJoin(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a FROM x LIMIT 10)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testBreakupDeadlock_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a FROM x LIMIT 10)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEnableReuseTableSource(): Unit = {
    util.tableEnv.getConfig.setTableSourceReuse(true)
    val sqlQuery =
      """
        |WITH t AS (SELECT x.a AS a, x.b AS b, y.d AS d, y.e AS e FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.tableEnv.alterTableStats("x", Some(TableStats(100000000L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(1000000000L)))
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDisableReuseTableSource(): Unit = {
    util.tableEnv.getConfig.setTableSourceReuse(false)
    val sqlQuery =
      """
        |WITH t AS (SELECT x.a AS a, x.b AS b, y.d AS d, y.e AS e FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.tableEnv.alterTableStats("x", Some(TableStats(100000000L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(1000000000L)))
    util.verifyPlan(sqlQuery)
  }

  @Test @Ignore // FIXME: BLINK-16477898
  def testNestedReusableSubPlan(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sqlQuery =
      """
        |WITH v1 AS (
        | SELECT
        |   SUM(b) sum_b,
        |   AVG(SUM(b)) OVER (PARTITION BY c, e) avg_b,
        |   RANK() OVER (PARTITION BY c, e ORDER BY c, e) rn,
        |   c, e
        | FROM x, y
        | WHERE x.a = y.d AND c IS NOT NULl AND e > 10
        | GROUP BY c, e
        |),
        |   v2 AS (
        | SELECT
        |    v11.c,
        |    v11.e,
        |    v11.avg_b,
        |    v11.sum_b,
        |    v12.sum_b psum,
        |    v13.sum_b nsum,
        |    v12.avg_b avg_b2
        |  FROM v1 v11, v1 v12, v1 v13
        |  WHERE v11.c = v12.c AND v11.c = v13.c AND
        |    v11.e = v12.e AND v11.e = v13.e AND
        |    v11.rn = v12.rn + 1 AND
        |    v11.rn = v13.rn - 1
        |)
        |SELECT * from v2 WHERE c <> '' AND sum_b - avg_b > 3
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicUdfOnProject(): Unit = {
    util.tableEnv.registerFunction("random_udf", RandomUdf)

    val sqlQuery =
      """
        |(SELECT a, random_udf() FROM x WHERE a > 10)
        |UNION ALL
        |(SELECT a, random_udf() FROM x WHERE a > 10)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicUdfOnFilter(): Unit = {
    util.tableEnv.registerFunction("random_udf", RandomUdf)

    val sqlQuery =
      """
        |(SELECT a FROM x WHERE b > random_udf(a))
        |UNION ALL
        |(SELECT a FROM x WHERE b > random_udf(a))
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_DeterministicJoinCondition(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x FULL OUTER JOIN y ON ABS(a) = ABS(d) OR c = f
        |           WHERE b > 1 and e < 2)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicJoinCondition_Disabled(): Unit = {
    util.tableEnv.registerFunction("random_udf", RandomUdf)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(false)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x FULL OUTER JOIN y ON random_udf(a) = random_udf(d) OR c = f
        |           WHERE b > 1 and e < 2)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicJoinCondition_Enabled(): Unit = {
    util.tableEnv.registerFunction("random_udf", RandomUdf)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(true)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x FULL OUTER JOIN y ON random_udf(a) = random_udf(d) OR c = f
        |           WHERE b > 1 and e < 2)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicAgg_Disabled(): Unit = {
    util.tableEnv.registerFunction("MyFirst", new IntFirstValueAggFunction)
    util.tableEnv.registerFunction("MyLast", new LongLastValueAggFunction)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(false)

    val sqlQuery =
      """
        |WITH r AS (SELECT c, MyFirst(a) a, MyLast(b) b FROM x GROUP BY c)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicAgg_Enabled(): Unit = {
    util.tableEnv.registerFunction("MyFirst", new IntFirstValueAggFunction)
    util.tableEnv.registerFunction("MyLast", new LongLastValueAggFunction)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(true)

    val sqlQuery =
      """
        |WITH r AS (SELECT c, MyFirst(a) a, MyLast(b) b FROM x GROUP BY c)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }


  @Ignore  // [BLINK-14928444]
  @Test
  def testReusableSubPlan_NonDeterministicOverWindowAgg(): Unit = {
    util.tableEnv.registerFunction("MyFirst", new IntFirstValueAggFunction)
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, MyFirst(c) OVER (PARTITION BY c ORDER BY c DESC) FROM x)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b < 100 AND r2.b > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Ignore  // [BLINK-14928444]
  @Test
  def testReusableSubPlan_NonDeterministicOverWindowAgg_Disabled(): Unit = {
    util.tableEnv.registerFunction("MyFirst", new IntFirstValueAggFunction)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(false)

    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, MyFirst(c) OVER (PARTITION BY c ORDER BY c DESC) FROM x)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b < 100 AND r2.b > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Ignore  // [BLINK-14928444]
  @Test
  def testReusableSubPlan_NonDeterministicOverWindowAgg_Enabled(): Unit = {
    util.tableEnv.registerFunction("MyFirst", new IntFirstValueAggFunction)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(true)

    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, MyFirst(c) OVER (PARTITION BY c ORDER BY c DESC) FROM x)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b < 100 AND r2.b > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicUDTF_Disabled(): Unit = {
    util.tableEnv.registerFunction("MyTable", new MyTableFunc)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(false)

    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c, s FROM x, LATERAL TABLE(MyTable(c)) AS T(s))
        |SELECT * FROM r r1, r r2 WHERE r1.c = r2.s
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusableSubPlan_NonDeterministicUDTF_Enabled(): Unit = {
    util.tableEnv.registerFunction("MyTable", new MyTableFunc)
    util.tableEnv.getConfig.setNondeterministicOperatorReuse(true)

    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c, s FROM x, LATERAL TABLE(MyTable(c)) AS T(s))
        |SELECT * FROM r r1, r r2 WHERE r1.c = r2.s
      """.stripMargin
    // TODO the sub-plan of Correlate should be reused,
    // however the digests of Correlates are different
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGetOptimizedPlanWithCost(): Unit = {
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.printSql(sqlQuery, explainLevel = SqlExplainLevel.ALL_ATTRIBUTES)
  }
}

object RandomUdf extends ScalarFunction {
  val random = new Random()

  def eval(): Int = {
    random.nextInt()
  }

  def eval(v: Int): Int = {
    v + random.nextInt()
  }

  override def isDeterministic: Boolean = false
}

class MyTableFunc extends TableFunction[String] {

  val random = new Random()

  def eval(str: String): Unit = {
    val values = str.split("#")
    values.drop(random.nextInt(values.length)).foreach(collect)
  }

  override def isDeterministic: Boolean = false
}
