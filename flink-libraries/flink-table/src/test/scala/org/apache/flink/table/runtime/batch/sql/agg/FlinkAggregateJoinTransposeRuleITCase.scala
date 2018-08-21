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

package org.apache.flink.table.runtime.batch.sql.agg

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.logical.FlinkAggregateJoinTransposeRule
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.runtime.batch.sql.QueryTest.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.types.DataTypes
import org.junit.{Before, Test}

import _root_.scala.collection.JavaConverters._

class FlinkAggregateJoinTransposeRuleITCase extends QueryTest {

  @Before
  def before(): Unit = {
    val programs = tEnv.getConfig.getCalciteConfig.getBatchExecPrograms
    // remove FlinkAggregateJoinTransposeRule from logical program (volcano planner)
    programs.getFlinkRuleSetProgram(FlinkBatchExecPrograms.LOGICAL)
      .getOrElse(throw new TableException(s"${FlinkBatchExecPrograms.LOGICAL} does not exist"))
      .remove(RuleSets.ofList(FlinkAggregateJoinTransposeRule.EXTENDED))

    // add FlinkAggregateJoinTransposeRule to hep program
    // to make sure that the aggregation must be pushed down
    programs.addBefore(
      FlinkBatchExecPrograms.LOGICAL,
      "FlinkAggregateJoinTransposeRule",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          AggregateProjectMergeRule.INSTANCE,
          FlinkAggregateJoinTransposeRule.EXTENDED
        )).build()
    )

    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM, 3)
    registerCollection("T3", data3, type3, "a, b, c", nullablesOfData3)
    tEnv.registerTableSource("T2",
      CommonTestData.createCsvTableSource(
        Seq(row(1, 1L, "X"),
          row(1, 2L, "Y"),
          row(2, 3L, null),
          row(2, 4L, "Z")),
        Array("a2", "b2", "c2"),
        Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING)
      ),
      Set(Set("b2").asJava).asJava
    )
  }

  @Test
  def testPushCountAggThroughJoinOverUniqueColumn(): Unit = {
    checkResult(
      "SELECT COUNT(A.a) FROM (SELECT DISTINCT a FROM T3) AS A JOIN T3 AS B ON A.a=B.a",
      Seq(row(21))
    )
  }

  @Test
  def testPushSumAggThroughJoinOverUniqueColumn(): Unit = {
    checkResult(
      "SELECT SUM(A.a) FROM (SELECT DISTINCT a FROM T3) AS A JOIN T3 AS B ON A.a=B.a",
      Seq(row(231))
    )
  }

  @Test
  def testSomeAggCallColumnsAndJoinConditionColumnsIsSame(): Unit = {
    checkResult(
      "SELECT MIN(a2), MIN(b2), a, b, COUNT(c2) FROM " +
        "(SELECT * FROM T2, T3 WHERE b2 = b) t GROUP BY b, a",
      Seq(row(1, 1, 1, 1, 1), row(1, 2, 2, 2, 1), row(1, 2, 3, 2, 1),
        row(2, 3, 4, 3, 0), row(2, 3, 5, 3, 0), row(2, 3, 6, 3, 0),
        row(2, 4, 10, 4, 1), row(2, 4, 7, 4, 1), row(2, 4, 8, 4, 1), row(2, 4, 9, 4, 1))
    )
  }

}
