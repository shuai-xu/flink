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
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalCalc, FlinkLogicalNativeTableScan, FlinkLogicalSink, FlinkLogicalTableSourceScan, FlinkLogicalValues}
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.util.{NullableBatchExecTableTestUtil, TableTestBatchExecBase}

import org.apache.calcite.rel.rules.{FilterCalcMergeRule, FilterToCalcRule, ProjectCalcMergeRule, ProjectToCalcRule}
import org.apache.calcite.tools.RuleSets
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
class FlinkAggregateRemoveRuleTest(fieldsNullable: Boolean) extends TableTestBatchExecBase {
  val util: NullableBatchExecTableTestUtil = nullableBatchExecTestUtil(fieldsNullable)

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      // use volcano planner because
      // rel.getCluster.getPlanner is volcano planner used in FlinkAggregateRemoveRule
      FlinkVolcanoProgramBuilder.newBuilder
        .add(RuleSets.ofList(
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          FlinkCalcMergeRule.INSTANCE,
          FlinkAggregateRemoveRule.INSTANCE,
          FlinkLogicalAggregate.BATCH_CONVERTER,
          FlinkLogicalCalc.CONVERTER,
          FlinkLogicalValues.CONVERTER,
          FlinkLogicalTableSourceScan.CONVERTER,
          FlinkLogicalNativeTableScan.CONVERTER,
          FlinkLogicalSink.CONVERTER))
        .setTargetTraits(Array(FlinkConventions.LOGICAL))
        .build())
    val calciteConfig = new CalciteConfigBuilder().setBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Int, String)]("MyTable1", 'a, 'b, 'c)
    util.addTable[(Int, Int, String)]("MyTable2", Set(Set("a")), 'a, 'b, 'c)
  }

  @Test
  def testAggRemove_GroupKeyIsNotUnique(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, MAX(c) from MyTable1 GROUP BY a")
  }

  @Test
  def testAggRemove_WithoutFilter1(): Unit = {
    util.verifyPlan("SELECT a, b + 1, c, s FROM (" +
      "SELECT a, MIN(b) AS b, SUM(b) AS s, MAX(c) AS c FROM MyTable2 GROUP BY a)")
  }

  @Test
  def testAggRemove_WithoutFilter2(): Unit = {
    util.verifyPlan("SELECT a, SUM(b) AS s FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_WithDistinct(): Unit = {
    util.verifyPlan(
      "SELECT a, SUM(DISTINCT b), MAX(DISTINCT b), MIN(DISTINCT c) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_WithoutGroupBy1(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT MAX(a), SUM(b), MIN(c) FROM MyTable2")
  }

  @Test
  def testAggRemove_WithoutGroupBy2(): Unit = {
    util.verifyPlan("SELECT MAX(a), SUM(b), MIN(c) FROM (VALUES (1, 2, 3)) T(a, b, c)")
  }

  @Test
  def testAggRemove_WithoutAggCall(): Unit = {
    util.verifyPlan("SELECT a, b FROM MyTable2 GROUP BY a, b")
  }

  @Test
  def testAggRemove_WithFilter(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, MIN(c) FILTER (WHERE b > 0), MAX(b) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_Count(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, COUNT(c) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_CountStar(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, COUNT(*) FROM MyTable2 GROUP BY a")
  }

}

object FlinkAggregateRemoveRuleTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
