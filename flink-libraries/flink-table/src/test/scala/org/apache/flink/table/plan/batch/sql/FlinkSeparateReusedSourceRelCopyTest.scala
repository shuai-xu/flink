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

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCalc, BatchExecExchange, BatchExecHashJoin, BatchExecRel, BatchExecReused}
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.assertNotEquals
import org.junit.{Before, Test}

class FlinkSeparateReusedSourceRelCopyTest extends TableTestBatchExecBase {

  private val util = batchExecTestUtil()

  @Before
  def before(): Unit = {
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("d", "e", "f")))
    util.tableEnv.alterTableStats("x", Some(TableStats(100L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(100L)))
    BatchExecRel.resetReuseIdCounter()
  }

  @Test
  def testReusedSource(): Unit = {
    val sqlQuery = "SELECT * FROM x x1, x x2 WHERE x1.a = x2.a"
    val resultTable = util.getTableEnv.sqlQuery(sqlQuery)
    val optimized = util.getTableEnv.optimize(resultTable.getRelNode)
    val joinRelNode = optimized.asInstanceOf[BatchExecHashJoin]
    val exchangeRelNode = joinRelNode.getRight.asInstanceOf[BatchExecExchange]
    assertEquals(joinRelNode.getLeft.toString, exchangeRelNode.getInput.toString)
    assertNotEquals(joinRelNode.getLeft, exchangeRelNode.getInput)
  }

  @Test
  def testReuseNode(): Unit = {
    util.tableEnv.getConfig.setSubPlanReuse(true)
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |    SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a < 300
      """.stripMargin
    val resultTable = util.getTableEnv.sqlQuery(sqlQuery)
    val optimized = util.getTableEnv.optimize(resultTable.getRelNode)
    val hashJoinNode = optimized.asInstanceOf[BatchExecCalc].
        getInput.asInstanceOf[BatchExecHashJoin]
    val reuseNode = hashJoinNode.getRight.asInstanceOf[BatchExecExchange].
        getInput.asInstanceOf[BatchExecCalc].
        getInput.asInstanceOf[BatchExecReused]
    val referencedNode = hashJoinNode.getLeft.asInstanceOf[BatchExecExchange].
        getInput.asInstanceOf[BatchExecCalc].getInput
    assertTrue(reuseNode.getInput == referencedNode)
  }
}
