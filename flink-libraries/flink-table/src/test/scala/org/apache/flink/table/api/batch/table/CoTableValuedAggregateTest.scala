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

package org.apache.flink.table.api.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggregate.TestInnerJoinFunc
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.Test

/**
 * Test for testing co-table-valued aggregate plans.
 */
class CoTableValuedAggregateTest extends TableTestBatchExecBase {

  @Test
  def testCoGroupTableValuedAggregate(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
  }

  @Test
  def testCoGroupTableValuedAggregateAndAs(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .as('k, 'a, 'b, 'c, 'd, 'e, 'f)

    util.verifyPlan(resultTable)
  }

  @Test
  def testCoNonGroupTableValuedAggregate(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
  }

  @Test
  def testPushDownDistributionIntoCoTableValuedAgg(): Unit = {
    val util = batchTestUtil()

    val configuration = new Configuration()
    configuration.setInteger(TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM, 4)
    util.tableEnv.getConfig.getParameters.addAll(configuration)
    util.tableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "SortAgg")

    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .groupBy('l1)
      .select('l1, 'f0.count)

    util.verifyPlan(resultTable)
  }
}
