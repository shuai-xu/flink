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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggregate.{TVAGGDoNothingWithMerge, TVAGGDoNothing, SimpleTVAGG}
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.Test

/**
 * Test for testing table-valued aggregate plans.
 */
class TableValuedAggregateTest extends TableTestBatchExecBase {

  @Test
  def testSimpleTVAGG(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val testTVAGGFun = new SimpleTVAGG
    val resultTable = sourceTable
      .groupBy('b).aggApply(testTVAGGFun('a)).select(1983, 'f0)

    util.verifyPlan(resultTable)

  }

  @Test
  def testTVAGGDoNothing(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val testTVAGGFun = new TVAGGDoNothing

    val resultTable = sourceTable
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c)).as('g, 'v).select('v)

    util.verifyPlan(resultTable)
  }

  @Test
  def testTVAGGDoNothingWithMerge(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val testTVAGGFun = new TVAGGDoNothingWithMerge

    val resultTable = sourceTable
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c)).as('g, 'v).select('v)

    util.verifyPlan(resultTable)
  }
}
