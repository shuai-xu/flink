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

package org.apache.flink.table.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.{Before, Test}

class SubsectionOptimizationTest extends TableTestBatchExecBase {

  private val util = batchExecTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable3", 'a, 'b, 'c)
    util.getTableEnv.getConfig.setSubsectionOptimization(true)
  }

  @Test
  def testSingleSink1(): Unit = {
    util.tableEnv.scan("SmallTable3")
      .groupBy('c)
      .select('c, 'b.count as 'cnt)
      .writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink2(): Unit = {
    val table = util.tableEnv.scan("SmallTable3")
    val table1 = table.where('a <= 10).select('a as 'a1, 'b)
    val table2 = table.where('a >= 0).select('a, 'b, 'c)
    val table3 = table2.where('b >= 5).select('a as 'a2, 'c)
    val table4 = table2.where('b < 5).select('a as 'a3, 'c as 'c1)
    val table5 = table1.join(table3, 'a1 === 'a2).select('a1, 'b, 'c as 'c2)
    val table6 = table4.join(table5, 'a1 === 'a3).select('a1, 'b, 'c1)
    table6.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks(): Unit = {
    val query = "SELECT SUM(a) AS sum_a, c FROM SmallTable3 GROUP BY c"
    val table = util.getTableEnv.sqlQuery(query)
    val result1 = table.select('sum_a.sum as 'total_sum)
    val result2 = table.select('sum_a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def tesSQL(): Unit = {
    util.tableEnv.sqlQuery("SELECT c, count(a) as cnt FROM SmallTable3 GROUP BY c")
      .writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

}
