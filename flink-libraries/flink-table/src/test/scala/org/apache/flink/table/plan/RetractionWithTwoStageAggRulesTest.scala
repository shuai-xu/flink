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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

class RetractionWithTwoStageAggRulesTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    val queryConfig = new StreamQueryConfig()
    queryConfig
      .withIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    queryConfig.enableMiniBatch
      .withMiniBatchTriggerTime(1000L)
      .withMiniBatchTriggerSize(3)
    queryConfig.enableLocalAgg
    util.tableEnv.setQueryConfig(queryConfig)
  }

  // one level unbounded groupBy
  @Test
  def testGroupBy(): Unit = {
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('number.count)

    util.verifyTrait(resultTable)
  }

  // two level unbounded groupBy
  @Test
  def testTwoGroupBy(): Unit = {
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    util.verifyTrait(resultTable)
  }

  // test binaryNode
  @Test
  def testBinaryNode(): Unit = {
    val lTable = util.addTable[(String, Int)]('word, 'number)
    val rTable = util.addTable[(String, Long)]('word_r, 'count_r)

    val resultTable = lTable
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .unionAll(rTable)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    util.verifyTrait(resultTable)
  }
}
