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
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

class QueryConfigConfigurationModeTest() extends TableTestBase {

  private var streamUtil: StreamTableTestUtil = streamTestUtil()


  @Before
  def setUp(): Unit = {
    streamUtil = streamTestUtil()
    streamUtil.addTable[(Long, Int, String)](
      "MyTable", 'a, 'b, 'c)
  }

  @Test
  def testEnableMicroBatch(): Unit = {
    streamUtil.tableEnv.getConfig
      .enableMicroBatch
      .withMicroBatchTriggerTime(1000L)
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testEnableMiniBatch(): Unit = {
    streamUtil.tableEnv.getConfig.enableMiniBatch
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testEnablePartialAgg(): Unit = {
    streamUtil.tableEnv.getConfig.enableMiniBatch
    streamUtil.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_AGG_PARTIAL_ENABLED, true)
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testEnableByParameters(): Unit = {
    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.BLINK_MINIBATCH_ALLOW_LATENCY, 6000L)
    streamUtil.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.BLINK_MINIBATCH_SIZE, 200)
    streamUtil.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_AGG_PARTIAL_ENABLED, true)
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }
}
