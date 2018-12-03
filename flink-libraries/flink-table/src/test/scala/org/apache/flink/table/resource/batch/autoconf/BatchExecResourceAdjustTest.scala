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

package org.apache.flink.table.resource.batch.autoconf

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.resource.batch.BatchExecResourceTest
import org.apache.flink.table.resource.batch.BatchExecResourceTest.MockTableSource
import org.apache.flink.table.tpc.{STATS_MODE, TpcHSchemaProvider, TpchTableStatsProvider}
import org.apache.flink.table.util.{ExecResourceUtil, BatchExecTableTestUtil, TableTestBatchExecBase}
import org.junit.{Before, Test}

class BatchExecResourceAdjustTest extends TableTestBatchExecBase {

  private var util: BatchExecTableTestUtil = _

  @Before
  def before(): Unit = {
    util = batchExecTestUtil()
    util.getTableEnv.getConfig.setSubsectionOptimization(false)
    util.getTableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_EXEC_INFER_RESOURCE_MODE,
      ExecResourceUtil.InferMode.ALL.toString
    )
    BatchExecResourceTest.setResourceConfig(util.getTableEnv.getConfig)
  }

  @Test
  def testAdjustAccordingToFewCPU(): Unit = {
    setAdjustResource(util, 100)
    testResource(util)
  }

  @Test
  def testAdjustAccordingToMuchResource(): Unit = {
    setAdjustResource(util, Double.MaxValue)
    testResource(util)
  }

  private def testResource(util: BatchExecTableTestUtil): Unit = {
    val customerSchema = TpcHSchemaProvider.getSchema("customer")
    val colStatsOfCustomer =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("customer")
    val customer = new MockTableSource(customerSchema, colStatsOfCustomer.get)
    util.addTable("customer", customer)

    val ordersSchema = TpcHSchemaProvider.getSchema("orders")
    val colStastOfOrders =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("orders")
    val orders = new MockTableSource(ordersSchema, colStastOfOrders.get)
    util.addTable("orders", orders)
    val lineitemSchema = TpcHSchemaProvider.getSchema("lineitem")
    val colStatsOfLineitem =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("lineitem")
    val lineitem = new MockTableSource(lineitemSchema, colStatsOfLineitem.get)
    util.addTable("lineitem", lineitem)

    val sqlQuery = "select c.c_name, sum(l.l_quantity)" +
        " from customer c, orders o, lineitem l" +
        " where o.o_orderkey in ( " +
        " select l_orderkey from lineitem group by l_orderkey having" +
        " sum(l_quantity) > 300)" +
        " and c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey" +
        " group by c.c_name"
    util.verifyResource(sqlQuery)
  }

  private def setAdjustResource(
      util: BatchExecTableTestUtil,
      cpu: Double): Unit = {
    util.getTableEnv.getConfig.getParameters.setDouble(
      TableConfig.SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU,
      cpu
    )
  }
}
