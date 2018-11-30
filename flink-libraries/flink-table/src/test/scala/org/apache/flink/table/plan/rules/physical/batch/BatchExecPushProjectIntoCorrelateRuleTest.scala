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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{TableFunc1, TableTestBatchExecBase}

import org.junit.Test

class BatchExecPushProjectIntoCorrelateRuleTest extends TableTestBatchExecBase {

  @Test
  def testRemoveCalc(): Unit = {
    val util = batchExecTestUtil()
    val tEnv = util.tableEnv
    util.addTable[(String, String)]("MyTable", 'a, 'b)
    tEnv.registerFunction("split", new TableFunc1)
    val table = tEnv.sqlQuery(
      """
        | SELECT a1 as a FROM MyTable JOIN LATERAL TABLE(split(a)) AS T(a1) ON TRUE
      """.stripMargin)
    util.verifyPlan(table)
  }

}
