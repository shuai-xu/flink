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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.runtime.batch.sql.QueryTest.row
import org.apache.flink.table.runtime.utils.CommonTestData.createCsvTableSource
import org.apache.flink.table.types.DataTypes
import org.junit.Test

import scala.collection.Seq

class DeadlockBreakupITCase extends QueryTest {

  @Test
  def testReuseSubPlan_ReusedNodeIsNotBarrierNode(): Unit = {
    tEnv.getConfig.setSubPlanReuse(true)
    tEnv.getConfig.setTableSourceReuse(true)
    // make sure that buffer pool of channel can't hold all data
    val data = (1 until 100000).map(row(_))
    tEnv.registerTableSource("t", createCsvTableSource(data, Array("a"), Array(DataTypes.LONG)))
    checkResult(
      """
        |WITH r AS (SELECT a FROM t LIMIT 100000)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a ORDER BY r1.a LIMIT 3
      """.stripMargin,
      Seq(row(1), row(2), row(3))
    )
  }

  @Test
  def testReuseSubPlan_ReusedNodeIsBarrierNode(): Unit = {
    tEnv.getConfig.setSubPlanReuse(true)
    tEnv.getConfig.setTableSourceReuse(true)
    // make sure that buffer pool of channel can't hold all data
    val data = (1 until 100000).map {
      i => row(i.toLong, i.toLong, ((i / 10) + 1).toLong)
    }
    tEnv.registerTableSource("t", createCsvTableSource(data, Array("a", "b", "c"),
      Array(DataTypes.LONG, DataTypes.LONG, DataTypes.LONG)))
    checkResult(
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM t GROUP BY c)
        |    SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a < 300
      """.stripMargin,
      Seq(row(1, 45, 45, 1, 45, 45), row(2, 145, 145, 2, 145, 145), row(3, 245, 245, 3, 245, 245))
    )
  }

  @Test
  def testDataStreamScan(): Unit = {
    tEnv.getConfig.setSubPlanReuse(false)
    // make sure that buffer pool of channel can't hold all data
    val data = (1 until 100000).map(i => row(i.toLong))
    tEnv.registerCollection("t", data, new RowTypeInfo(Types.LONG), 'a)
    checkResult(
      "SELECT * FROM t INTERSECT SELECT * FROM t",
      data
    )
  }

}
