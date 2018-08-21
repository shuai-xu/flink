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
package org.apache.flink.table.tpc

import java.util

import org.apache.calcite.sql.SqlExplainLevel
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel
import org.apache.flink.table.plan.resource.DefaultResultPartitionCalculator
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.tpc.TpcUtils.getTpcHQuery
import org.apache.flink.table.util.FlinkRelOptUtil
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Ignore, Test}
import org.scalatest.prop.PropertyChecks

@Ignore
@RunWith(classOf[Parameterized])
class TpcHBatchExecParallelismTest(caseName: String) extends QueryTest with PropertyChecks {

  def getDataFile(tableName: String): String = {
    getClass.getResource(s"/tpch/data/$tableName/$tableName.tbl").getFile
  }

  @Before
  def before(): Unit = {
    for ((tableName, schema) <- TpcHSchemaProvider.schemaMap) {
      lazy val tableSource = CsvTableSource.builder()
          .path(getDataFile(tableName))
          .fields(schema.getFieldNames, schema.getFieldTypes, schema.getFieldNullables)
          .fieldDelimiter("|")
          .lineDelimiter("\n")
          .build()
      tEnv.registerTableSource(tableName, tableSource)
    }
    tEnv.streamEnv.setParallelism(3)
    tEnv.getConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_REL_PROCESS_ROWS_PER_PARTITION,
      2000
    )
    TpcUtils.disableBroadcastHashJoin(tEnv)
  }

  def execute(caseName: String): Unit = {
    println(s"Begin to explain $caseName")
    val table = parseQuery(getTpcHQuery(caseName))
    val optimized = tEnv.optimize(table.getRelNode)
    optimized.asInstanceOf[RowBatchExecRel].accept(
      new DefaultResultPartitionCalculator(tEnv.getConfig, tEnv))
    val sb = FlinkRelOptUtil.toString(
      optimized.asInstanceOf[RowBatchExecRel],
      detailLevel=SqlExplainLevel.NO_ATTRIBUTES,
      printResource = true)
    print(sb)
  }

  @Test
  def test(): Unit = {
    execute(caseName)
  }
}

object TpcHBatchExecParallelismTest {

  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[String] = {
    util.Arrays.asList(
      "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
      "11", "12", "13", "14", "15_1", "16", "17", "18", "19", "20", "21", "22"
     //  15 plan: VIEW is unsupported
    )
  }
}
