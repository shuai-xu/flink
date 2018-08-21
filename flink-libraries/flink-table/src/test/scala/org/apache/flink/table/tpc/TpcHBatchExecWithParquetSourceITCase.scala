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

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.sources.parquet.ParquetVectorizedColumnRowTableSource
import org.apache.flink.table.tpc.TpcUtils.getTpcHQuery
import org.apache.flink.table.types.DataTypes
import org.apache.flink.test.util.TestBaseUtils
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}
import org.scalatest.prop.PropertyChecks

@RunWith(classOf[Parameterized])
class TpcHBatchExecWithParquetSourceITCase(caseName: String) extends QueryTest with PropertyChecks {

  def getDataPath(tableName: String, schema: Schema): String = {
    getClass.getResource(s"/tpch/parquet-data/$tableName/$tableName.parquet").getPath
  }

  @Before
  def before(): Unit = {
    for ((tableName, schema) <- TpcHSchemaProvider.schemaMap) {
     lazy val tableSource = new ParquetVectorizedColumnRowTableSource(
        new Path(getDataPath(tableName, schema)),
        schema.getFieldTypes.map(DataTypes.internal),
        schema.getFieldNames,
        schema.getFieldNullables,
        true
      )
      tEnv.registerTableSource(tableName, tableSource)
    }
    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM, 3)
    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_SORT_DEFAULT_LIMIT, -1)
    TpcUtils.disableBroadcastHashJoin(tEnv)
    TpcUtils.disableRangeSort(tEnv)
    tEnv.getConfig.setJoinReorderEnabled(true)
  }

  def execute(caseName: String): Unit = {
    val result = TpcUtils.formatResult(executeQuery(parseQuery(getTpcHQuery(caseName))))
    TestBaseUtils.compareResultAsText(result, TpcUtils.getTpcHResult(caseName))
  }

  @Test
  def test(): Unit = {
    execute(caseName)
  }

}

object TpcHBatchExecWithParquetSourceITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[String] = {
    util.Arrays.asList(
      "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
      "11", "12", "13", "14", "15_1", "16", "17", "18", "19", "20", "21", "22"
      // 15 plan: VIEW is unsupported
    )
  }
}
