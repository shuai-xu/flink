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

import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.sources.parquet.ParquetVectorizedColumnRowTableSource
import org.apache.flink.table.tpc.TpcUtils.getTpcHQuery
import org.apache.flink.table.util.ExecResourceUtil.InferMode
import org.junit.{Before, Ignore}

@Ignore
class TpchBenchmark extends QueryTest {

  val parquetPath = "/Users/zhixin/data/tpch/parquet"
  val caseName = "01"
  val runCount = 100

  override def getConfiguration: Configuration = {
    val config = new Configuration()
    config.setLong("taskmanager.memory.size", 500L)
    config
  }

  @Before
  def before(): Unit = {
    for ((tableName, schema) <- TpcHSchemaProvider.schemaMap) {
      val schema = TpcHSchemaProvider.schemaMap(tableName)
      lazy val tableSource = new ParquetVectorizedColumnRowTableSource(
        new Path(s"$parquetPath/$tableName"),
        schema.getFieldTypes,
        schema.getFieldNames,
        schema.getFieldNullables,
        true
      )
      tEnv.registerTableSource(tableName, tableSource)
    }
    for ((tableName, tableStats) <- TpchTableStatsProvider.getTableStatsMap(
      1000, STATS_MODE.FULL)) {
      tEnv.alterTableStats(tableName, Some(tableStats))
    }
    TpcUtils.disableParquetFilterPushDown(tEnv)
    tEnv.getConfig.setJoinReorderEnabled(true)
    tEnv.getConfig.getParameters.setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE,
      InferMode.NONE.toString)
    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM, 1)
    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_SORT_DEFAULT_LIMIT, -1)

    conf.getParameters.setInteger(TableConfig.SQL_EXEC_SORT_BUFFER_MEM, 10)
    conf.getParameters.setInteger(TableConfig.SQL_EXEC_HASH_JOIN_TABLE_MEM, 80)
    conf.getParameters.setInteger(TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM, 80)
    conf.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_MEM, 10)
    conf.getParameters.setInteger(TableConfig.SQL_EXEC_EXTERNAL_BUFFER_MEM, 10)
  }

  @org.junit.Test
  def test(): Unit = {
    for (i <- 0 until runCount) {
      println(TpcUtils.formatResult(executeQuery(parseQuery(getTpcHQuery(caseName)))))
    }
  }
}
