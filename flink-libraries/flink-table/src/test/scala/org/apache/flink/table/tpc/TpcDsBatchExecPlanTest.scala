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

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.dataformat.ColumnarRow
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.InsertRuntimeFilterRule
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.plan.util.{FlinkNodeOptUtil, FlinkRelOptUtil}
import org.apache.flink.table.sources.parquet.{ParquetTableSource, ParquetVectorizedColumnRowTableSource}
import org.apache.flink.table.tpc.STATS_MODE.STATS_MODE
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.sql.SqlExplainLevel
import org.junit.{Before, Test}
import org.scalatest.prop.PropertyChecks

import scala.collection.Seq

/**
  * This class is used to get optimized TPC-DS queries plan with TableStats.
  * The TableStats of each table in TpcDsTableStatsProvider is static result,
  * and is independent of any specific TableSource.
  */
abstract class TpcDsBatchExecPlanTest(
    caseName: String,
    factor: Int,
    statsMode: STATS_MODE,
    explainLevel: SqlExplainLevel,
    joinReorderEnabled: Boolean,
    printOptimizedResult: Boolean)
  extends TableTestBase with PropertyChecks {

  private val util = batchTestUtil()
  private val tEnv = util.tableEnv

  @Before
  def before(): Unit = {
    for ((tableName, schema) <- TpcDsSchemaProvider.schemaMap) {
      lazy val tableSource = new TestParquetTableSource(
        tableName,
        schema.getFieldTypes,
        schema.getFieldNames,
        schema.getFieldNullables)
      tEnv.registerTableSource(tableName, tableSource, schema.getUniqueKeys)
    }
    // alter TableStats
    for ((tableName, tableStats) <- TpcDsTableStatsProvider.getTableStatsMap(factor, statsMode)) {
      tEnv.alterTableStats(tableName, Some(tableStats))
    }
    TpcUtils.disableParquetFilterPushDown(tEnv)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, joinReorderEnabled)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, false)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ENABLED, true)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD,
                                            10 * 1024 * 1024)
  }

  // create a new ParquetTableSource to override `createTableSource` and `getTableStats` methods
  private class TestParquetTableSource(
    tableName: String,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean]) extends ParquetVectorizedColumnRowTableSource(
    new Path("/tmp"), fieldTypes, fieldNames, fieldNullables, true) {

    override protected def createTableSource(
      fieldTypes: Array[InternalType],
      fieldNames: Array[String],
      fieldNullables: Array[Boolean]): ParquetTableSource[ColumnarRow] = {
      val tableSource = new TestParquetTableSource(
        tableName,
        fieldTypes,
        fieldNames,
        fieldNullables)
      tableSource.setFilterPredicate(filterPredicate)
      tableSource.setFilterPushedDown(filterPushedDown)
      tableSource
    }

    override def getTableStats: TableStats = {
      // the `filterPredicate` in TPC-DS queries can not drop any row group for current test data,
      // we can directly use the static statistics.
      // TODO if the test data or TPC-DS queries are changed, the statistics should also be updated.
      TpcDsTableStatsProvider.getTableStatsMap(factor, STATS_MODE.PART).get(tableName).orNull
    }

    override def explainSource(): String = {
      s"TestParquetTableSource -> " +
        s"selectedFields=[${fieldNames.mkString(", ")}];" +
        s"filterPredicates=[${if (filterPredicate == null) "" else filterPredicate.toString}]"
    }
  }

  @Test
  def test(): Unit = {
    val sqlQuery = TpcUtils.getTpcDsQuery(caseName, factor)
    InsertRuntimeFilterRule.resetBroadcastIdCounter()
    if (printOptimizedResult) {
      val table = tEnv.sqlQuery(sqlQuery)
      val optimized = tEnv.optimize(table.getRelNode)
      val optimizedNodes = tEnv.translateToExecNodeDag(Seq(optimized))
      require(optimizedNodes.length == 1)
      val result =  FlinkNodeOptUtil.treeToString(optimizedNodes.head, detailLevel = explainLevel)
      println(s"caseName:$caseName, factor: $factor, statsMode:$statsMode\n$result")
    } else {
      util.verifyPlan(sqlQuery, explainLevel, printPlanBefore = false)
    }
  }
}
