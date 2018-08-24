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
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.dataformat.ColumnarRow
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.InsertRuntimeFilterRule
import org.apache.flink.table.sources.parquet._
import org.apache.flink.table.tpc.STATS_MODE.STATS_MODE
import org.apache.flink.table.types.{DataTypes, InternalType}
import org.apache.flink.table.util.FlinkRelOptUtil
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConversions._

/**
  * This class is used to get optimized TPC-DS queries plan with TableStats.
  * The TableStats of each table in TpcDsTableStatsProvider is static result,
  * and is independent of any specific TableSource.
  */
@org.junit.Ignore // TODO: unstable; causing too much trouble for everybody; disabled until fixed.
@RunWith(classOf[Parameterized])
class TpcDsBatchExecPlanTest(
    caseName: String,
    factor: Int,
    statsMode: STATS_MODE,
    explainLevel: SqlExplainLevel,
    joinReorderEnabled: Boolean,
    printOptimizedResult: Boolean)
  extends TableTestBatchExecBase
  with PropertyChecks {

  private val util = batchExecTestUtil()
  private val tEnv = util.tableEnv

  @Before
  def before(): Unit = {
    for ((tableName, schema) <- TpcDsSchemaProvider.schemaMap) {
      lazy val tableSource = new TestParquetTableSource(
        tableName,
        schema.getFieldTypes.map(DataTypes.internal),
        schema.getFieldNames,
        schema.getFieldNullables)
      tEnv.registerTableSource(tableName, tableSource, schema.getUniqueKeys)
    }
    // alter TableStats
    for ((tableName, tableStats) <- TpcDsTableStatsProvider.getTableStatsMap(factor, statsMode)) {
      tEnv.alterTableStats(tableName, Some(tableStats))
    }
    TpcUtils.disableParquetFilterPushDown(tEnv)
    tEnv.getConfig.setJoinReorderEnabled(joinReorderEnabled)
    tEnv.getConfig.setSubPlanReuse(true)
    tEnv.getConfig.setTableSourceReuse(false)
    tEnv.getConfig.getParameters.setBoolean(TableConfig.SQL_RUNTIME_FILTER_ENABLE, true)
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
    BatchExecRel.resetReuseIdCounter()
    InsertRuntimeFilterRule.resetBroadcastIdCounter()
    if (printOptimizedResult) {
      val table = tEnv.sqlQuery(sqlQuery)
      val optimized = tEnv.optimize(table.getRelNode)
      val result = FlinkRelOptUtil.toString(optimized, detailLevel = explainLevel)
      println(s"caseName:$caseName, factor: $factor, statsMode:$statsMode\n$result")
    } else {
      util.verifyPlan(sqlQuery)
    }
  }
}

object TpcDsBatchExecPlanTest {
  @Parameterized.Parameters(name = "caseName={0}, factor={1}, statsMode={2}, joinReorder={4}")
  def parameters(): util.Collection[Array[Any]] = {
    val factor = 1000
    val explainLevel = SqlExplainLevel.ALL_ATTRIBUTES
    val joinReorderEnabled = true
    val printResult = false
    util.Arrays.asList(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
      "q11", "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99"
    ).flatMap { s =>
      Seq(
        Array(s, factor, STATS_MODE.ROW_COUNT, explainLevel, joinReorderEnabled, printResult),
        Array(s, factor, STATS_MODE.PART, explainLevel, joinReorderEnabled, printResult),
        Array(s, factor, STATS_MODE.FULL, explainLevel, joinReorderEnabled, printResult)
      )
    }
  }

}
