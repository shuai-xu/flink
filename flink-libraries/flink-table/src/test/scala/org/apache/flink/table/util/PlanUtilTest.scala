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

package org.apache.flink.table.util

import java.nio.file.{Files, Paths}

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraphGenerator
import org.apache.flink.streaming.api.graph.StreamGraphGenerator.Context
import org.apache.flink.table.api.java.BatchTableEnvironment
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.api.{Table, TableConfig, TableConfigOptions, TableEnvironment}
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms
import org.apache.flink.table.plan.rules.physical.batch.{BatchExecNestedLoopJoinRule, BatchExecSortMergeJoinRule}
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.utils.CommonTestData._
import org.apache.flink.table.sinks.{CollectRowTableSink, CollectTableSink}
import ExecResourceUtil.InferMode
import org.apache.flink.table.util.PlanUtil.toPlanWihMetrics
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID
import org.junit._

import scala.collection.JavaConversions._

import scala.io.Source

class PlanUtilTest extends AbstractTestBase {

  private val conf = QueryTest.initConfigForTest(new TableConfig)
  private var env: StreamExecutionEnvironment = _
  private var tableEnv: BatchTableEnvironment = _
  private var tmpFile: String = _

  @Before
  def before(): Unit = {
    conf.getConf.setBoolean(TableConfigOptions.SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED, true)
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tableEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    tableEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_DEFAULT_PARALLELISM, 2)
    tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_INFER_RESOURCE_MODE,
      InferMode.NONE.toString
    )
    tableEnv.getConfig.setCalciteConfig(new CalciteConfigBuilder().build())
  }

  @Test
  def testDumpPlanWithMetricsOfFilter(): Unit = {
    tmpFile = "filter-plan.tmp"
    tableEnv.registerCollection("Table3", data3, type3, "a, b, c")
    val sqlQuery = "SELECT * FROM Table3 WHERE a > 0 " +
        "ORDER BY a OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY"
    val table = tableEnv.sqlQuery(sqlQuery)
    val sink = createCollectTableSink(
      Array("a, b, c"),
      Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING))
    execute(table, sink)
    val expected = readFromResource("testFilterPlanWithMetrics.out")
    assertPlanEqualsIgnoreStageIds(expected)
  }

  @Test
  def testDumpPlanWithMetricsOfJoin(): Unit = {
    tableEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_CBO_JOIN_REORDER_ENABLED, true)
    val program = tableEnv.getConfig.getCalciteConfig.getBatchPrograms
        .getFlinkRuleSetProgram(FlinkBatchPrograms.PHYSICAL)
    program.get.remove(RuleSets.ofList(
      BatchExecSortMergeJoinRule.INSTANCE,
      BatchExecNestedLoopJoinRule.INSTANCE))
    tmpFile = "join-plan.tmp"
    tableEnv.registerCollection("Table3", get3DataRow()._1, get3DataRow()._2, "a, b, c")
    tableEnv.registerCollection("Table5", get5DataRow()._1, get5DataRow()._2, "d, e, f, g, h")
    val table = tableEnv.sqlQuery("SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b")
    val sink = createCollectTableSink(
      Array("c", "g"),
      Array(DataTypes.STRING, DataTypes.STRING))
    execute(table, sink)
    val expected = readFromResource("testJoinPlanWithMetrics.out")
    assertPlanEqualsIgnoreStageIds(expected)
  }

  @After
  def after(): Unit = {
    // clean file
    Files.deleteIfExists(Paths.get(tmpFile))
  }

  /**
    * Creates collect table sink using given fieldNames and fieldTypes.
    *
    * @param fieldNames field names of table
    * @param fieldTypes field types of table
    * @return collect table sink using given fieldNames and fieldTypes.
    */
  private def createCollectTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[DataType]): CollectTableSink[Row] = {
    val originSink = new CollectRowTableSink()
    val sink = originSink.configure(fieldNames, fieldTypes).asInstanceOf[CollectTableSink[Row]]
    val sinkTyp = sink.getOutputType
    val typeSerializer = DataTypes.to(sinkTyp).createSerializer(env.getConfig)
        .asInstanceOf[TypeSerializer[Row]]
    sink.init(typeSerializer, new AbstractID().toString)
    sink
  }

  /**
    * Generates stream graph using given sink and input table, executes it
    *
    * @param inputOfSink input table of sink
    * @param sink        table slink to output result
    */
  private def execute(inputOfSink: Table, sink: CollectTableSink[Row]): Unit = {
    val sinkTransformation = tableEnv.toBoundedStream(inputOfSink, sink).getTransformation
    setDumpFileToConfig()
    val streamGraph = StreamGraphGenerator.generate(
      Context.buildBatchProperties(tableEnv.streamEnv),
      _root_.java.util.Arrays.asList(sinkTransformation))
    val jobResult = env.execute(streamGraph)
    streamGraph.dumpPlanWithMetrics(tmpFile, jobResult)
  }

  /**
    * Sets dumpFileOfPlanWithMetrics to execute environment.
    */
  private def setDumpFileToConfig(): Unit = {
    conf.getConf.setString(TableConfigOptions.SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH, tmpFile)
    tableEnv.setupOperatorMetricCollect()
  }

  /**
    * Asserts actual plan is equals to expected plan.
    *
    * Note: Stage {id} is ignored, because id keeps incrementing in test class
    * while StreamExecutionEnvironment is up.
    * source csv path is ignored, because it keeps changes.
    *
    * @param expected expected plan
    */
  private def assertPlanEqualsIgnoreStageIds(expected: String): Unit = {
    val actual = scala.io.Source.fromFile(tmpFile).mkString
    Assert.assertEquals(replaceString(expected), replaceString(actual))
  }

  private def readFromResource(name: String): String = {
    val inputStream = getClass.getResource("/explain/" + name).getFile
    Source.fromFile(inputStream).mkString
  }

  private def replaceString(s: String): String = {
    s.replaceAll("\\r\\n", "\n")
      .replaceAll("Stage \\d+ : ", "")
      .replaceAll(",\\s__id__=\\[\\d+\\]", "")
      .replaceAll("SortLimitRuleLocal , metric=\\{\"rowCount\":\\d+", "")
  }

}
