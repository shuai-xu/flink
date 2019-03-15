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
package org.apache.flink.table.runtime.stream.sql

import java.io.File

import org.apache.flink.api.scala._
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithAggTestBase.AggMode
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.runtime.utils.{StreamingWithAggTestBase, TestingRetractTableSink}
import org.apache.flink.table.types.DataTypes
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class StateReuseITCase(
    aggMode: AggMode,
    miniBatch: MiniBatchMode,
    backend: StateBackendMode)
  extends StreamingWithAggTestBase(aggMode, miniBatch, backend) {

  val leftData1 = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (3L, 3, "Hello")
  )

  val leftData2 = List(
    (4L, 1, "Hi"),
    (5L, 2, "Hello"),
    (6L, 3, "Hello world")
  )

  val rightData1 = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (2L, 3, "Hello world"),
    (3L, 4, "Hello world")
  )

  val rightData2 = List(
    (1L, 0, "Hi"),
    (3L, 2, "Hello"),
    (4L, 0, "Hello world")
  )

  @Before
  override def before(): Unit = {
    super.before()

    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_STATE_REUSE, true)

    // retain the checkpoints after job finished
    tEnv.execEnv.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
  }

  private def getLatestCheckpointPath(checkpointBasePath: File): Option[String] = {
    if (checkpointBasePath.exists()) {
      val checkpointPaths = checkpointBasePath.listFiles()
      // the directory structure is: /checkpointBasePath/jobID/chk-xxx
      if (checkpointPaths.length == 1) {
        return Some(checkpointPaths(0).getAbsolutePath)
      }
    }
    None
  }

  /**
    * Executes the job and retains the checkpoint.
    */
  private def executeStage1(): Unit = {
    tEnv.compile()

    try {
      tEnv.execEnv.execute()
    } catch {
      case e: Throwable =>
        // ignore this exception which is expected and used to save
        // the last checkpoint after job finished.
        if (!e.getMessage.contains("Job finished normally")) {
          throw e
        }
    } finally {
      tEnv.sinkNodes.clear()
    }
  }

  /**
    * Executes the job with the latest checkpoint.
    */
  private def executeStage2(): Unit = {
    tEnv.compile()

    getLatestCheckpointPath(baseCheckpointPath) match {
      case Some(latestCheckpointPath) =>
        tEnv.execEnv.execute(
          StreamExecutionEnvironment.DEFAULT_JOB_NAME,
          SavepointRestoreSettings.forResumePath(latestCheckpointPath, true))

      case _ => tEnv.execEnv.execute()
    }
  }

  @Test
  def testGroupAggregation(): Unit = {
    val sink = new TestingRetractTableSink()
      .configure(Array("a", "b"), Array(DataTypes.INT, DataTypes.LONG))
      .asInstanceOf[TestingRetractTableSink]

    // execute the old sql
    val t1 = retainStateDataSource(leftData1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("source", t1)

    val oldSql =
      s"""
         |SELECT b, sum(a)
         |FROM source
         |GROUP BY b
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(oldSql), sink, "sink")
    executeStage1()

    // execute the new sql: add WHERE b > 1
    val t2 = failingDataSource(leftData2).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerOrReplaceTable("source", t2)
    val newSql =
      s"""
         |SELECT b, sum(a)
         |FROM source
         |WHERE b > 1
         |GROUP BY b
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(newSql), sink, "sink")
    executeStage2()

    val expected = List("1,1", "2,7", "3,9")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testWindowAggregate(): Unit = {
    val sink = new TestingRetractTableSink()
      .configure(
        Array("a", "b", "c"),
        Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.TIMESTAMP))
      .asInstanceOf[TestingRetractTableSink]

    // execute the old sql
    val t1 = retainStateDataSource(leftData1)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
              [(Long, Int, String)](0L))
      .toTable(tEnv, 'ts.rowtime, 'b, 'c)
    tEnv.registerTable("source", t1)

    val oldSql =
      s"""
         |SELECT SUM(b),
         |  HOP_START(ts, INTERVAL '0.003' SECOND, INTERVAL '0.003' SECOND),
         |  HOP_END(ts, INTERVAL '0.003' SECOND, INTERVAL '0.003' SECOND)
         |FROM source
         |GROUP BY HOP(ts, INTERVAL '0.003' SECOND, INTERVAL '0.003' SECOND)
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(oldSql), sink, "sink")
    executeStage1()

    // execute the new sql: add WHERE b <> 2
    val t2 = failingDataSource(leftData2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset
              [(Long, Int, String)](0L))
      .toTable(tEnv, 'ts.rowtime, 'b, 'c)
    tEnv.registerOrReplaceTable("source", t2)
    val newSql =
      s"""
         |SELECT SUM(b),
         |  HOP_START(ts, INTERVAL '0.003' SECOND, INTERVAL '0.003' SECOND),
         |  HOP_END(ts, INTERVAL '0.003' SECOND, INTERVAL '0.003' SECOND)
         |FROM source
         |WHERE b <> 2
         |GROUP BY HOP(ts, INTERVAL '0.003' SECOND, INTERVAL '0.003' SECOND)
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(newSql), sink, "sink")
    executeStage2()

    val expected = List("3,1970-01-01 00:00:00.0,1970-01-01 00:00:00.003",
                        "3,1970-01-01 00:00:00.006,1970-01-01 00:00:00.009",
                        "4,1970-01-01 00:00:00.003,1970-01-01 00:00:00.006")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testOverAggregate(): Unit = {
    val sink = new TestingRetractTableSink()
      .configure(
        Array("a", "b", "c"),
        Array(DataTypes.INT, DataTypes.LONG, DataTypes.LONG))
      .asInstanceOf[TestingRetractTableSink]

    // execute the old sql
    val t1 = retainStateDataSource(leftData1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("source", t1)

    val oldSql =
      s"""
         |SELECT b,
         |  SUM(a) OVER (
         |    PARTITION BY b ORDER BY proctime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
         |  COUNT(c) OVER (
         |    PARTITION BY b ORDER BY proctime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
         |FROM source
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(oldSql), sink, "sink")
    executeStage1()

    // execute the new sql: add WHERE a <> 5
    val t2 = failingDataSource(leftData2)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerOrReplaceTable("source", t2)
    val newSql =
      s"""
         |SELECT b,
         |  SUM(a) OVER (
         |    PARTITION BY b ORDER BY proctime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
         |  COUNT(c) OVER (
         |    PARTITION BY b ORDER BY proctime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
         |FROM source
         |WHERE a <> 5
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(newSql), sink, "sink")
    executeStage2()

    val expected = List("1,1,1", "2,2,1", "3,3,1", "1,5,2", "3,9,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoin(): Unit = {
    val sink = new TestingRetractTableSink()
      .configure(
        Array("a", "b", "c"),
        Array(DataTypes.LONG, DataTypes.INT, DataTypes.INT))
      .asInstanceOf[TestingRetractTableSink]

    // execute the old sql
    val t1 = retainStateDataSource(leftData1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("leftT", t1)
    val t2 = retainStateDataSource(rightData1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("rightT", t2)

    val oldSql =
      s"""
         |SELECT
         |  leftT.a, leftT.b, rightT.b
         |FROM leftT JOIN rightT
         |  ON leftT.a = rightT.a AND leftT.b < rightT.b
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(oldSql), sink, "sink")
    executeStage1()

    // execute the new sql: left.b < right.b change to left.b > right.b
    val t3 = failingDataSource(leftData2).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerOrReplaceTable("leftT", t3)
    val t4 = failingDataSource(rightData2).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerOrReplaceTable("rightT", t4)
    val newSql =
      s"""
         |SELECT
         |  leftT.a, leftT.b, rightT.b
         |FROM leftT JOIN rightT
         |  ON leftT.a = rightT.a AND leftT.b > rightT.b
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(newSql), sink, "sink")
    executeStage2()

    val expected = List("2,2,3", "3,3,4", "1,1,0", "3,3,2", "4,1,0")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAddingGroupAggregate(): Unit = {
    val sink = new TestingRetractTableSink()
      .configure(Array("a", "b"), Array(DataTypes.LONG, DataTypes.LONG))
      .asInstanceOf[TestingRetractTableSink]

    tEnv.execEnv.setParallelism(1)

    // execute the old sql
    val t1 = retainStateDataSource(leftData1).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("source", t1)

    val oldSql =
      s"""
         |SELECT sum(a), count(distinct c)
         |FROM source
         |GROUP BY b
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(oldSql), sink, "sink")
    executeStage1()

    // execute the new sql: adding another group aggregate
    val t2 = failingDataSource(leftData2).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerOrReplaceTable("source", t2)
    val newSql =
      s"""
         |(
         |  SELECT sum(a), count(distinct c)
         |  FROM source
         |  GROUP BY b
         |)
         |UNION ALL
         |(
         |  SELECT sum(a), count(b)
         |  FROM source
         |  GROUP BY c
         |)
       """.stripMargin
    tEnv.writeToSink(tEnv.sqlQuery(newSql), sink, "sink")
    executeStage2()

    val expected = List("5,1", "7,1", "9,2", "4,1", "5,1", "6,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
