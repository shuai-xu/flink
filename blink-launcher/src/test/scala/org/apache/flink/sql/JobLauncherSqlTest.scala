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
package org.apache.flink.sql

import java.util.Properties

import com.alibaba.blink.launcher.TestingTableFactory
import com.alibaba.blink.launcher.util.{JobBuildHelper, SqlJobAdapter}
import org.apache.calcite.plan.RelOptUtil
import org.apache.flink.factories.FlinkTableFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment, TableException}
import org.apache.flink.table.plan.schema.{StreamTableSourceTable, TableSourceSinkTable}
import org.apache.flink.table.types.{DataTypes, DecimalType}
import org.junit.Assert.assertEquals
import org.junit.Test

class JobLauncherSqlTest {

  @Test
  /**
    * Fix issue: https://workitem.aone.alibaba-inc.com/project/577104/issue/13932997
    * (NPE while applying rule ProjectFilterTransposeRule)
    * mv this case to flink package due to `StreamTableEnvironment.optimize` is private[flink]
    */
  def testViewColumnWithAliasAndFilter() {
    val sql =
      s"""
         |CREATE TABLE test_source_stream (
         |  a bigint,
         |  b varchar,
         |  c varchar,
         |  d varchar,
         |  PRIMARY KEY (a, b)
         |) with (
         |  type = 'test'
         |);
         |
        |CREATE VIEW test_view(aa, bb, cc)
         |AS
         |  SELECT
         |  a + 1 as a1,
         |  b as b1,
         |  c
         |  FROM test_source_stream
         |  WHERE  a > 0;
         |
        |CREATE VIEW test_view2(aaa, ccc)
         |AS
         |  select aa, cc
         |  from
         |  (select * from test_view) in1
         |  WHERE  aa > 1;
         |
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    SqlJobAdapter.registerViews(tEnv, sqlNodeInfoList)

    val rel = tEnv.scan("test_view2").getRelNode
    tEnv.optimize(rel, updatesAsRetraction = false)
  }

  @Test
  def testDecimal(): Unit = {
     val sql =
      s"""
         |CREATE TABLE test_source_stream (
         |  a decimal,
         |  b decimal(1, 1),
         |  c decimal(1, 0),
         |  d decimal(0, 0)
         |) with (
         |  type = 'test'
         |);
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    val tableSchema =
      tEnv.getTable("test_source_stream").get.asInstanceOf[TableSourceSinkTable[_]]
              .tableSourceTable.get.tableSource.getTableSchema
    assertEquals(
      tableSchema.getType("a").get,
      DataTypes.createDecimalType(DecimalType.DEFAULT.precision,
        DecimalType.DEFAULT.scale))
    assertEquals(
      tableSchema.getType("b").get,
      DataTypes.createDecimalType(1, 1))
    assertEquals(
      tableSchema.getType("c").get,
      DataTypes.createDecimalType(1, 0))
    assertEquals(
      tableSchema.getType("d").get,
      DataTypes.createDecimalType(0, 0))
  }

  @Test
  def testProcessOutputStatements(): Unit = {
    val sql =
      s"""
         |CREATE TABLE test_sink_stream (
         |  a decimal,
         |  b decimal(1, 1),
         |  c decimal(1, 0),
         |  d decimal(0, 0)
         |) with (
         |  type = 'test'
         |);
         |
         |insert into test_sink_stream
         |select cast(1 as decimal),
         |      cast(1 as decimal(1, 1)),
         |      cast(1 as decimal(1, 0)),
         |      cast(1 as decimal(0, 0))
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    try {
      SqlJobAdapter.processOutputStatements(true,
        tEnv, sqlNodeInfoList, new Properties(), "", 1, -1, "", "", "", false, false)
    } catch {
      case e1: TableException => assert(e1.msg
        .contains("Values source input is not supported currently."))
      case _ =>
    }
  }

  @Test
  def testPk(): Unit = {
    val sql =
      s"""
         |CREATE TABLE test_source_stream (
         |  a bigint,
         |  b varchar,
         |  c as to_timestamp(a),
         |  d varchar,
         |  PRIMARY KEY (a, b),
         |  watermark wm1 for c as withOffset(c, 100)
         |) with (
         |  type = 'test'
         |);
         |
         |CREATE VIEW test_view(aa, bb, cc)
         |AS
         |  SELECT
         |  c,
         |  a as a1,
         |  b as b1
         |  FROM test_source_stream
         |
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    SqlJobAdapter.registerViews(tEnv, sqlNodeInfoList)

    val rel = tEnv.scan("test_view").getRelNode
    val optimized = tEnv.optimize(rel, updatesAsRetraction = false)

    assertEquals(
      "StreamExecLastRow(" +
      "LastRow=[LastRow: (key: (a1, b1), select: (c, a1, b1))])\n  " +
      "StreamExecExchange(distribution=[hash[a1, b1]])\n    " +
      "StreamExecCalc(select=[c, a AS a1, b AS b1])\n      " +
      "StreamExecWatermarkAssigner(" +
      "fields=[[a, b, c, d]], rowtimeField=[c], watermarkOffset=[100])\n        " +
      "StreamExecCalc(select=[a, b, TO_TIMESTAMP(a) AS c, d])\n         " +
      " StreamExecTableSourceScan(table=[[_DataStreamTable_0]], fields=[a, b, d])\n",
      RelOptUtil.toString(optimized))
  }

  @Test
  def testWithQuery(): Unit = {
    val sql =
      s"""
         |CREATE TABLE `src` (
         |  key bigint,
         |  v varchar
         |) WITH (
         |  `path`='/path/to/src',
         |  `type`='csv',
         |  `fieldDelim`='|'
         |);
         |CREATE TABLE `oneandzero` (
         |  i int
         |) WITH (
         |  `path`='/path/to/oneandzero',
         |  `type`='csv',
         |  `fieldDelim`='|'
         |);
         |
         |with src_null as (
         |  select * from src union all (select cast(5444 as bigint) as key, cast(null as
         |  varchar) as v from oneandzero where i = 0)
         |)
         |select key, v, count(*)
         |from src_null b
         |where NOT EXISTS (select key from src_null where src_null.v <> b.v)
         |group by key, v
         |having count(*) not in (select count(*) from src_null s1 where s1.key > '9' and s1.v <>
         | b.v group by s1.key);
         |
       """.stripMargin

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = new BatchTableEnvironment(env, new TableConfig)
    JobBuildHelper.buildSqlJobByString(
      false,
      Thread.currentThread().getContextClassLoader,
      new Properties(),
      null,
      tEnv,
      sql,
      "/path/to/preview/csv",
      1,
      100000,
      ",",
      "\n",
      "\"",
      false,
      false)
  }
}
