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
package com.alibaba.blink.launcher

import java.sql.Timestamp
import java.util.Properties

import com.alibaba.blink.launcher.util.SqlJobAdapter
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.factories.FlinkTableFactory
import org.apache.flink.sql.parser.ddl.SqlCreateTable
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.types.DataTypes

import scala.collection.JavaConverters._
import org.junit.Test
import org.junit.Assert.{assertArrayEquals, assertFalse, assertNotNull, assertTrue}

class JobLauncherTest {

  @Test
  def testComputedColumn(): Unit = {
    val sql  =
      s"""
        |create function to_timestamp as '${classOf[ToTimestamp].getCanonicalName}';
        |
        |create table test_stream (
        |  a bigint,
        |  b varchar,
        |  c as to_timestamp(a),
        |  proc as PROCTIME(),
        |  d int,
        |  primary key (a),
        |  watermark wm1 for c as withOffset(c, 100)
        |) with (
        |  type = 'test',
        |  accessKey = 'xxx'
        |);
      """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val classLoader = Thread.currentThread().getContextClassLoader
    val env = TableEnvironment.getTableEnvironment(
        StreamExecutionEnvironment.getExecutionEnvironment)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerFunctions(env, sqlNodeInfoList, classLoader, null)
    SqlJobAdapter.registerTables(env, sqlNodeInfoList, new Properties(), classLoader)

    val ts = env.scan("test_stream")
    assertArrayEquals(
      Array("a", "b", "c", "proc", "d").asInstanceOf[Array[AnyRef]],
      ts.getSchema.getColumnNames.asInstanceOf[Array[AnyRef]])
    assertArrayEquals(
      Array(
        DataTypes.LONG,
        DataTypes.STRING,
        DataTypes.ROWTIME_INDICATOR,
        DataTypes.PROCTIME_INDICATOR,
        DataTypes.INT)
        .asInstanceOf[Array[AnyRef]],
      ts.getSchema.getTypes.asInstanceOf[Array[AnyRef]])
  }

  @Test
  def testViewColumnWithAlias() {
    val sql  =
      s"""
        |CREATE TABLE test_source_stream (
        |  a bigint,
        |  b varchar,
        |  PRIMARY KEY (a, b)
        |) with (
        |  type = 'test'
        |);
        |
        |CREATE VIEW test_view(aa)
        |AS
        |  SELECT a + 1
        |  FROM test_source_stream;
        |
        |CREATE VIEW test_view2(aaa)
        |AS
        |  select aa
        |  from
        |  (select * from test_view) in1
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
    assertNotNull(tEnv.scan("test_source_stream"))
    assertNotNull(tEnv.scan("test_view"))
    assertNotNull(tEnv.scan("test_view2"))
  }

  @Test
  def testViewWithMultiColumnAndFilter() {
    val sql  =
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
    assertNotNull(tEnv.scan("test_source_stream"))
    assertNotNull(tEnv.scan("test_view"))
    assertNotNull(tEnv.scan("test_view2"))
  }

  @Test
  def testExtractSTSUIDFromRoleArn() {
    val sql  =
      s"""
         |CREATE TABLE test_source_stream (
         |  a bigint,
         |  b varchar,
         |  c varchar,
         |  d varchar,
         |  PRIMARY KEY (a, b)
         |) with (
         |  type = 'test',
         |  roleArn = 'acs:ram::1234567:role/aliyunstreamdefaultrole'
         |);
         |
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val properties = new Properties()
    properties.setProperty("stsRegionId", "shanghai")
    properties.setProperty("stsAccessId", "a")
    properties.setProperty("stsAccesskey", "b")

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)

    for (sqlNodeInfo <- sqlNodeInfoList.asScala){
      if (sqlNodeInfo.getSqlNode.isInstanceOf[SqlCreateTable] ) {
        val sqlCreateTable = sqlNodeInfo.getSqlNode.asInstanceOf[SqlCreateTable]
        val tableName = sqlCreateTable.getTableName.toString
        //set with properties
        val propertyList = sqlCreateTable.getPropertyList
        val p = SqlJobAdapter.
          createTableProperties(propertyList, properties, "test_source")
        assert("1234567".equals(p.getString("stsUid", null)))
      }
    }
  }

  @Test
  def testBatchEnvironment(): Unit = {
    val sql  =
      s"""
         |CREATE TABLE test_source_stream (
         |  a bigint,
         |  b varchar,
         |  PRIMARY KEY (a, b)
         |) with (
         |  type = 'CSV',
         |  path = 'xx'
         |);
         |
         |CREATE VIEW test_view(aa)
         |AS
         |  SELECT a + 1
         |  FROM test_source_stream;
         |
         |CREATE VIEW test_view2(aaa)
         |AS
         |  select aa
         |  from
         |  (select * from test_view) in1
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    SqlJobAdapter.registerViews(tEnv, sqlNodeInfoList)
    assertNotNull(tEnv.scan("test_source_stream"))
    assertNotNull(tEnv.scan("test_view"))
    assertNotNull(tEnv.scan("test_view2"))
  }

  @Test
  def testStreamEnvironment_AlterUniqueKeys(): Unit = {
    val sql  =
      s"""
         |CREATE TABLE test_source (
         |  a bigint,
         |  b varchar,
         |  c bigint,
         |  UNIQUE(c)
         |) with (
         |  type = 'CSV',
         |  path = 'xx'
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
    SqlJobAdapter.registerViews(tEnv, sqlNodeInfoList)
    val table = tEnv.scan("test_source")
    assertNotNull(table)
    val scanNode = table.getRelNode
    val mq = scanNode.getCluster.getMetadataQuery
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(2)))
  }

  @Test
  def testBatchEnvironment_AlterUniqueKeys(): Unit = {
    val sql  =
      s"""
         |CREATE TABLE test_source (
         |  a bigint,
         |  b varchar,
         |  c bigint,
         |  PRIMARY KEY (a, b),
         |  UNIQUE(c)
         |) with (
         |  type = 'CSV',
         |  path = 'xx'
         |);
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    SqlJobAdapter.registerViews(tEnv, sqlNodeInfoList)
    val table = tEnv.scan("test_source")
    assertNotNull(table)
    val scanNode = table.getRelNode
    val mq = scanNode.getCluster.getMetadataQuery
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(2)))
  }

  @Test
  def testBatchEnvironment_MultiUniqueKeys(): Unit = {
    val sql  =
      s"""
         |CREATE TABLE test_source (
         |  a bigint,
         |  b varchar,
         |  c bigint,
         |  d bigint,
         |  e bigint,
         |  PRIMARY KEY (a, b),
         |  UNIQUE(c),
         |  UNIQUE(d, e)
         |) with (
         |  type = 'CSV',
         |  path = 'xx'
         |);
       """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)
    SqlJobAdapter.registerViews(tEnv, sqlNodeInfoList)
    val table = tEnv.scan("test_source")
    assertNotNull(table)
    val scanNode = table.getRelNode
    val mq = scanNode.getCluster.getMetadataQuery
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(0)))
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(1)))
    assertTrue(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(2)))
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(3)))
    assertFalse(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(4)))
    assertTrue(mq.areColumnsUnique(scanNode, ImmutableBitSet.of(3, 4)))
  }

  @Test(expected = classOf[ RuntimeException ])
  def testRegisterInvalidUDF(): Unit = {
    val sql  =
      s"""
         |create function to_timestamp as '${classOf[InvalidUDF].getCanonicalName}';
         |
        |create table test_stream (
         |  a bigint,
         |  b varchar,
         |  c as to_timestamp(a),
         |  proc as PROCTIME(),
         |  d int,
         |  primary key (a),
         |  watermark wm1 for c as withOffset(c, 100)
         |) with (
         |  type = 'test',
         |  accessKey = 'xxx'
         |);
      """.stripMargin

    FlinkTableFactory.DIRECTORY.put("TEST", classOf[TestingTableFactory].getCanonicalName)
    val classLoader = Thread.currentThread().getContextClassLoader
    val env = TableEnvironment.getTableEnvironment(
      StreamExecutionEnvironment.getExecutionEnvironment)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerFunctions(env, sqlNodeInfoList, classLoader, null)
    SqlJobAdapter.registerTables(env, sqlNodeInfoList, new Properties(), classLoader)

    val ts = env.scan("test_stream")
    assertArrayEquals(
      Array("a", "b", "c", "proc", "d").asInstanceOf[Array[AnyRef]],
      ts.getSchema.getColumnNames.asInstanceOf[Array[AnyRef]])
    assertArrayEquals(
      Array(
        DataTypes.LONG,
        DataTypes.STRING,
        DataTypes.ROWTIME_INDICATOR,
        DataTypes.PROCTIME_INDICATOR,
        DataTypes.INT)
        .asInstanceOf[Array[AnyRef]],
      ts.getSchema.getTypes.asInstanceOf[Array[AnyRef]])
  }
}

class ToTimestamp extends ScalarFunction {
  def eval(l: Long): Timestamp = {
    new Timestamp(l)
  }
}

class InvalidUDF {
  def eval(l: Long): Timestamp = {
    new Timestamp(l)
  }
}
