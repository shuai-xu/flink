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

import java.lang.{Integer => JInt}
import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{Consumer, Supplier}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{StreamQueryConfig, TableSchema, Types}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamingTestBase, StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.table.sources.{AsyncConfig, DimensionTableSource, IndexKey}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

class JoinDimensionTableITCase extends StreamingTestBase {

  val data = List(
    (1, 12, "Julian"),
    (2, 15, "Hello"),
    (3, 15, "Fabian"),
    (8, 11, "Hello world"),
    (9, 12, "Hello world!"))

  val dataWithNull = List(
    Row.of(null, new JInt(15), "Hello"),
    Row.of(new JInt(3), new JInt(15), "Fabian"),
    Row.of(null, new JInt(11), "Hello world"),
    Row.of(new JInt(9), new JInt(12), "Hello world!"))

  @Test
  def testJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian",
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnNullableKey(): Unit = {

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    val stream = env.fromCollection(dataWithNull)

    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22",
      "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource2
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields2(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    // pk is (id: Int, name: String)
    val dim = new TestDimensionTableSource2
    tEnv.registerTableSource("csvdim", dim)

    // test left table's join key define order diffs from right's
    val sql = "SELECT t1.id, t1.len, D.name FROM (select content, id, len FROM T) t1 JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON t1.content = D.name AND t1.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithConstantKey(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource2
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.content = D.name AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithStringConstantKey(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource2
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON D.name = 'Fabian' AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testJoinTemporalTableOnMultiConstantKey(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource2
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON D.name = 'Fabian' AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Fabian",
      "2,15,Fabian",
      "3,15,Fabian",
      "8,11,Fabian",
      "9,12,Fabian"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,Jark,22",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testLeftJoinTemporalTableOnNullableKey(): Unit = {

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    val stream = env.fromCollection(dataWithNull)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T LEFT OUTER JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,15,null",
      "3,15,Fabian",
      "null,11,null",
      "9,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }
  @Test
  def testLeftJoinTemporalTableOnMultKeyFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id and T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,null,null",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }


  @Test
  def testCollectOnNestedJoinDimTable(): Unit = {
    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6"))
    )
    tEnv.registerTable("src", env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c))

    // register dim table
    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("dim0", dim)

    val sql = "SELECT a, b, COLLECT(c) as `set` FROM src GROUP BY a, b"
    val view1 = tEnv.sqlQuery(sql)
    tEnv.registerTable("v1", view1)

    val toCompositeObj = ToCompositeObj
    tEnv.registerFunction("toCompObj", toCompositeObj)

    val sql1 =
      s"""
         |SELECT
         |  a, b, COLLECT(toCompObj(t.b, D.name, D.age, t.point)) as info
         |from (
         | select
         |  a, b, V.sid, V.point
         | from
         |  v1, unnest(v1.`set`) as V(sid, point)
         |) t
         |JOIN LATERAL dim0 FOR SYSTEM_TIME AS OF PROCTIME() AS D
         |ON t.b = D.id
         |group by t.a, t.b
       """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql1).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,{CompositeObj(1,Julian,11,45.6)=1}",
      "2,2,{CompositeObj(2,Jark,22,45.612)=1}",
      "3,2,{CompositeObj(2,Jark,22,41.6)=1}",
      "4,3,{CompositeObj(3,Fabian,33,45.2136)=1}",
      "5,3,{CompositeObj(3,Fabian,33,42.6)=1}")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}

@RunWith(classOf[Parameterized])
class AsyncJoinDimensionTableITCase(backend: StateBackendMode)
  extends StreamingWithStateTestBase(backend) {

  val data = List(
    (1, 12, "Julian"),
    (2, 15, "Hello"),
    (3, 15, "Fabian"),
    (8, 11, "Hello world"),
    (9, 12, "Hello world!"))

  @Test
  def testAsyncJoinTemporalTableOnMultiKeyFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    // pk is (id: Int, name: String)
    val dim = new TestDimensionTableSource2(true)
    tEnv.registerTableSource("csvdim", dim)

    // test left table's join key define order diffs from right's
    val sql = "SELECT t1.id, t1.len, D.name FROM (select content, id, len FROM T) t1 JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON t1.content = D.name AND t1.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testAsyncJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian",
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22",
      "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id " +
      "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
      "WHERE T.id > 1"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,null,null",
      "3,15,Fabian,Fabian,33",
      "8,11,Hello world,null,null",
      "9,12,Hello world!,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFieldsWithUdf(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)
    tEnv.registerFunction("mod1", TestMod)
    tEnv.registerFunction("wrapper1", TestWrapperUdf)

    val sql = "SELECT T.id, T.len, wrapper1(D.name) as name FROM T JOIN csvdim " +
      "for system_time as of PROCTIME() AS D " +
      "ON mod1(T.id, 4) = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

  @Test
  def testMinibatchAggAndAsyncLeftJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    val queryConfig = new StreamQueryConfig()
    queryConfig.enableMiniBatch
    queryConfig.withMiniBatchTriggerSize(1)
    queryConfig.withMiniBatchTriggerTime(2)
    tEnv.setQueryConfig(queryConfig)
    tEnv.registerTable("T", streamTable)

    val asyncConfig = new AsyncConfig
    asyncConfig.setBufferCapacity(1)
    asyncConfig.setTimeoutMs(5000L)
    val dim = new TestDimensionTableSource(true, asyncConfig, delayedReturn = 1000L)
    tEnv.registerTableSource("csvdim", dim)

    val sql1 = "SELECT max(id) as id from T group by len"

    val table1 = tEnv.sqlQuery(sql1)
    tEnv.registerTable("t1", table1)

    val sql2 = "SELECT t1.id, D.name, D.age FROM t1 LEFT JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON t1.id = D.id"

    val sink = new TestingRetractSink
    tEnv.sql(sql2).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "3,Fabian,33",
      "8,null,null",
      "9,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }


  @Test
  def testAsyncLeftJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = failingDataSource(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val dim = new TestDimensionTableSource(true)
    tEnv.registerTableSource("csvdim", dim)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvdim " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sql(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,Jark,22",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, dim.getFetcherResourceCount())
  }

}

// ------------------------------- Utils ----------------------------------------------


object TestWrapperUdf extends ScalarFunction {
  def eval(id: Int): Int = {
    id
  }

  def eval(id: String): String = {
    id
  }
}

object TestMod extends ScalarFunction {
  def eval(src: Int, mod: Int): Int = {
    src % mod
  }
}

class TestDimensionTableSource2(async: Boolean = false)
  extends TestDimensionTableSource(async) {

  override def getIndexes: util.Collection[IndexKey] = {
    Collections.singleton(IndexKey.of(true, 1, 2))   // primary key(id, name)
  }

  override def getLookupFunction(index: IndexKey): TestDoubleKeyFetcher = {
    fetcher = new TestDoubleKeyFetcher(0, 1) // new key idx mapping to keysRow
    fetcher
  }

  override def getAsyncLookupFunction(index: IndexKey): TestAsyncDoubleKeyFetcher = {
    asyncFetcher = new TestAsyncDoubleKeyFetcher(0, 1) // new idx mapping to keysRow
    asyncFetcher
  }
}

class TestDimensionTableSource(
  async: Boolean = false,
  conf: AsyncConfig = null,
  delayedReturn: Long = 0L) extends DimensionTableSource[BaseRow] {
  var fetcher: TestDoubleKeyFetcher = null
  var asyncFetcher: TestAsyncDoubleKeyFetcher = null

  override def getReturnType: DataType =
    DataTypes.internal(
      new BaseRowTypeInfo(
        classOf[GenericRow],
        Array(Types.INT, Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        Array( "age", "id", "name")))

  override def getLookupFunction(index: IndexKey): TestDoubleKeyFetcher = {
    fetcher = new TestSingleKeyFetcher(0)
    fetcher
  }

  override def isTemporal: Boolean = true

  override def isAsync: Boolean = async

  override def getAsyncConfig: AsyncConfig = {
    if (conf == null) {
      val config = new AsyncConfig
      config.setTimeoutMs(10000)
      config
    } else {
      conf
    }
  }

  override def getAsyncLookupFunction(index: IndexKey): TestAsyncDoubleKeyFetcher = {
    asyncFetcher = new TestAsyncSingleKeyFetcher(0)
    asyncFetcher
  }

  override def getIndexes: util.Collection[IndexKey] = {
    Collections.singleton(IndexKey.of(true, 1)) // primary key(id)
  }

  def getFetcherResourceCount(): Int = {
    if (async && null != asyncFetcher) {
      asyncFetcher.resourceCounter
    } else if (null != fetcher) {
      fetcher.resourceCounter
    } else {
      0
    }
  }

  /** Returns the table schema of the table source */
  override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType)
}

// lookup data table using id index
class TestSingleKeyFetcher(idIndex: Int) extends TestDoubleKeyFetcher(idIndex, 1) {

  override def flatMap(keysRow: BaseRow, collector: Collector[BaseRow]): Unit = {
    if (!keysRow.isNullAt(idIndex)) {
      val key = keysRow.getInt(idIndex)
      val value = TestTable.singleKeyTable.get(key)
      if (value.isDefined) {
        collect(value.get._1, value.get._2, value.get._3, collector)
      }
    }
    //else null
  }
}

class TestDoubleKeyFetcher(idIndex: Int, nameIndex: Int)
  extends RichFlatMapFunction[BaseRow, BaseRow] {
  var resourceCounter: Int = 0
  var reuse: GenericRow = _
  if (idIndex < 0 || nameIndex < 0) {
    throw new RuntimeException("Must join on primary keys")
  }

  override def open(parameters: Configuration): Unit = {
    resourceCounter += 1
    reuse = new GenericRow(3)
  }

  override def close(): Unit = {
    resourceCounter -= 1
  }

  override def flatMap(keysRow: BaseRow, collector: Collector[BaseRow]): Unit = {
    if (!keysRow.isNullAt(idIndex) && !keysRow.isNullAt(nameIndex)) {
      val key: (Int, String) = (keysRow.getInt(idIndex), keysRow.getString(nameIndex))
      val value = TestTable.doubleKeyTable.get(key)
      if (value.isDefined) {
        collect(value.get._1, value.get._2, value.get._3, collector)
      }
    }
    //else null
  }

  def collect(age: Int, id: Int, name: String, out: Collector[BaseRow]): Unit = {
    reuse.update(0, age)
    reuse.update(1, id)
    reuse.update(2, name)
    out.collect(reuse)
  }
}

class TestAsyncSingleKeyFetcher(leftKeyIdx: Int)
  extends TestAsyncDoubleKeyFetcher(leftKeyIdx, 1) {

  override def asyncInvoke(keysRow: BaseRow, asyncCollector: ResultFuture[BaseRow]): Unit = {
    CompletableFuture
      .supplyAsync(new RowSupplier(keysRow), executor)
      .thenAccept(new Consumer[BaseRow] {
        override def accept(t: BaseRow): Unit = {
          if (delayedReturn > 0L) {
            Thread.sleep(delayedReturn)
          }
          if (t == null) {
            asyncCollector.complete(Collections.emptyList[BaseRow]())
          } else {
            asyncCollector.complete(Collections.singleton(t))
          }
        }
      })
  }

  class RowSupplier(val keysRow: BaseRow) extends Supplier[BaseRow] {
    override def get(): BaseRow = {
      if (!keysRow.isNullAt(leftKeyIdx)) {
       val key: Int =  keysRow.getInt(leftKeyIdx)
        val value = TestTable.singleKeyTable.get(key)
        if (value.isDefined) {
          collect(value.get._1, value.get._2, value.get._3)
        } else {
          null
        }
      } else {
        null
      }
    }

    def collect(age: Int, id: Int, name: String): BaseRow = {
      val row = new GenericRow(3)
      row.update(0, age)
      row.update(1, id)
      row.update(2, name)
      row
    }
  }
}

class TestAsyncDoubleKeyFetcher(leftKeyIdx: Int, nameKeyIdx: Int)
  extends RichAsyncFunction[BaseRow, BaseRow] {

  var resourceCounter: Int = 0
  if (leftKeyIdx < 0 || nameKeyIdx < 0) {
    throw new RuntimeException("Must join on primary keys")
  }

  var delayedReturn: Long = 0L

  def setDelayedReturn(delayedReturn: Long): Unit = {
    this.delayedReturn = delayedReturn
  }

  @transient
  var executor: ExecutorService = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    resourceCounter += 1
    executor = Executors.newSingleThreadExecutor()
  }

  override def close(): Unit = {
    resourceCounter -= 1

    if (null != executor && !executor.isShutdown) {
      executor.shutdownNow()
    }
  }

  override def asyncInvoke(keysRow: BaseRow, asyncCollector: ResultFuture[BaseRow]): Unit = {
    CompletableFuture
      .supplyAsync(new RowSupplier(keysRow), executor)
      .thenAccept(new Consumer[BaseRow] {
        override def accept(t: BaseRow): Unit = {
          if (delayedReturn > 0L) {
            Thread.sleep(delayedReturn)
          }
          if (t == null) {
            asyncCollector.complete(Collections.emptyList[BaseRow]())
          } else {
            asyncCollector.complete(Collections.singleton(t))
          }
        }
      })
  }

  class RowSupplier(val keysRow: BaseRow) extends Supplier[BaseRow] {
    override def get(): BaseRow = {
      if (!keysRow.isNullAt(leftKeyIdx) && !keysRow.isNullAt(nameKeyIdx)) {
        val key: (Int, String) = (keysRow.getInt(leftKeyIdx), keysRow.getString(nameKeyIdx))
        val value = TestTable.doubleKeyTable.get(key)
        if (value.isDefined) {
          collect(value.get._1, value.get._2, value.get._3)
        } else {
          null
        }
      } else {
        null
      }
    }

    def collect(age: Int, id: Int, name: String): BaseRow = {
      val row = new GenericRow(3)
      row.update(0, age)
      row.update(1, id)
      row.update(2, name)
      row
    }
  }

}

object TestTable {
  // index by id
  val singleKeyTable: Map[Int, (Int, Int, String)] = Map(
    1 -> (11, 1, "Julian"),
    2 -> (22, 2, "Jark"),
    3 -> (33, 3, "Fabian"))

  // index by (id, name)
  val doubleKeyTable: Map[(Int, String), (Int, Int, String)] = Map(
    (1, "Julian") -> (11, 1, "Julian"),
    (2, "Jark") -> (22, 2, "Jark"),
    (3, "Fabian") -> (33, 3, "Fabian"))
}

object ToCompositeObj extends ScalarFunction {
  def eval(id: Int, name: String, age: Int): CompositeObj = {
    CompositeObj(id, name, age, "0.0")
  }

  def eval(id: Int, name: String, age: Int, point: String): CompositeObj = {
    CompositeObj(id, name, age, point)
  }
}

case class CompositeObj(id: Int, name: String, age: Int, point: String)
