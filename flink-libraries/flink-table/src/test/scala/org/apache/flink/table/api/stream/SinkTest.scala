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

package org.apache.flink.table.api.stream

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.util.RandomUdf
import org.apache.flink.table.runtime.utils.TemporalTableUtils.TestingTemporalTableSource
import org.apache.flink.table.runtime.utils.{TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.{TableFunc0, TableFunc1, TableTestBase}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.util

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class SinkTest(subplanReuseEnabled: Boolean)  extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable3", 'a, 'b, 'c)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, subplanReuseEnabled)
  }

  @Test
  def testSingleSink1(): Unit = {
    util.tableEnv.scan("SmallTable3")
      .groupBy('c)
      .select('c, 'b.count as 'cnt)
      .writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink2(): Unit = {
    val table = util.tableEnv.scan("SmallTable3")
    val table1 = table.where('a <= 10).select('a as 'a1, 'b)
    val table2 = table.where('a >= 0).select('a, 'b, 'c)
    val table3 = table2.where('b >= 5).select('a as 'a2, 'c)
    val table4 = table2.where('b < 5).select('a as 'a3, 'c as 'c1)
    val table5 = table1.join(table3, 'a1 === 'a2).select('a1, 'b, 'c as 'c2)
    val table6 = table4.join(table5, 'a1 === 'a3).select('a1, 'b, 'c1)
    table6.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink3(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, false)
    util.addTable[(Int, Long, String, Double, Boolean)]("SmallTable5", 'a, 'b, 'c, 'd, 'e)
    val t3 = util.tableEnv.scan("SmallTable3")
    val t5 = util.tableEnv.scan("SmallTable5")
    val table1 = t3.where('a <= 10).select('a as 'a1, 'b as 'b1)
    val table2 = table1.join(t5, 'a1 === 'a).select('a, 'b1)
    val table3 = table1.unionAll(table2)
    table3.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink4(): Unit = {
    val table = util.tableEnv.scan("SmallTable3")
    val table1 = table.where('a <= 10).select('a as 'a1, 'b)
    val table2 = table.where('a >= 0).select('a, 'b, 'c)
    val table3 = table2.where('b >= 5).select('a as 'a2, 'c)
    val table4 = table2.where('b < 5).select('a as 'a3, 'c as 'c1)
    val table5 = table1.join(table3, 'a1 === 'a2).select('a1, 'b, 'c as 'c2)
    val table6 = table4.join(table5, 'a1 === 'a3).select('a3, 'b as 'b1, 'c1)
    val table7 = table1.join(table6, 'a1 === 'a3).select('a1, 'b1, 'c1)
    table7.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSinkSplitOnUnion1(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    val scan1 = util.tableEnv.scan("SmallTable3").select('a, 'c)
    val scan2 = util.tableEnv.scan("SmallTable1").select('d, 'f)
    val table = scan1.unionAll(scan2)
    val result = table.select('a.sum as 'total_sum)
    result.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSinkSplitOnUnion2(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    val query = "SELECT a, c FROM SmallTable3  union all SELECT d, f FROM SmallTable1"
    val table = util.tableEnv.sqlQuery(query)
    val result = table.select('a.sum as 'total_sum)
    result.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks(): Unit = {
    val query = "SELECT SUM(a) AS sum_a, c FROM SmallTable3 GROUP BY c"
    val table = util.tableEnv.sqlQuery(query)
    val result1 = table.select('sum_a.sum as 'total_sum)
    val result2 = table.select('sum_a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks2(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, false)
    util.addTable[(Int, Long, String, Double, Boolean)]("SmallTable5", 'a, 'b, 'c, 'd, 'e)
    val t3 = util.tableEnv.scan("SmallTable3")
    val t5 = util.tableEnv.scan("SmallTable5")
    val table1 = t3.where('a <= 10).select('a as 'a1, 'b as 'b1)
    val table2 = table1.join(t5, 'a1 === 'a).select('a, 'b1)
    val table3 = table1.unionAll(table2)
    table3.writeToSink(new CsvTableSink("/tmp/1"))
    table3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks3(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, false)
    util.addTable[(Int, Long, String, Double, Boolean)]("SmallTable5", 'a, 'b, 'c, 'd, 'e)
    val t3 = util.tableEnv.scan("SmallTable3")
    val t5 = util.tableEnv.scan("SmallTable5")
    val table1 = t3.where('a <= 10).select('a as 'a1, 'b as 'b1)
    val table2 = table1.join(t5, 'a1 === 'a).select('a, 'b1)
    val table3 = table1.unionAll(table2)
    table2.writeToSink(new CsvTableSink("/tmp/1"))
    table3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks4(): Unit = {
    val table = util.tableEnv.scan("SmallTable3")
    val table1 = table.where('a <= 10).select('a as 'a1, 'b)
    val table2 = table.where('a >= 0).select('a, 'b, 'c)
    val table3 = table2.where('b >= 5).select('a as 'a2, 'c)
    val table4 = table2.where('b < 5).select('a as 'a3, 'c as 'c1)
    val table5 = table1.join(table3, 'a1 === 'a2).select('a1, 'b, 'c as 'c2)
    val table6 = table4.join(table5, 'a1 === 'a3).select('a1, 'b, 'c1)
    table5.writeToSink(new CsvTableSink("/tmp/1"))
    table6.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinksWithUDTF(): Unit = {
    util.tableEnv.registerFunction("split", new TableFunc1)
    val view1 =
      """
        |SELECT  a, b - MOD(b, 300) AS b, c FROM SmallTable3
        |WHERE b >= UNIX_TIMESTAMP('${startTime}')
      """.stripMargin
    util.tableEnv.registerTable("view1", util.tableEnv.sqlQuery(view1))

    val view2 = "SELECT a, b, c1 AS c FROM view1, LATERAL TABLE(split(c)) AS T(c1) WHERE c <> '' "
    util.tableEnv.registerTable("view2", util.tableEnv.sqlQuery(view2))

    val view3 = "SELECT a, b, COUNT(DISTINCT c) AS total_c FROM view2 GROUP BY a, b"
    util.tableEnv.registerTable("view3", util.tableEnv.sqlQuery(view3))

    val table = util.tableEnv.sqlQuery(
      "SELECT a, total_c FROM view3 UNION ALL SELECT a, 0 AS total_c FROM view1")

    table.filter('a > 50).writeToSink(new CsvTableSink("file1"))
    table.filter('a < 50).writeToSink(new CsvTableSink("file2"))

    util.verifyPlan()
  }

  @Test
  def testMultiSinksWithWindow(): Unit = {
    util.addTable[(Int, Double, Int)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

    val query1 =
      """
        |SELECT
        |    a,
        |    SUM (CAST (c AS DOUBLE)) AS sum_c,
        |    CAST(TUMBLE_END(rowtime, INTERVAL '15' SECOND) as INTEGER) AS `time`,
        |    CAST(TUMBLE_START(rowtime, INTERVAL '15' SECOND) as INTEGER) AS window_start,
        |    CAST(TUMBLE_END (rowtime, INTERVAL '15' SECOND) as INTEGER) AS window_end
        |FROM
        |    MyTable
        |GROUP BY
        |    TUMBLE (rowtime, INTERVAL '15' SECOND), a
      """.stripMargin

    val query2 =
      """
        |SELECT
        |    a,
        |    SUM (CAST (c AS DOUBLE)) AS sum_c,
        |    CAST(TUMBLE_END(rowtime, INTERVAL '15' SECOND) as INTEGER) AS `time`
        |FROM
        |    MyTable
        |GROUP BY
        |    TUMBLE (rowtime, INTERVAL '15' SECOND), a
      """.stripMargin

    util.tableEnv.sqlQuery(query1).writeToSink(new CsvTableSink("file1"))
    util.tableEnv.sqlQuery(query2).writeToSink(new CsvTableSink("file2"))

    util.verifyPlan()
  }

  @Test
  def testMultiSinksWithTemporalTableSource(): Unit = {
    util.addTable[(Long, Double, Long)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.tableEnv.registerTableSource("TemporalSource", new TestingTemporalTableSource)

    val query1 =
      """
        |SELECT
        |    HOP_START(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE),
        |    HOP_END(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE),
        |    name1,
        |    name2,
        |    AVG(b) as avg_b
        |FROM(
        |    SELECT
        |       t2.name as name1, t3.name as name2, t1.b, t1.rowtime
        |    FROM
        |        MyTable t1
        |    INNER join
        |        TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t2
        |        ON t1.a = t2.id
        |    INNER JOIN
        |        TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t3
        |    ON t1.c = t3.id
        |) d
        |    GROUP BY HOP(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE), name1, name2
      """.stripMargin
    util.tableEnv.sqlQuery(query1).writeToSink(new CsvTableSink("file1"))

    val query2 =
      """
        |SELECT
        |    HOP_START(rowtime, INTERVAL '10' SECOND, INTERVAL '3' MINUTE),
        |    HOP_END(rowtime, INTERVAL '10' SECOND, INTERVAL '3' MINUTE),
        |    name1,
        |    AVG(b) as avg_b
        |FROM(
        |    SELECT
        |       t2.name as name1, t1.b, t1.rowtime
        |    FROM
        |        MyTable t1
        |    INNER join
        |        TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t2
        |        ON t1.a = t2.id
        |) d
        |    GROUP BY HOP(rowtime, INTERVAL '10' SECOND, INTERVAL '3' MINUTE), name1
      """.stripMargin
    util.tableEnv.sqlQuery(query2).writeToSink(new CsvTableSink("file2"))

    util.verifyPlan()
  }

  @Test
  def testRetractAndUpsertSinkWithUDTF(): Unit = {
    val func0 = new TableFunc0
    util.addTable[(Int, Long, String)]("MyTable", 'id, 'num, 'text, 'proctime.proctime)
    val table = util.tableEnv.scan("MyTable")
    val t = table.join(func0('text))
      .window(Over orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('num, 'id.count over 'w as 'cnt, 'proctime)

    val retractSink = new TestingRetractTableSink
    t.where('num < 5).select('num, 'cnt).writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    t.where('num >= 5).select('num, 'cnt).writeToSink(upsertSink)

    util.verifyPlan()
  }

  @Test
  def testMultiSinksSplitOnUnion1(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    val scan1 = util.tableEnv.scan("SmallTable3").select('a, 'c)
    val scan2 = util.tableEnv.scan("SmallTable1").select('d, 'f)
    val table = scan1.unionAll(scan2)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinksSplitOnUnion2(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    val query = "SELECT a, c FROM SmallTable3  union all SELECT d, f FROM SmallTable1"
    val table = util.tableEnv.sqlQuery(query)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinksSplitOnUnion3(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("SmallTable2", 'a, 'b, 'c)
    val scan1 = util.tableEnv.scan("SmallTable3").select('a, 'c)
    val scan2 = util.tableEnv.scan("SmallTable1").select('d, 'f)
    val scan3 = util.tableEnv.scan("SmallTable2").select('a, 'c)
    val table = scan1.unionAll(scan2).unionAll(scan3)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    val result3 = scan1.unionAll(scan2).select('a)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    result3.writeToSink(new CsvTableSink("/tmp/3"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinksSplitOnUnion4(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("SmallTable2", 'a, 'b, 'c)
    val query = "SELECT a, c FROM SmallTable3 union all SELECT d, f FROM SmallTable1 "
    val table = util.tableEnv.sqlQuery(query)
    val table2 = table.unionAll(util.tableEnv.sqlQuery("select a, c from SmallTable2"))
    val result1 = table.select('a)
    val result2 = table2.select('a.sum as 'total_sum)
    val result3 = table2.select('a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    result3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinksSplitOnUnion5(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable1", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("SmallTable2", 'a, 'b, 'c)
    val query = "SELECT a, c FROM SmallTable3 union all SELECT d, f FROM SmallTable1 " +
      "union all select a, c from SmallTable2"
    val table = util.tableEnv.sqlQuery(query)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink1SQL(): Unit = {
    util.tableEnv.sqlQuery("SELECT c, count(a) as cnt FROM SmallTable3 GROUP BY c")
      .writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink2SQL(): Unit = {
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b from SmallTable3 where a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("select a, b, c from SmallTable3 where a >= 0")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("select a as a2, c from table2 where b >= 5")
    util.tableEnv.registerTable("table3", table3)
    val table4 = util.tableEnv.sqlQuery("select a as a3, c as c1 from table2 where b < 5")
    util.tableEnv.registerTable("table4", table4)
    val table5 = util.tableEnv.sqlQuery("select a1, b, c as c2 from table1, table3 where a1 = a2")
    util.tableEnv.registerTable("table5", table5)
    val table6 = util.tableEnv.sqlQuery("select a1, b, c1 from table4, table5 where a1 = a3")
    table6.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink3SQL(): Unit = {
    util.addTable[(Int, Long, String, Double, Boolean)]("SmallTable5", 'a, 'b, 'c, 'd, 'e)
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b as b1 from SmallTable3 where a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 from table1, SmallTable5 where a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * from table1 union all select * from table2")
    table3.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSink4SQL(): Unit = {
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b from SmallTable3 where a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("select a, b, c from SmallTable3 where a >= 0")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("select a as a2, c from table2 where b >= 5")
    util.tableEnv.registerTable("table3", table3)
    val table4 = util.tableEnv.sqlQuery("select a as a3, c as c1 from table2 where b < 5")
    util.tableEnv.registerTable("table4", table4)
    val table5 = util.tableEnv.sqlQuery("select a1, b, c as c2 from table1, table3 where a1 = a2")
    util.tableEnv.registerTable("table5", table5)
    val table6 = util.tableEnv.sqlQuery("select a3, b as b1, c1 from table4, table5 where a1 = a3")
    util.tableEnv.registerTable("table6", table6)
    val table7 = util.tableEnv.sqlQuery("select a1, b1, c1 from table1, table6 where a1 = a3")
    table7.writeToSink(new CsvTableSink("/tmp/1"))
    util.verifyPlan()
  }

  @Test
  def testSingleSinkWithTemporalTableSource(): Unit = {
    util.addTable[(Long, Double, Long)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.tableEnv.registerTableSource("TemporalSource", new TestingTemporalTableSource)

    val query =
      """
        |SELECT
        |    HOP_START(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE),
        |    HOP_END(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE),
        |    name1,
        |    name2,
        |    AVG(b) as avg_b
        |FROM(
        |    SELECT
        |       t2.name as name1, t3.name as name2, t1.b, t1.rowtime
        |    FROM
        |        MyTable t1
        |    INNER join
        |        TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t2
        |        ON t1.a = t2.id
        |    INNER JOIN
        |        TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t3
        |    ON t1.c = t3.id
        |) d
        |    GROUP BY HOP(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE), name1, name2
      """.stripMargin
    util.tableEnv.sqlQuery(query).writeToSink(new CsvTableSink("file1"))

    util.verifyPlan()
  }

  @Test
  def testMultiSinks1SQL(): Unit = {
    val table1 = util.tableEnv.sqlQuery("SELECT SUM(a) AS sum_a, c FROM SmallTable3 GROUP BY c")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("select sum(sum_a) as total_sum from table1")
    val table3 = util.tableEnv.sqlQuery("select min(sum_a) as total_min from table1")
    table2.writeToSink(new CsvTableSink("/tmp/1"))
    table3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks2SQL(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, false)
    util.addTable[(Int, Long, String, Double, Boolean)]("SmallTable5", 'a, 'b, 'c, 'd, 'e)
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b as b1 from SmallTable3 where a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 from table1, SmallTable5 where a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * from table1 union all select * from table2")
    table3.writeToSink(new CsvTableSink("/tmp/1"))
    table3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks3SQL(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, false)
    util.addTable[(Int, Long, String, Double, Boolean)]("SmallTable5", 'a, 'b, 'c, 'd, 'e)
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b as b1 from SmallTable3 where a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b1 from table1, SmallTable5 where a = a1")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT * from table1 union all select * from table2")
    table2.writeToSink(new CsvTableSink("/tmp/1"))
    table3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks4SQL(): Unit = {
    val table1 = util.tableEnv.sqlQuery("SELECT a as a1, b from SmallTable3 where a <= 10")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("SELECT a, b, c from SmallTable3 where a >= 0")
    util.tableEnv.registerTable("table2", table2)
    val table3 = util.tableEnv.sqlQuery("SELECT a as a2, c from table2 where b >= 5")
    util.tableEnv.registerTable("table3", table3)
    val table4 = util.tableEnv.sqlQuery("SELECT a as a3, c as c1 from table2 where b < 5")
    util.tableEnv.registerTable("table4", table4)
    val table5 = util.tableEnv.sqlQuery("SELECT a1, b, c as c2 from table1, table3 where a1 = a2")
    util.tableEnv.registerTable("table5", table5)
    val table6 = util.tableEnv.sqlQuery("SELECT a1, b, c1 from table4, table5 where a1 = a3")
    table5.writeToSink(new CsvTableSink("/tmp/1"))
    table6.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

  @Test
  def testMultiSinks5SQL(): Unit = {
    // test with non-deterministic udf
    util.tableEnv.registerFunction("random_udf", RandomUdf)
    val table1 = util.tableEnv.sqlQuery("SELECT random_udf(a) as a, c FROM SmallTable3")
    util.tableEnv.registerTable("table1", table1)
    val table2 = util.tableEnv.sqlQuery("select sum(a) as total_sum from table1")
    val table3 = util.tableEnv.sqlQuery("select min(a) as total_min from table1")
    table2.writeToSink(new CsvTableSink("/tmp/1"))
    table3.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyPlan()
  }

}

object SinkTest {

  @Parameterized.Parameters(name = "subplanReuseEnabled={0}")
  def parameters(): util.Collection[Boolean] = List(false, true)
}
