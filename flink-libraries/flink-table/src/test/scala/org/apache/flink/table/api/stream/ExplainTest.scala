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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableConfigOptions}
import org.apache.flink.table.runtime.utils.{StreamTestData, TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.{StreamTableTestUtil, TableFunc1, TableTestBase}

import org.junit._

class ExplainTest extends TableTestBase {

  private var util: StreamTableTestUtil = _

  @Before
  def setup(): Unit = {
    util = streamTestUtil()
  }

  @Test
  def testFilter(): Unit = {
    val table = util.addTable[(Int, String)]('a, 'b).filter("a % 2 = 0")
    util.verifyExplain(table)
  }

  @Test
  def testUnion(): Unit = {
    val table1 = util.addTable[(Int, String)]('count, 'word)
    val table2 = util.addTable[(Int, String)]('count, 'word)
    val table = table1.unionAll(table2)
    util.verifyExplain(table)
  }

  @Test
  def testUpsertSink0(): Unit = {
    val t = StreamTestData.get3TupleDataStream(util.env)
      .toTable(util.tableEnv, 'id, 'num, 'text)

    t.groupBy('num)
      .select('num, 'id.count as 'cnt)
      .writeToSink(new TestingUpsertTableSink(Array(0)))

    util.verifyExplain()
  }

  @Test
  def testUpsertSink1(): Unit = {
    val ds = StreamTestData.get3TupleDataStream(util.env)
    util.tableEnv.registerDataStream("T", ds, 'id, 'num, 'text)
    util.tableEnv.sqlQuery("SELECT num, count(id) as cnt FROM T GROUP BY num")
      .writeToSink(new TestingUpsertTableSink(Array(0)))

    util.verifyExplain()
  }

  @Test
  def testUpsertSink2(): Unit = {
    val table = StreamTestData.get3TupleDataStream(util.env)
      .toTable(util.tableEnv, 'id, 'num, 'text)

    val table1 = table.where('id <= 10).select('id as 'id1, 'num)
    val table2 = table.where('id >= 0).select('id, 'num, 'text)
    val table3 = table2.where('num >= 5).select('id as 'id2, 'text)
    val table4 = table2.where('num < 5).select('id as 'id3, 'text as 'text1)
    val table5 = table1.join(table3, 'id1 === 'id2)
      .select('id1, 'num, 'text as 'text2)
    val table6 = table4.join(table5, 'id1 === 'id3).select('id1, 'num, 'text1)

    table6.writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testRetractAndUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(util.env)
      .toTable(util.tableEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    t.where('num < 4).select('num, 'cnt)
      .writeToSink(new TestingRetractTableSink)
    t.where('num >= 4 && 'num < 6).select('num, 'cnt)
      .writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testRetractAndUpsertSinkForSQL(): Unit = {
    val ds = StreamTestData.get3TupleDataStream(util.env)
    util.tableEnv.registerDataStream("T", ds, 'id, 'num, 'text)
    val t1 = util.tableEnv.sqlQuery("SELECT num, count(id) as cnt FROM T GROUP BY num")
    util.tableEnv.registerTable("T1", t1)

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T1 WHERE num < 4")
      .writeToSink(new TestingRetractTableSink)

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T1 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testUpdateAsRetractConsumedAtSinkBlock(): Unit = {
    util.tableEnv.registerTable("MyTable",
      StreamTestData.get3TupleDataStream(util.env).toTable(util.tableEnv, 'a, 'b, 'c))

    val t = util.tableEnv.sqlQuery("SELECT a, b, c FROM MyTable")
    util.tableEnv.registerTable("T", t)
    val retractSink = new TestingRetractTableSink
    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM T)
         |WHERE rank_num <= 10
      """.stripMargin
    util.tableEnv.sqlQuery(sql).writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    util.tableEnv.sqlQuery("SELECT a, b FROM T WHERE a < 6").writeToSink(upsertSink)

    util.verifyExplain()
  }

  @Test
  def testUpdateAsRetractConsumedAtSourceBlock(): Unit = {
    util.tableEnv.registerTable("MyTable",
      StreamTestData.get3TupleDataStream(util.env).toTable(util.tableEnv, 'a, 'b, 'c))

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY c DESC) as rank_num
         |  FROM MyTable)
         |WHERE rank_num <= 10
      """.stripMargin

    val t = util.tableEnv.sqlQuery(sql)
    util.tableEnv.registerTable("T", t)
    val retractSink = new TestingRetractTableSink
    util.tableEnv.sqlQuery("SELECT a FROM T where a > 6").writeToSink(retractSink)
    val upsertSink = new TestingUpsertTableSink(Array())
    util.tableEnv.sqlQuery("SELECT a, b FROM T WHERE a < 6").writeToSink(upsertSink)

    util.verifyExplain()
  }

  @Test
  def testUpsertAndUpsertSink(): Unit = {
    val t = StreamTestData.get3TupleDataStream(util.env)
      .toTable(util.tableEnv, 'id, 'num, 'text)
      .groupBy('num)
      .select('num, 'id.count as 'cnt)

    t.where('num < 4).groupBy('cnt).select('cnt, 'num.count as 'frequency)
      .writeToSink(new TestingUpsertTableSink(Array(0)))
    t.where('num >= 4 && 'num < 6).select('num, 'cnt)
      .writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testUpsertAndUpsertSinkForSQL(): Unit = {
    val ds = StreamTestData.get3TupleDataStream(util.env)
    util.tableEnv.registerDataStream("T", ds, 'id, 'num, 'text)
    val t1 = util.tableEnv.sqlQuery("SELECT num, count(id) as cnt FROM T GROUP BY num")
    util.tableEnv.registerTable("T1", t1)

    util.tableEnv.sqlQuery(
      """
        |SELECT cnt, count(num) frequency
        |FROM (
        |  SELECT * FROM T1 WHERE num < 4
        |)
        |GROUP BY cnt
      """.stripMargin)
      .writeToSink(new TestingUpsertTableSink(Array(0)))

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T1 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testMultiLevelViewForSQL(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    val ds = StreamTestData.get3TupleDataStream(util.env)
    util.tableEnv.registerDataStream("T", ds, 'id, 'num, 'text)

    val t1 = util.tableEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%hello%'")
    util.tableEnv.registerTable("T1", t1)
    t1.writeToSink(new TestingAppendTableSink)
    val t2 = util.tableEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%world%'")
    util.tableEnv.registerTable("T2", t2)

    val t3 = util.tableEnv.sqlQuery(
      """
        |SELECT num, count(id) as cnt
        |FROM
        |(
        | (SELECT * FROM T1)
        | UNION ALL
        | (SELECT * FROM T2)
        |)
        |GROUP BY num
      """.stripMargin)
    util.tableEnv.registerTable("T3", t3)

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T3 WHERE num < 4")
      .writeToSink(new TestingRetractTableSink)

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T3 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testSharedUnionNode(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)
    val ds = StreamTestData.get3TupleDataStream(util.env)
    util.tableEnv.registerDataStream("T", ds, 'id, 'num, 'text)

    val t1 = util.tableEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%hello%'")
    util.tableEnv.registerTable("T1", t1)
    t1.writeToSink(new TestingAppendTableSink)
    val t2 = util.tableEnv.sqlQuery("SELECT id, num, text FROM T WHERE text LIKE '%world%'")
    util.tableEnv.registerTable("T2", t2)

    val t3 = util.tableEnv.sqlQuery(
      """
        |SELECT * FROM T1
        |UNION ALL
        |SELECT * FROM T2
      """.stripMargin)
    util.tableEnv.registerTable("T3", t3)
    util.tableEnv.sqlQuery("SELECT * FROM T3 WHERE num >= 5")
      .writeToSink(new TestingRetractTableSink)

    val t4 = util.tableEnv.sqlQuery(
      """
        |SELECT num, count(id) as cnt
        |FROM T3
        |GROUP BY num
      """.stripMargin)
    util.tableEnv.registerTable("T4", t4)

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T4 WHERE num < 4")
      .writeToSink(new TestingRetractTableSink)

    util.tableEnv.sqlQuery("SELECT num, cnt FROM T4 WHERE num >=4 AND num < 6")
      .writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testUdtf(): Unit = {
    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)
    util.tableEnv.registerDataStream("t3",
      StreamTestData.get5TupleDataStream(util.env), 'i, 'j, 'k, 'l, 'm)
    util.tableEnv.registerFunction("split", new TableFunc1)

    val t1 = util.tableEnv.scan("t1")
    val t2 = util.tableEnv.scan("t2")
    val t3 = util.tableEnv.scan("t3")
    val result = t1.join(t2).where("b === e").join(t3).where("a === i")
      .join(new Table(util.tableEnv, "split(f) AS f1"))
    result.writeToSink(new CsvTableSink("file"))

    util.verifyExplain()
  }

  @Test
  def testMultiSinksSplitOnUnion1(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)

    val t1 = util.tableEnv.scan("t1").select('a, 'c)
    val t2 = util.tableEnv.scan("t2").select('d, 'f)
    val table = t1.unionAll(t2)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)

    util.verifyExplain()
  }

  @Test
  def testMultiSinksSplitOnUnion2(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)

    val query = "SELECT a, c FROM t1 union all SELECT d, f FROM t2"
    val table = util.tableEnv.sqlQuery(query)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)

    util.verifyExplain()
  }

  @Test
  def testMultiSinksSplitOnUnion3(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)
    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)
    util.tableEnv.registerDataStream("t3",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)

    val t1 = util.tableEnv.scan("t1")
    val t2 = util.tableEnv.scan("t2")
    val t3 = util.tableEnv.scan("t3")
    val table = t1.unionAll(t2).unionAll(t3)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    val result3 = t1.unionAll(t2).select('a)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)
    result3.writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testMultiSinksSplitOnUnion4(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)
    util.tableEnv.registerDataStream("t3",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)

    val query = "SELECT a, c FROM t1 union all SELECT d, f FROM t2 "
    val table = util.tableEnv.sqlQuery(query)
    val table2 = table.unionAll(util.tableEnv.sqlQuery("select a, c from t3"))
    val result1 = table.select('a)
    val result2 = table2.select('a.sum as 'total_sum)
    val result3 = table2.select('a.min as 'total_min)
    result1.writeToSink(new TestingAppendTableSink)
    result2.writeToSink(new TestingRetractTableSink)
    result3.writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }

  @Test
  def testMultiSinksSplitOnUnion5(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)
    util.tableEnv.registerDataStream("t3",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)

    val query = "SELECT a, c FROM t1 union all SELECT d, f FROM t2 " +
      "union all select a, c from t3"
    val table = util.tableEnv.sqlQuery(query)
    val result1 = table.select('a.sum as 'total_sum)
    val result2 = table.select('a.min as 'total_min)
    result1.writeToSink(new TestingUpsertTableSink(Array()))
    result2.writeToSink(new TestingRetractTableSink)

    util.verifyExplain()
  }

  @Test
  def testSingleSinkSplitOnUnion1(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)

    val scan1 = util.tableEnv.scan("t1").select('a, 'c)
    val scan2 = util.tableEnv.scan("t2").select('d, 'f)
    val table = scan1.unionAll(scan2)
    val result = table.select('a.sum as 'total_sum)
    result.writeToSink(new TestingRetractTableSink)

    util.verifyExplain()
  }

  @Test
  def testSingleSinkSplitOnUnion2(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED, true)

    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)
    util.tableEnv.registerDataStream("t2",
      StreamTestData.getSmall3TupleDataStream(util.env), 'd, 'e, 'f)
    val query = "SELECT a, c FROM t1  union all SELECT d, f FROM t2"
    val table = util.tableEnv.sqlQuery(query)
    val result = table.select('a.sum as 'total_sum)
    result.writeToSink(new TestingRetractTableSink)

    util.verifyExplain()
  }

  @Test
  def testUnionAggWithDifferentGroupings(): Unit = {
    util.tableEnv.registerDataStream("t1",
      StreamTestData.get3TupleDataStream(util.env), 'a, 'b, 'c)

    val query = "SELECT a, b, c FROM t1"
    val table = util.tableEnv.sqlQuery(query)
    val result1 = table.groupBy('a, 'b, 'c).select('a, 'b, 'c, 'a.sum as 'a_sum)
    val result2 = table.groupBy('b, 'c).select(1 as 'a, 'b, 'c, 'a.sum as 'a_sum)
    val result3 = result1.unionAll(result2)
    result3.writeToSink(new TestingUpsertTableSink(Array()))

    util.verifyExplain()
  }
}
