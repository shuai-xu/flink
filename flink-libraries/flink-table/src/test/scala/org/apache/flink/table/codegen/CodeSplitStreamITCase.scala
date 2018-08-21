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

package org.apache.flink.table.codegen

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.expressions.utils.{Func18, RichFunc2}
import org.apache.flink.table.functions.aggregate.CountAggFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, WeightedAvg}
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamTestSink, TestingAppendSink, UserDefinedFunctionTestUtils}
import org.apache.flink.table.util.{PojoTableFunc, TableFunc0}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.collection.mutable

/**
  * copying test cases which can not pass the test during code split development
  * set 'sql.codegen.maxLength' to 1 to verify these test cases
  */
class CodeSplitStreamITCase {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val tableConfig = new TableConfig
  tableConfig.setMaxGeneratedCodeLength(1)
  val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env, tableConfig)

  val data = new mutable.MutableList[(Int, Long, String)]
  data.+=((1, 1L, "Jack#22"))
  data.+=((2, 2L, "John#19"))
  data.+=((3, 2L, "Anna#44"))
  data.+=((4, 3L, "nosharp"))

  val sourceData = List(
    (1L, 1, "Hello"),
    (2L, 2, "Hello"),
    (3L, 3, "Hello"),
    (4L, 4, "Hello"),
    (5L, 5, "Hello"),
    (6L, 6, "Hello"),
    (7L, 7, "Hello World"),
    (8L, 8, "Hello World"),
    (8L, 8, "Hello World"),
    (20L, 20, "Hello World"),
    (20L, 20, null.asInstanceOf[String]))

  @Before
  def clear(): Unit = {

  }

  @Test
  def testCrossJoin(): Unit = {
    val t = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0
    val pojoFunc0 = new PojoTableFunc()

    val result = t
      .join(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .join(pojoFunc0('c))
      .where('age > 20)
      .select('c, 'name, 'age)
      .toAppendStream[Row]

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "Anna#44,Anna,44")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val t = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .leftOuterJoin(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .toAppendStream[Row]

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "nosharp,null,null", "Jack#22,Jack,22",
      "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    val t = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .join(func0('c) as('d, 'e))
      .where(Func18('d, "J"))
      .select('c, 'd, 'e)
      .toAppendStream[Row]

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "John#19,John,19")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUserDefinedFunctionWithParameter(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='ABC#Hello'")
      .select('c)

    val results = result.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFilterSplit(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='ABC#Hello' && a < 100 && b < 1000")
      .select('c)

    val results = result.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFilterSplitWithSecondLayer(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("(RichFunc2(c)='ABC#Hello' || RichFunc2(c)='XYZ#Hello') && (a < 100 || b < 1000)")
      .select('c)

    val results = result.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProcTimeUnBoundedPartitionedRowOver(): Unit = {

    env.setParallelism(1)
    StreamTestSink.clear()

    val stream = env.fromCollection(sourceData)
    val table = stream.toTable(tEnv, 'a, 'b, 'c)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDist = new CountDistinct

    val windowedTable = table//.select('a, 'b, 'c, proctime() as 'proctime)
      .window(
      Over partitionBy 'c orderBy proctime() preceding UNBOUNDED_ROW as 'w)
      .select('c,
        countFun('b) over 'w as 'mycount,
        weightAvgFun('a, 'b) over 'w as 'wAvg,
        countDist('a) over 'w as 'countDist)
      .select('c, 'mycount, 'wAvg, 'countDist)

    val results = windowedTable.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1,7,1", "Hello World,2,7,2", "Hello World,3,7,2", "Hello World,4,13,3",
      "Hello,1,1,1", "Hello,2,1,2", "Hello,3,2,3", "Hello,4,3,4", "Hello,5,3,5", "Hello,6,4,6",
      "null,1,20,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
