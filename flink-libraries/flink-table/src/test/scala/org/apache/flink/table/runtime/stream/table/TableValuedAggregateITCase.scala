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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggregate._
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamingWithMiniBatchTestBase, StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class TableValuedAggregateITCase(
    miniBatch: MiniBatchMode,
    stateMode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, stateMode) {
  val data = new mutable.MutableList[(Int, Long, String)]
  data.+=((1, 1L, "Hi"))
  data.+=((2, 2L, "Hello"))
  data.+=((3, 2L, "Hello world"))
  data.+=((4, 3L, "Hello world, how are you?"))
  data.+=((5, 3L, "I am fine."))
  data.+=((6, 3L, "Luke Skywalker"))
  data.+=((7, 4L, "Comment#3"))
  data.+=((8, 4L, "Comment#3"))
  data.+=((9, 4L, "Comment#3"))
  data.+=((10, 4L, "Comment#4"))
  data.+=((11, 5L, "Comment#4"))
  data.+=((12, 5L, "Comment#4"))
  data.+=((13, 5L, "Comment#4"))
  data.+=((14, 5L, "Comment#5"))
  data.+=((15, 5L, "Comment#5"))
  data.+=((16, 6L, "Comment#5"))
  data.+=((17, 6L, "Comment#5"))
  data.+=((18, 6L, "Comment#5"))
  data.+=((19, 6L, "Comment#13"))
  data.+=((20, 6L, "Comment#14"))
  data.+=((21, 6L, "Comment#15"))

  @Test
  def testSimpleUDTVAGGWithoutKey(): Unit = {
    val testTVAGGFun = new SimpleTVAGG

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source.aggApply(testTVAGGFun('a)).as('sum).select('sum)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("1", "231")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)

  }

  @Test
  def testSimpleUDTVAGG(): Unit = {
    val testTVAGGFun = new SimpleTVAGG

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source.groupBy('b).aggApply(testTVAGGFun('a)).select(1983, 'f0)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,111",
      "1983,15",
      "1983,34",
      "1983,5",
      "1983,65")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNonDeterministicSimpleUDTVAGG(): Unit = {
    val testTVAGGFun = new NonDeterministicSimpleTVAGG

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source.groupBy('b).aggApply(testTVAGGFun('a)).select(1983, 'f0)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,1",
      "1983,111",
      "1983,15",
      "1983,34",
      "1983,5",
      "1983,65")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSimpleUDTVAGGWithPojoResult(): Unit = {
    val testTVAGGFun = new SimpleTVAGGWithPojoResult

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b)
      .aggApply(testTVAGGFun('a))
      .select('a, 'f, 'c, 'e, 'b)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,0,hello,false,1",
      "1,0,hello,false,1",
      "1,0,hello,false,2",
      "1,0,hello,false,3",
      "1,0,hello,false,4",
      "1,0,hello,false,5",
      "1,0,hello,false,6",
      "111,0,hello,false,6",
      "15,0,hello,false,3",
      "34,0,hello,false,4",
      "5,0,hello,false,2",
      "65,0,hello,false,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSimpleUDTVAGGWithTupleResult(): Unit = {
    val testTVAGGFun = new SimpleTVAGGWithTupleResult

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).aggApply(testTVAGGFun('a)).select('_1, '_2, '_3, '_4, '_5, '_6)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,2.5,3.5,127,32767,00:00:00",
      "1,2.5,3.5,127,32767,00:00:00",
      "1,2.5,3.5,127,32767,00:00:00",
      "1,2.5,3.5,127,32767,00:00:00",
      "1,2.5,3.5,127,32767,00:00:00",
      "1,2.5,3.5,127,32767,00:00:00",
      "1,2.5,3.5,127,32767,00:00:00",
      "111,2.5,3.5,127,32767,00:00:00",
      "15,2.5,3.5,127,32767,00:00:00",
      "34,2.5,3.5,127,32767,00:00:00",
      "5,2.5,3.5,127,32767,00:00:00",
      "65,2.5,3.5,127,32767,00:00:00")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)

  }

  @Test
  def testSimpleUDTVAGGWithRetract(): Unit = {
    val testTVAGGFun = new SimpleTVAGG

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c))

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1", "1,231")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNonDeterministicSimpleUDTVAGGWithRetract(): Unit = {
    val testTVAGGFun = new NonDeterministicSimpleTVAGG

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c))

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1", "1,231")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGWithRowAccumulator(): Unit = {
    val testTVAGGFun = new TVAGGWithRowAccumulator

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).aggApply(testTVAGGFun('a)).select('f0)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1", "1", "1", "1", "1", "1", "1", "111", "15", "34", "5", "65")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGWithRowAccumulatorWithRetract(): Unit = {
    val testTVAGGFun = new TVAGGWithRowAccumulator

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c))

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1", "1,231")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGWithRow(): Unit = {
    val testTVAGGFun = new TVAGGWithRow

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source.groupBy('b)
      .aggApply(testTVAGGFun('a)).as('g, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,1,first row",
      "1,1,first row",
      "1,1,first row",
      "1,1,first row",
      "1,1,first row",
      "1,1,first row",
      "1,1,second row",
      "111,1,second row",
      "15,1,second row",
      "34,1,second row",
      "5,1,second row",
      "65,1,second row")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGWithRowWithRetract(): Unit = {
    val testTVAGGFun = new TVAGGWithRow

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c))

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,1,first row", "1,231,1,second row")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGDoNothing(): Unit = {
    val testTVAGGFun = new TVAGGDoNothing

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).aggApply(testTVAGGFun('a))
      .select('f0, 1 as 'b)
      .groupBy('b).aggApply(testTVAGGFun('f0)).select('f0)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1",
      "10",
      "11",
      "12",
      "13",
      "14",
      "15",
      "16",
      "17",
      "18",
      "19",
      "2",
      "20",
      "21",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGDoNothingWithRetract(): Unit = {
    val testTVAGGFun = new TVAGGDoNothing

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c)).as('g, 'v).select('v)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1",
      "111",
      "15",
      "34",
      "5",
      "65")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGWithDataView(): Unit = {
    val testTVAGGFun = new TVAGGWithDataView

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c)).as('g, 'v).select('v)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1",
      "111",
      "15",
      "34",
      "5",
      "65")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTVAGGWithDataViewWithRowAccType(): Unit = {
    val testTVAGGFun = new TVAGGWithDataViewWithRowAccType

    val source = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t = source
      .groupBy('b).select('b, 'a.sum as 'c, 1 as 'd)
      .groupBy('d).aggApply(testTVAGGFun('c))

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,1",
      "1,1",
      "1,111",
      "1,111",
      "1,15",
      "1,15",
      "1,34",
      "1,34",
      "1,5",
      "1,5",
      "1,65",
      "1,65")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
