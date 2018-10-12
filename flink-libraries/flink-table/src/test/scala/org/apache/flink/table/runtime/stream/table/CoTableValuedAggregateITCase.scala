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

import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggregate.{NonDeterministicInnerJoinFunc, TestInnerJoinFunc, TestInnerJoinWithPojoResultFunc}
import org.apache.flink.table.plan.cost.Func0
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class CoTableValuedAggregateITCase(
    stateMode: StateBackendMode)
  extends StreamingWithStateTestBase(stateMode) {

  val left = new mutable.MutableList[(Int, String)]
  left.+=((1, "one_l"))
  left.+=((2, "two_l1"))
  left.+=((2, "two_l2"))

  val right = new mutable.MutableList[(Int, String)]
  right.+=((1, "one_r"))
  right.+=((2, "two_r1"))
  right.+=((2, "two_r2"))
  right.+=((3, "three_r"))
  right.+=((3, "three_r"))
  right.+=((3, "three_r"))

  @Test
  def testInnerJoinUsingCoTableValuedFunction(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc

    val func = new Func0
    val sourceL = env.fromCollection(left).toTable(tEnv, 'l1, 'l2)
    val sourceR = env.fromCollection(right).toTable(tEnv, 'r1, 'r2)
    val t = sourceL.connect(sourceR,'l1 === func('r1))
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .select('l1, 'f0, 'f1, 'f2, 'f3, 'f4, 'f5)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,one_l,1,one_r,2,2",
      "2,2,two_l1,2,two_r1,3,4",
      "2,2,two_l1,2,two_r2,3,4",
      "2,2,two_l2,2,two_r1,3,4",
      "2,2,two_l2,2,two_r2,3,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinUsingNonDeterministicCoTableValuedFunction(): Unit = {
    val coTVAGGFunc = new NonDeterministicInnerJoinFunc

    val func = new Func0
    val sourceL = env.fromCollection(left).toTable(tEnv, 'l1, 'l2)
    val sourceR = env.fromCollection(right).toTable(tEnv, 'r1, 'r2)
    val t = sourceL.connect(sourceR,'l1 + 2 === func('r1) + 2)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .select('_lg0, 'f0, 'f1, 'f3, 'f4, 'f5)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "3,1,one_l,one_r,2,2",
      "4,2,two_l1,two_r1,3,4",
      "4,2,two_l1,two_r2,3,4",
      "4,2,two_l2,two_r1,3,4",
      "4,2,two_l2,two_r2,3,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testWithPojoResult(): Unit = {
    val coTVAGGFunc = new TestInnerJoinWithPojoResultFunc

    val func = new Func0
    val sourceL = env.fromCollection(left).toTable(tEnv, 'l1, 'l2)
    val sourceR = env.fromCollection(right).toTable(tEnv, 'r1, 'r2)
    val t = sourceL.connect(sourceR,'l1 + 2 === func('r1) + 2)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .as('k, 'a, 'b, 'c, 'd, 'e)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "3,1,one_l,1,one_r,2,2",
      "4,2,two_l1,2,two_r1,3,4",
      "4,2,two_l1,2,two_r2,3,4",
      "4,2,two_l2,2,two_r1,3,4",
      "4,2,two_l2,2,two_r2,3,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testReceiveRetract(): Unit = {
    val coTVAGGFunc = new TestInnerJoinWithPojoResultFunc

    val func = new Func0
    val last = new StringLast
    val sourceL = env.fromCollection(left).toTable(tEnv, 'l1, 'l2)
      .groupBy('l1)
      .select('l1, last('l2) as 'l2)
    val sourceR = env.fromCollection(right).toTable(tEnv, 'r1, 'r2)
      .groupBy('r1)
      .select('r1, last('r2) as 'r2)
    val t = sourceL.connect(sourceR, 'l1 + 2 === func('r1) + 2)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .as('k, 'a, 'b, 'c, 'd, 'e)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "3,1,one_l,1,one_r,2,2",
      "4,2,two_l2,2,two_r2,3,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNonKeyCoTableValuedFunction(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc

    val left = new mutable.MutableList[(Int, String)]
    left.+=((1, "one_l"))
    left.+=((2, "two_l1"))
    val right = new mutable.MutableList[(Int, String)]
    right.+=((2, "two_r2"))
    right.+=((3, "three_r"))

    val sourceL = env.fromCollection(left).toTable(tEnv, 'l1, 'l2)
    val sourceR = env.fromCollection(right).toTable(tEnv, 'r1, 'r2)
    val t = sourceL.connect(sourceR)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .select('f0, 'f1, 'f2, 'f3, 'f4, 'f5)

    val sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,one_l,2,two_r2,3,4",
      "1,one_l,3,three_r,4,4",
      "2,two_l1,2,two_r2,3,4",
      "2,two_l1,3,three_r,4,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}

class UDLastAccumulator[T] extends JTuple1[T]

abstract class UDLast[T] extends AggregateFunction[T, UDLastAccumulator[T]] {

  override def createAccumulator(): UDLastAccumulator[T] = {
    val acc = new UDLastAccumulator[T]
    acc.f0 = getInitValue
    acc
  }

  def accumulate(acc: UDLastAccumulator[T], value: Any): Unit = {
    if (null != value) {
      acc.f0 = value.asInstanceOf[T]
    }
  }

  override def getValue(acc: UDLastAccumulator[T]): T = {
    acc.f0
  }

  def resetAccumulator(acc: UDLastAccumulator[T]): Unit = {
    acc.f0 = getInitValue
  }

  def getInitValue: T = {
    null.asInstanceOf[T]
  }

  def getValueTypeInfo: DataType

  override def getResultType(): DataType = getValueTypeInfo

  override def getAccumulatorType(): DataType = {
    DataTypes.createTupleType(classOf[UDLastAccumulator[T]], getValueTypeInfo)
  }
}

class StringLast extends UDLast[String] {
  override def getValueTypeInfo = DataTypes.STRING
}

