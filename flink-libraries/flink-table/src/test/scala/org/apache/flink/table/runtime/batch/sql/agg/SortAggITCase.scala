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

package org.apache.flink.table.runtime.batch.sql.agg

import java.lang.{Iterable => JIterable}
import java.util

import org.apache.flink.api.java.typeutils.{RowTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.batch.sql.MyPojo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.functions.aggregate.{CountAccumulator, CountAggFunction, IntSumAggFunction}
import org.apache.flink.table.runtime.batch.sql.QueryTest.row
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.types.Row
import org.junit.Test

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.Seq

/**
  * AggregateITCase using SortAgg Operator.
  */
class SortAggITCase
    extends AggregateITCaseBase("SortAggregate") {
  override def prepareAggOp(): Unit = {
    tEnv.getConfig.getParameters.setString(
      TableConfig.SQL_PHYSICAL_OPERATORS_DISABLED, "HashAgg")

    registerFunction("countFun", new CountAggFunction())
    registerFunction("intSumFun", new IntSumAggFunction())
    registerFunction("weightedAvg", new WeightedAvgWithMergeAndReset())

    registerFunction("myPrimitiveArrayUdaf", new MyPrimitiveArrayUdaf())
    registerFunction("myObjectArrayUdaf", new MyObjectArrayUdaf())
    registerFunction("myNestedLongArrayUdaf", new MyNestedLongArrayUdaf())
    registerFunction("myNestedStringArrayUdaf", new MyNestedStringArrayUdaf())

    registerFunction("myPrimitiveMapUdaf", new MyPrimitiveMapUdaf())
    registerFunction("myObjectMapUdaf", new MyObjectMapUdaf())
    registerFunction("myNestedMapUdaf", new MyNestedMapUdf())
  }

  @Test
  def testMultiSetAggBufferGroupBy(): Unit = {
    checkResult(
      "SELECT collect(b) FROM Table3",
      Seq(
        row(collection.immutable.SortedMap(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6).asJava)
      )
    )
  }

  @Test
  def testUDAGGWithoutGroupby(): Unit = {
    checkResult(
      "SELECT countFun(c) FROM Table3",
      Seq(
        row(21)
      )
    )
  }

  @Test
  def testUDAGGWithGroupby(): Unit = {
    checkResult(
      "SELECT countFun(a), count(a), b FROM Table3 GROUP BY b",
      Seq(
        row(1, 1, 1),
        row(2, 2, 2),
        row(3, 3, 3),
        row(4, 4, 4),
        row(5, 5, 5),
        row(6, 6, 6)
      )
    )
  }

  @Test
  def testUDAGGNullGroupKeyAggregation(): Unit = {
    checkResult("SELECT intSumFun(d), d, count(d) FROM NullTable5 GROUP BY d",
      Seq(
        row(1, 1, 1),
        row(25, 5, 5),
        row(null, null, 0),
        row(16, 4, 4),
        row(4, 2, 2),
        row(9, 3, 3)
      )
    )
  }

  @Test
  def testComplexUDAGGWithGroupBy(): Unit = {
    checkResult(
      "SELECT b, weightedAvg(b, a) FROM Table3 GROUP BY b",
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4),
        row(5, 5),
        row(6, 6)
      )
    )
  }

  // NOTE: Spark has agg functions collect_list(), collect_set().
  //       instead, we'll test concat_agg() here
  @Test
  def testConcatAgg(): Unit = {
    checkResult(
      "SELECT concat_agg('-', c), concat_agg(c) FROM SmallTable3",
      Seq(
        row("Hi-Hello-Hello world", "Hi\nHello\nHello world")
      )
    )

    // EmptyTable5
    checkResult(
      "SELECT concat_agg('-', g), concat_agg(g) FROM EmptyTable5",
      Seq(
        row(null, null)
      )
    )

    checkResult(
      "SELECT concat_agg('-', c), concat_agg(c) FROM AllNullTable3",
      Seq(
        row(null, null)
      )
    )
  }

  @Test
  def testPojoField(): Unit = {
    val data = Seq(
      row(1, new MyPojo(5, 105)),
      row(1, new MyPojo(6, 11)),
      row(1, new MyPojo(7, 12)))
    tEnv.registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(Types.INT, TypeExtractor.createTypeInfo(classOf[MyPojo])),
      'a, 'b)

    tEnv.registerFunction("pojoFunc", new MyPojoAggFunction)
    check(
      "SELECT pojoFunc(b) FROM MyTable group by a",
      (result: Seq[Row]) => {
        val baseRow = result.head.getField(0).asInstanceOf[BaseRow]
        if (baseRow.getInt(0) == 128 && baseRow.getInt(1) == 128) {
          None
        } else {
          Some("Fail: " + result)
        }
      })
  }

  @Test
  def testVarArgs(): Unit = {
    val data = Seq(
      row(1, 1L, "5", "3"),
      row(1, 22L, "15", "13"),
      row(3, 33L, "25", "23"))
    tEnv.registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.STRING, Types.STRING),
      'id, 's, 's1, 's2)
    val func = new VarArgsAggFunction
    tEnv.registerFunction("func", func)

    // no group
    checkResult(
      "SELECT func(s, s1, s2) FROM MyTable",
      Seq(row(140)))

    // with group
    checkResult(
      "SELECT id, func(s, s1, s2) FROM MyTable group by id",
      Seq(
        row(1, 59),
        row(3, 81)))
  }

  @Test
  def testMaxString(): Unit = {
    checkResult(
      "SELECT max(c) FROM Table3 GROUP BY b",
      Seq(
        row("Comment#15"),
        row("Comment#4"),
        row("Comment#9"),
        row("Hello world"),
        row("Hi"),
        row("Luke Skywalker")
      )
    )

    checkResult(
      "SELECT max(c) FROM Table3",
      Seq(
        row("Luke Skywalker")
      )
    )
  }

  @Test
  def testMaxStringAllNull(): Unit = {
    checkResult(
      "SELECT max(c) FROM AllNullTable3 GROUP BY b",
      Seq(
        row(null)
      )
    )

    checkResult(
      "SELECT max(c) FROM AllNullTable3",
      Seq(
        row(null)
      )
    )
  }

  @Test
  def testFirstValueOnString(): Unit = {
    checkResult(
      "SELECT first_value(c) over () FROM Table3",
      Seq(
        row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"),
        row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi"),
        row("Hi"), row("Hi"), row("Hi"), row("Hi"), row("Hi")
      )
    )
  }

  @Test
  def testArrayUdaf(): Unit = {
    checkResult(
      "SELECT myPrimitiveArrayUdaf(a, b) FROM Table3",
      Seq(row(Array(231, 91)))
    )
    checkResult(
      "SELECT myObjectArrayUdaf(c) FROM Table3",
      Seq(row(Array("HHHHILCCCCCCCCCCCCCCC", "iod?.r123456789012345")))
    )
    checkResult(
      "SELECT myNestedLongArrayUdaf(a, b)[2] FROM Table3",
      Seq(row(Array(91, 231)))
    )
    checkResult(
      "SELECT myNestedStringArrayUdaf(c)[2] FROM Table3",
      Seq(row(Array("iod?.r123456789012345", "HHHHILCCCCCCCCCCCCCCC")))
    )
  }

  @Test
  def testMapUdaf(): Unit = {
    checkResult(
      "SELECT myPrimitiveMapUdaf(a, b)[3] FROM Table3",
      Seq(row(15))
    )
    checkResult(
      "SELECT myPrimitiveMapUdaf(a, b)[6] FROM Table3",
      Seq(row(111))
    )
    checkResult(
      "SELECT myObjectMapUdaf(a, c)['Co'] FROM Table3",
      Seq(row(210))
    )
    checkResult(
      "SELECT myObjectMapUdaf(a, c)['He'] FROM Table3",
      Seq(row(9))
    )
    checkResult(
      "SELECT myNestedMapUdaf(a, b, c)[6]['Co'] FROM Table3",
      Seq(row(111))
    )
    checkResult(
      "SELECT myNestedMapUdaf(a, b, c)[3]['He'] FROM Table3",
      Seq(row(4))
    )
    checkResult(
      "SELECT myNestedMapUdaf(a, b, c)[3]['Co'] FROM Table3",
      Seq(row("null"))
    )
  }
}

class MyPojoAggFunction extends AggregateFunction[MyPojo, CountAccumulator] {

  def accumulate(acc: CountAccumulator, value: MyPojo): Unit = {
    if (value != null) {
      acc.f0 += value.f2
    }
  }

  def retract(acc: CountAccumulator, value: MyPojo): Unit = {
    if (value != null) {
      acc.f0 -= value.f2
    }
  }

  override def getValue(acc: CountAccumulator): MyPojo = {
    new MyPojo(acc.f0.asInstanceOf[Int], acc.f0.asInstanceOf[Int])
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  def resetAccumulator(acc: CountAccumulator): Unit = {
    acc.f0 = 0L
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(classOf[CountAccumulator], DataTypes.LONG)
  }

  private def pojoType = DataTypes.pojoBuilder(classOf[MyPojo])
      .field("f1", DataTypes.INT)
      .field("f2", DataTypes.INT)
      .build()

  override def getResultType: DataType = pojoType
}

class VarArgsAggFunction extends AggregateFunction[Long, CountAccumulator] {

  @varargs
  def accumulate(acc: CountAccumulator, value: Long, args: String*): Unit = {
    acc.f0 += value
    args.foreach(s => acc.f0 += s.toLong)
  }

  @varargs
  def retract(acc: CountAccumulator, value: Long, args: String*): Unit = {
    acc.f0 -= value
    args.foreach(s => acc.f0 -= s.toLong)
  }

  override def getValue(acc: CountAccumulator): Long = {
    acc.f0
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  def resetAccumulator(acc: CountAccumulator): Unit = {
    acc.f0 = 0L
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(classOf[CountAccumulator], DataTypes.LONG)
  }
}

class MyPrimitiveArrayUdaf extends AggregateFunction[Array[Long], Array[Long]] {

  override def createAccumulator(): Array[Long] = new Array[Long](2)

  override def getValue(accumulator: Array[Long]): Array[Long] = accumulator

  def accumulate(accumulator: Array[Long], a: Int, b: Long): Unit = {
    accumulator(0) += a
    accumulator(1) += b
  }

  override def getAccumulatorType: DataType = DataTypes.createPrimitiveArrayType(DataTypes.LONG)

  override def getResultType: DataType = DataTypes.createPrimitiveArrayType(DataTypes.LONG)
}

class MyObjectArrayUdaf extends AggregateFunction[Array[String], Array[String]] {

  override def createAccumulator(): Array[String] = Array("", "")

  override def getValue(accumulator: Array[String]): Array[String] = accumulator

  def accumulate(accumulator: Array[String], c: String): Unit = {
    accumulator(0) = accumulator(0) + c.charAt(0)
    accumulator(1) = accumulator(1) + c.charAt(c.length - 1)
  }

  override def getAccumulatorType: DataType = DataTypes.createArrayType(DataTypes.STRING)

  override def getResultType: DataType = DataTypes.createArrayType(DataTypes.STRING)
}

class MyNestedLongArrayUdaf extends AggregateFunction[Array[Array[Long]], Array[Array[Long]]] {

  override def createAccumulator(): Array[Array[Long]] = Array(Array(0, 0), Array(0, 0))

  override def getValue(accumulator: Array[Array[Long]]): Array[Array[Long]] = accumulator

  def accumulate(accumulator: Array[Array[Long]], a: Int, b: Long): Unit = {
    accumulator(0)(0) += a
    accumulator(0)(1) += b
    accumulator(1)(0) += b
    accumulator(1)(1) += a
  }

  override def getAccumulatorType: DataType =
    DataTypes.createArrayType(DataTypes.createPrimitiveArrayType(DataTypes.LONG))

  override def getResultType: DataType =
    DataTypes.createArrayType(DataTypes.createPrimitiveArrayType(DataTypes.LONG))
}

class MyNestedStringArrayUdaf extends AggregateFunction[
    Array[Array[String]], Array[Array[String]]] {

  override def createAccumulator(): Array[Array[String]] = Array(Array("", ""), Array("", ""))

  override def getValue(accumulator: Array[Array[String]]): Array[Array[String]] = accumulator

  def accumulate(accumulator: Array[Array[String]], c: String): Unit = {
    accumulator(0)(0) = accumulator(0)(0) + c.charAt(0)
    accumulator(0)(1) = accumulator(0)(1) + c.charAt(c.length - 1)
    accumulator(1)(0) = accumulator(1)(0) + c.charAt(c.length - 1)
    accumulator(1)(1) = accumulator(1)(1) + c.charAt(0)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.STRING))

  override def getResultType: DataType =
    DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.STRING))
}

class MyPrimitiveMapUdaf extends AggregateFunction[
    java.util.Map[Long, Int], java.util.Map[Long, Int]] {

  override def createAccumulator(): util.Map[Long, Int] = new util.HashMap[Long, Int]()

  override def getValue(accumulator: util.Map[Long, Int]): util.Map[Long, Int] =
    accumulator

  def accumulate(accumulator: util.Map[Long, Int], a: Int, b: Long): Unit = {
    accumulator.putIfAbsent(b, 0)
    accumulator.put(b, accumulator.get(b) + a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(DataTypes.LONG, DataTypes.INT)

  override def getResultType: DataType =
    DataTypes.createMapType(DataTypes.LONG, DataTypes.INT)
}

class MyObjectMapUdaf extends AggregateFunction[
    java.util.Map[String, Int], java.util.Map[String, Int]] {

  override def createAccumulator(): util.Map[String, Int] = new util.HashMap[String, Int]()

  override def getValue(accumulator: util.Map[String, Int]): util.Map[String, Int] =
    accumulator

  def accumulate(accumulator: util.Map[String, Int], a: Int, c: String): Unit = {
    val key = c.substring(0, 2)
    accumulator.putIfAbsent(key, 0)
    accumulator.put(key, accumulator.get(key) + a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(DataTypes.STRING, DataTypes.INT)

  override def getResultType: DataType =
    DataTypes.createMapType(DataTypes.STRING, DataTypes.INT)
}

class MyNestedMapUdf extends AggregateFunction[
    java.util.Map[Long, java.util.Map[String, Int]],
    java.util.Map[Long, java.util.Map[String, Int]]] {

  override def createAccumulator(): java.util.Map[Long, java.util.Map[String, Int]] =
    new java.util.HashMap[Long, java.util.Map[String, Int]]()

  override def getValue(accumulator: java.util.Map[Long, java.util.Map[String, Int]])
      : java.util.Map[Long, java.util.Map[String, Int]] =
    accumulator

  def accumulate(
      accumulator: java.util.Map[Long, java.util.Map[String, Int]],
      a: Int, b: Long, c: String): Unit = {
    val key = c.substring(0, 2)
    accumulator.putIfAbsent(b, new java.util.HashMap[String, Int]())
    accumulator.get(b).putIfAbsent(key, 0)
    accumulator.get(b).put(key, accumulator.get(b).get(key) + a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(
      DataTypes.LONG,
      DataTypes.createMapType(DataTypes.STRING, DataTypes.INT))

  override def getResultType: DataType =
    DataTypes.createMapType(
      DataTypes.LONG,
      DataTypes.createMapType(DataTypes.STRING, DataTypes.INT))
}
