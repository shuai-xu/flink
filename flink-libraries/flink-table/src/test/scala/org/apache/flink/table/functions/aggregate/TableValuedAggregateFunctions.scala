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
package org.apache.flink.table.functions.aggregate

import java.lang.Iterable
import java.sql.Time
import java.util
import java.util.Iterator
import java.util.function.Consumer

import org.apache.flink.table.api.dataview.{ListView, MapView}
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.functions.TableValuedAggregateFunction
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, GenericType}
import org.apache.flink.table.typeutils.{ListViewTypeInfo, MapViewTypeInfo}
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

class LongWrapper {
  var l: Long = 0
  val t: Time = new Time(1)
}


class SimpleTVAGG extends TableValuedAggregateFunction[Long, LongWrapper] {
  override def createAccumulator(): LongWrapper = {
    new LongWrapper
  }

  def accumulate(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l += input
  }

  def retract(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l -= input
  }

  override def emitValue(accumulator: LongWrapper, out: Collector[Long]): Unit = {
    out.collect(accumulator.t.getTime)
    out.collect(accumulator.l)
  }
}

class NonDeterministicSimpleTVAGG extends TableValuedAggregateFunction[Long, LongWrapper] {
  override def createAccumulator(): LongWrapper = {
    new LongWrapper
  }

  /**
    * @return true if a call to this function is guaranteed to always return
    *         the same result given the same parameters; true is assumed by default
    *         if user's function is not pure functional, like random(), date(), now()...
    *         isDeterministic must return false
    */
  override def isDeterministic: Boolean = false

  def accumulate(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l += input
  }

  def retract(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l -= input
  }

  override def emitValue(accumulator: LongWrapper, out: Collector[Long]): Unit = {
    out.collect(accumulator.t.getTime)
    out.collect(accumulator.l)
  }
}

class ResultWrapper {
  var a: Long = 0
  var f: Int = 0
  var c: String = "hello"
  var e: Boolean = false
}

class SimpleTVAGGWithPojoResult extends TableValuedAggregateFunction[ResultWrapper, LongWrapper] {
  override def createAccumulator(): LongWrapper = {
    new LongWrapper
  }

  def accumulate(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l += input
  }

  def retract(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l -= input
  }

  override def emitValue(accumulator: LongWrapper, out: Collector[ResultWrapper]): Unit = {
    val result = new ResultWrapper
    result.a = 1
    out.collect(result)
    result.a = accumulator.l
    out.collect(result)
  }
}

class SimpleTVAGGWithTupleResult
  extends TableValuedAggregateFunction[(Int, Float, Double, Byte, Short, Time), LongWrapper] {
  override def createAccumulator(): LongWrapper = {
    new LongWrapper
  }

  def accumulate(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l += input
  }

  def retract(accumulator: LongWrapper, input: Int): Unit = {
    accumulator.l -= input
  }

  override def emitValue(
    accumulator: LongWrapper,
    out: Collector[(Int, Float, Double, Byte, Short, Time)]): Unit = {

    out.collect((1, 2.5f, 3.5, 127, 32767, new Time(0)))
    out.collect((accumulator.l.toInt, 2.5f, 3.5, 127, 32767, new Time(0)))
  }
}

class TVAGGWithRowAccumulator extends TableValuedAggregateFunction[Long, GenericRow] {
  override def createAccumulator(): GenericRow = {
    val row = new GenericRow(2)
    row.setLong(0, 0)
    row.setInt(1, 1)
    row
  }

  def accumulate(accumulator: GenericRow, input: Int): Unit = {
    accumulator.setLong(0, accumulator.getLong(0) + input)
  }

  def retract(accumulator: GenericRow, input: Int): Unit = {
    accumulator.setLong(0, accumulator.getLong(0) - input)
  }

  override def emitValue(accumulator: GenericRow, out: Collector[Long]): Unit = {
    out.collect(1)
    out.collect(accumulator.getLong(0))
  }

  override def getAccumulatorType: DataType = {
    new BaseRowType(classOf[GenericRow], DataTypes.LONG, DataTypes.INT)
  }
}

class TVAGGWithRow extends TableValuedAggregateFunction[GenericRow, GenericRow] {
  override def createAccumulator(): GenericRow = {
    val row = new GenericRow(2)
    row.setLong(0, 0)
    row.setInt(1, 1)
    row
  }

  def accumulate(accumulator: GenericRow, input: Int): Unit = {
    accumulator.setLong(0, accumulator.getLong(0) + input)
  }

  def retract(accumulator: GenericRow, input: Int): Unit = {
    accumulator.setLong(0, accumulator.getLong(0) - input)
  }

  override def emitValue(accumulator: GenericRow, out: Collector[GenericRow]): Unit = {
    val result1 = new GenericRow(3)
    result1.setLong(0, 1)
    result1.setInt(1, 1)
    result1.update(2, "first row")
    out.collect(result1)
    result1.setLong(0, accumulator.getLong(0))
    result1.setInt(1, accumulator.getInt(1))
    result1.update(2, "second row")
    out.collect(result1)
  }

  override def getAccumulatorType: DataType = {
    new BaseRowType(classOf[GenericRow], DataTypes.LONG, DataTypes.INT)
  }

  override def getResultType: DataType = {
    new BaseRowType(classOf[GenericRow], DataTypes.LONG, DataTypes.INT, DataTypes.STRING)
  }
}

class TVAGGDoNothing extends TableValuedAggregateFunction[Int, ArrayBuffer[Int]] {
  override def createAccumulator(): ArrayBuffer[Int] = {
    new ArrayBuffer[Int]()
  }

  def accumulate(accumulator: ArrayBuffer[Int], input: Int): Unit = {
    accumulator += input
  }

  def retract(accumulator: ArrayBuffer[Int], input: Int): Unit = {
    accumulator -= input
  }

  override def emitValue(accumulator: ArrayBuffer[Int], out: Collector[Int]): Unit = {
    accumulator.toArray.map(out.collect)
  }
}

class TVAGGDoNothingWithMerge extends TableValuedAggregateFunction[Int, ArrayBuffer[Int]] {
  override def createAccumulator(): ArrayBuffer[Int] = {
    new ArrayBuffer[Int]()
  }

  def accumulate(accumulator: ArrayBuffer[Int], input: Int): Unit = {
    accumulator += input
  }

  def retract(accumulator: ArrayBuffer[Int], input: Int): Unit = {
    accumulator -= input
  }

  def merge(accumulator: ArrayBuffer[Int], its: Iterable[ArrayBuffer[Int]]): Unit = {
    val it = its.iterator()
    while (it.hasNext) {
      accumulator ++= it.next()
    }
  }

  override def emitValue(accumulator: ArrayBuffer[Int], out: Collector[Int]): Unit = {
    accumulator.toArray.map(out.collect)
  }
}


class AccWithDataView {
  val map: MapView[String, Int] = new MapView(DataTypes.STRING, DataTypes.INT)
}

class TVAGGWithDataView extends TableValuedAggregateFunction[Int, AccWithDataView] {
  override def createAccumulator(): AccWithDataView = {
    new AccWithDataView
  }

  def accumulate(accumulator: AccWithDataView, input: Int): Unit = {
    if (null == accumulator.map.get("" + input)) {
      accumulator.map.put("" + input, 1)
    } else {
      accumulator.map.put("" + input, accumulator.map.get("" + input) + 1)
    }

  }

  def retract(accumulator: AccWithDataView, input: Int): Unit = {
    if (accumulator.map.get("" + input) != null) {
      accumulator.map.put("" + input, accumulator.map.get("" + input) - 1)
      if (accumulator.map.get("" + input) <= 0) {
        accumulator.map.remove("" + input)
      }
    } else {
      println("retract arrived before accumulate!")
    }
  }

  override def emitValue(accumulator: AccWithDataView, out: Collector[Int]): Unit = {

    val it = accumulator.map.keys.iterator()
    while(it.hasNext){
      out.collect(it.next().toInt)
    }
  }

  override def getAccumulatorType: DataType = {
    DataTypes.pojoBuilder(classOf[AccWithDataView])
    .field("map",
           DataTypes.of(
             new MapViewTypeInfo(
               DataTypes.toTypeInfo(DataTypes.STRING),
               DataTypes.toTypeInfo(DataTypes.INT), true))).build()
  }
}

class TVAGGWithDataViewWithRowAccType extends TableValuedAggregateFunction[Int, GenericRow] {
  override def createAccumulator(): GenericRow = {
    val row = new GenericRow(2)
    row.update(0, new MapView[String, Int](DataTypes.STRING, DataTypes.INT))
    row.update(1, new ListView[Int](DataTypes.INT))
    row
  }

  def accumulate(accumulator: GenericRow, input: Int): Unit = {
    val list = accumulator.getGeneric(1, new GenericType(classOf[ListView[Int]]))
    val map = accumulator.getGeneric(0, new GenericType(classOf[MapView[String, Int]]))
    list.add(input)
    if (null == map.get("" + input)) {
      map.put("" + input, 1)
    } else {
      map.put("" + input, map.get("" + input) + 1)
    }

  }

  def retract(accumulator: GenericRow, input: Int): Unit = {
    val list = accumulator.getGeneric(1, new GenericType(classOf[ListView[Int]]))
    val map = accumulator.getGeneric(0, new GenericType(classOf[MapView[String, Int]]))
    list.remove(input)
    if (map.get("" + input) != null) {
      map.put("" + input, map.get("" + input) - 1)
      if (map.get("" + input) <= 0) {
        map.remove("" + input)
      }
    } else {
      println("retract arrived before accumulate!")
    }
  }

  override def emitValue(accumulator: GenericRow, out: Collector[Int]): Unit = {
    val list = accumulator.getGeneric(1, new GenericType(classOf[ListView[Int]]))
    val map = accumulator.getGeneric(0, new GenericType(classOf[MapView[String, Int]]))
    val ints: Iterable[Int] = list.get

    if (ints != null) { //if list is empty, ints will be null
      val it = ints.iterator()
      while(it.hasNext){
        out.collect(it.next())
      }
    }

    val it = map.keys.iterator()
    while(it.hasNext){
      out.collect(it.next().toInt)
    }
  }

  override def getAccumulatorType: DataType = {
    new BaseRowType(
      classOf[GenericRow],
      DataTypes.internal(
        new MapViewTypeInfo(
          DataTypes.toTypeInfo(DataTypes.STRING),
          DataTypes.toTypeInfo(DataTypes.INT), true)),
      DataTypes.internal(
        new ListViewTypeInfo(
          DataTypes.toTypeInfo(DataTypes.INT), true)))
  }
}
