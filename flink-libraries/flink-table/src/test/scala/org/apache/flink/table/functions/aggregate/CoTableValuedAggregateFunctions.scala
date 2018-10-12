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

import org.apache.flink.api.java.tuple.Tuple6
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.functions.CoTableValuedAggregateFunction
import org.apache.flink.table.types.DataTypes
import org.apache.flink.util.Collector

class AccWrapper {
  // keep left data
  val leftMap: MapView[GenericRow, Integer] =
    new MapView(
      DataTypes.createRowType(DataTypes.INT, DataTypes.STRING), DataTypes.INT)
  // keep right data
  val rightMap: MapView[GenericRow, Integer] =
    new MapView(
      DataTypes.createRowType(DataTypes.INT, DataTypes.STRING, DataTypes.INT), DataTypes.INT)
  // how many input rows of both left and right
  var count: Integer = 0
}

abstract class TestInnerJoinBase[OUT]
  extends CoTableValuedAggregateFunction
  [OUT, AccWrapper] {

  def accumulateLeft(accumulator: AccWrapper, a: Integer, b: String): Unit = {
    val row = GenericRow.of(a, b)
    val oldCnt = accumulator.leftMap.get(row)
    val newCnt = if (oldCnt == null) 1 else oldCnt + 1
    accumulator.leftMap.put(row, newCnt)
    accumulator.count += 1
  }

  def accumulateRight(accumulator: AccWrapper, a: Integer, b: String, c: Integer): Unit = {
    val row = GenericRow.of(a, b, c)
    val oldCnt = accumulator.rightMap.get(row)
    val newCnt = if (oldCnt == null) 1 else oldCnt + 1
    accumulator.rightMap.put(row, newCnt)
    accumulator.count += 1
  }

  def retractLeft(accumulator: AccWrapper, a: Integer, b: String): Unit = {
    val row = GenericRow.of(a, b)
    val oldCnt = accumulator.leftMap.get(row)
    if (oldCnt != null) {
      val newCnt = oldCnt - 1
      accumulator.count -= 1
      if (newCnt == 0) {
        accumulator.leftMap.remove(row)
      } else {
        accumulator.leftMap.put(row, newCnt)
      }
    }
  }

  def retractRight(accumulator: AccWrapper, a: Integer, b: String, c: Integer): Unit = {
    val row = GenericRow.of(a, b, c)
    val oldCnt = accumulator.rightMap.get(row)
    if (oldCnt != null) {
      val newCnt = oldCnt - 1
      accumulator.count -= 1
      if (newCnt == 0) {
        accumulator.rightMap.remove(row)
      } else {
        accumulator.rightMap.put(row, newCnt)
      }
    }
  }

  override def emitValue(accumulator: AccWrapper, out: Collector[OUT]): Unit

  override def createAccumulator(): AccWrapper = {
    new AccWrapper
  }
}

/**
  * Inner join left and right data. Also count left and right input rows.
  */
class TestInnerJoinFunc
  extends TestInnerJoinBase[Tuple6[Integer, String, Integer, String, Integer, Integer]] {

  override def emitValue(
    accumulator: AccWrapper,
    out: Collector[Tuple6[Integer, String, Integer, String, Integer, Integer]]): Unit = {

    val leftIterator = accumulator.leftMap.iterator
    while (leftIterator.hasNext) {
      val leftEntry = leftIterator.next()
      val leftRow = leftEntry.getKey
      val leftCnt = leftEntry.getValue

      val rightIterator = accumulator.rightMap.iterator
      while (rightIterator.hasNext) {
        val rightEntry = rightIterator.next()
        val rightRow = rightEntry.getKey
        val rightCnt = rightEntry.getValue

        output(leftRow, leftCnt, rightRow, rightCnt, accumulator.count, out)
      }
    }
  }

  def output(
    leftRow: GenericRow,
    leftCnt: Integer,
    rightRow: GenericRow,
    rightCnt: Integer,
    count: Integer,
    out: Collector[Tuple6[Integer, String, Integer, String, Integer, Integer]]): Unit = {

    val leftTuple = (leftRow.getInt(0), leftRow.getString(1))
    val rightTuple = (rightRow.getInt(0), rightRow.getString(1), rightRow.getInt(2))
    for (i <- (0 until leftCnt * rightCnt)) {
      out.collect(new Tuple6(
        leftTuple._1, leftTuple._2, rightTuple._1, rightTuple._2, rightTuple._3, count))
    }
  }
}

class NonDeterministicInnerJoinFunc extends TestInnerJoinFunc {
  override def isDeterministic: Boolean = false
}

class JoinResult {
  var a: Integer = 0
  var b: String = ""
  var c: Integer = 0
  var d: String = ""
  var e: Integer = 0
  var f: Integer = 0
}
class TestInnerJoinWithPojoResultFunc
  extends TestInnerJoinBase[JoinResult] {

  override def emitValue(accumulator: AccWrapper, out: Collector[JoinResult]): Unit = {

    val leftIterator = accumulator.leftMap.iterator
    while (leftIterator.hasNext) {
      val leftEntry = leftIterator.next()
      val leftRow = leftEntry.getKey
      val leftCnt = leftEntry.getValue

      val rightIterator = accumulator.rightMap.iterator
      while (rightIterator.hasNext) {
        val rightEntry = rightIterator.next()
        val rightRow = rightEntry.getKey
        val rightCnt = rightEntry.getValue

        output(leftRow, leftCnt, rightRow, rightCnt, accumulator.count, out)
      }
    }
  }

  def output(
    leftRow: GenericRow,
    leftCnt: Integer,
    rightRow: GenericRow,
    rightCnt: Integer,
    count: Integer,
    out: Collector[JoinResult]): Unit = {

    val leftTuple = (leftRow.getInt(0), leftRow.getString(1))
    val rightTuple = (rightRow.getInt(0), rightRow.getString(1), rightRow.getInt(2))
    for (i <- (0 until leftCnt * rightCnt)) {
      val result = new JoinResult
      result.a = leftTuple._1
      result.b = leftTuple._2
      result.c = rightTuple._1
      result.d = rightTuple._2
      result.e = rightTuple._3
      result.f = count
      out.collect(result)
    }
  }

  override def createAccumulator(): AccWrapper = {
    new AccWrapper
  }
}
