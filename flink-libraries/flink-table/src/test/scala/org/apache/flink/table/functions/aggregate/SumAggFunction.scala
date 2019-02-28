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

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.types.{DataType, DataTypes, DecimalType}

import java.lang.{Iterable => JIterable}
import java.math.BigDecimal

/** The initial accumulator for Sum aggregate function */
class SumAccumulator[T] extends JTuple2[T, Boolean]

/**
  * Base class for built-in Sum aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumAggFunction[T: Numeric] extends AggregateFunction[T, SumAccumulator[T]] {

  private val numeric = implicitly[Numeric[T]]

  override def createAccumulator(): SumAccumulator[T] = {
    val acc = new SumAccumulator[T]()
    acc.f0 = numeric.zero //sum
    acc.f1 = false
    acc
  }

  def accumulate(accumulator: SumAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      accumulator.f0 = numeric.plus(v, accumulator.f0)
      accumulator.f1 = true
    }
  }

  override def getValue(accumulator: SumAccumulator[T]): T = {
    if (accumulator.f1) {
      accumulator.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: SumAccumulator[T], its: JIterable[SumAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        acc.f0 = numeric.plus(acc.f0, a.f0)
        acc.f1 = true
      }
    }
  }

  def resetAccumulator(acc: SumAccumulator[T]): Unit = {
    acc.f0 = numeric.zero
    acc.f1 = false
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(
      classOf[SumAccumulator[T]],
      getValueTypeInfo,
      DataTypes.BOOLEAN)
  }

  def getValueTypeInfo: DataType
}

/**
  * Built-in Byte Sum aggregate function
  */
class ByteSumAggFunction extends SumAggFunction[Byte] {
  override def getValueTypeInfo = DataTypes.BYTE
}

/**
  * Built-in Short Sum aggregate function
  */
class ShortSumAggFunction extends SumAggFunction[Short] {
  override def getValueTypeInfo = DataTypes.SHORT
}

/**
  * Built-in Int Sum aggregate function
  */
class IntSumAggFunction extends SumAggFunction[Int] {
  override def getValueTypeInfo = DataTypes.INT
}

/**
  * Built-in Long Sum aggregate function
  */
class LongSumAggFunction extends SumAggFunction[Long] {
  override def getValueTypeInfo = DataTypes.LONG
}

/**
  * Built-in Float Sum aggregate function
  */
class FloatSumAggFunction extends SumAggFunction[Float] {
  override def getValueTypeInfo = DataTypes.FLOAT
}

/**
  * Built-in Double Sum aggregate function
  */
class DoubleSumAggFunction extends SumAggFunction[Double] {
  override def getValueTypeInfo = DataTypes.DOUBLE
}

/** The initial accumulator for Big Decimal Sum aggregate function */
class DecimalSumAccumulator extends JTuple2[BigDecimal, Boolean] {
  f0 = BigDecimal.ZERO
  f1 = false
}

/**
  * Built-in Big Decimal Sum aggregate function
  */
class DecimalSumAggFunction(argType: DecimalType)
  extends AggregateFunction[BigDecimal, DecimalSumAccumulator] {

  override def createAccumulator(): DecimalSumAccumulator = {
    new DecimalSumAccumulator
  }

  def accumulate(acc: DecimalSumAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      acc.f0 = acc.f0.add(v)
      acc.f1 = true
    }
  }

  override def getValue(acc: DecimalSumAccumulator): BigDecimal = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[BigDecimal]
    }
  }

  def merge(acc: DecimalSumAccumulator, its: JIterable[DecimalSumAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        acc.f0 = acc.f0.add(a.f0)
        acc.f1 = true
      }
    }
  }

  def resetAccumulator(acc: DecimalSumAccumulator): Unit = {
    acc.f0 = BigDecimal.ZERO
    acc.f1 = false
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(
      classOf[DecimalSumAccumulator],
      getResultType,
      DataTypes.BOOLEAN)
  }

  override def getResultType: DataType =
    Decimal.inferAggSumType(argType.precision, argType.scale)
}
