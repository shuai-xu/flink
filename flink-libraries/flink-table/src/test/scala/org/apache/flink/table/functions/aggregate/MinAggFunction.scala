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
import org.apache.flink.table.types.{DataType, DataTypes, DecimalType}

import java.lang.{Iterable => JIterable}
import java.math.BigDecimal

/** The initial accumulator for Min aggregate function */
class MinAccumulator[T] extends JTuple2[T, Boolean]

/**
  * Base class for built-in Min aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, MinAccumulator[T]] {

  override def createAccumulator(): MinAccumulator[T] = {
    val acc = new MinAccumulator[T]
    acc.f0 = getInitValue
    acc.f1 = false
    acc
  }

  def accumulate(acc: MinAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (!acc.f1 || ord.compare(acc.f0, v) > 0) {
        acc.f0 = v
        acc.f1 = true
      }
    }
  }

  override def getValue(acc: MinAccumulator[T]): T = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: MinAccumulator[T], its: JIterable[MinAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        accumulate(acc, a.f0)
      }
    }
  }

  def resetAccumulator(acc: MinAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = false
  }

  override def getAccumulatorType: DataType = {
    DataTypes.createTupleType(
      classOf[MinAccumulator[T]],
      getValueTypeInfo,
      DataTypes.BOOLEAN)
  }

  def getInitValue: T

  def getValueTypeInfo: DataType
}

/**
  * Built-in Byte Min aggregate function
  */
class ByteMinAggFunction extends MinAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = DataTypes.BYTE
}

/**
  * Built-in Short Min aggregate function
  */
class ShortMinAggFunction extends MinAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = DataTypes.SHORT
}

/**
  * Built-in Int Min aggregate function
  */
class IntMinAggFunction extends MinAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = DataTypes.INT
}

/**
  * Built-in Long Min aggregate function
  */
class LongMinAggFunction extends MinAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = DataTypes.LONG
}

/**
  * Built-in Float Min aggregate function
  */
class FloatMinAggFunction extends MinAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = DataTypes.FLOAT
}

/**
  * Built-in Double Min aggregate function
  */
class DoubleMinAggFunction extends MinAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Min aggregate function
  */
class BooleanMinAggFunction extends MinAggFunction[Boolean] {
  override def getInitValue: Boolean = false
  override def getValueTypeInfo = DataTypes.BOOLEAN
}

/**
  * Built-in Big Decimal Min aggregate function
  */
class DecimalMinAggFunction(decimalType: DecimalType)
  extends MinAggFunction[BigDecimal] {
  override def getInitValue: BigDecimal = BigDecimal.ZERO
  override def getValueTypeInfo = decimalType
}

/**
  * Built-in String Min aggregate function
  */
class StringMinAggFunction extends MinAggFunction[String] {
  override def getInitValue = ""
  override def getValueTypeInfo = DataTypes.STRING
}
