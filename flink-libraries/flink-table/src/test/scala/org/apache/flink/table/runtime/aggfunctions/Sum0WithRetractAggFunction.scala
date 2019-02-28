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
package org.apache.flink.table.runtime.aggfunctions

import org.apache.flink.table.types.{DataTypes, DecimalType}

import java.math.BigDecimal

/**
  * Base class for built-in Sum0 with retract aggregate function.
  * If all values are null, 0 is returned.
  *
  * @tparam T the type for the aggregation result
  */
abstract class Sum0WithRetractAggFunction[T: Numeric] extends SumWithRetractAggFunction[T] {

  override def getValue(acc: SumWithRetractAccumulator[T]): T = {
    if (acc.f1 != 0) {
      acc.f0
    } else {
      0.asInstanceOf[T]
    }
  }
}

/**
  * Built-in Byte Sum0 with retract aggregate function
  */
class ByteSum0WithRetractAggFunction extends Sum0WithRetractAggFunction[Byte] {
  override def getValueTypeInfo = DataTypes.BYTE
}

/**
  * Built-in Short Sum0 with retract aggregate function
  */
class ShortSum0WithRetractAggFunction extends Sum0WithRetractAggFunction[Short] {
  override def getValueTypeInfo = DataTypes.SHORT
}

/**
  * Built-in Int Sum0 with retract aggregate function
  */
class IntSum0WithRetractAggFunction extends Sum0WithRetractAggFunction[Int] {
  override def getValueTypeInfo = DataTypes.INT
}

/**
  * Built-in Long Sum0 with retract aggregate function
  */
class LongSum0WithRetractAggFunction extends Sum0WithRetractAggFunction[Long] {
  override def getValueTypeInfo = DataTypes.LONG
}

/**
  * Built-in Float Sum0 with retract aggregate function
  */
class FloatSum0WithRetractAggFunction extends Sum0WithRetractAggFunction[Float] {
  override def getValueTypeInfo = DataTypes.FLOAT
}

/**
  * Built-in Double Sum0 with retract aggregate function
  */
class DoubleSum0WithRetractAggFunction extends Sum0WithRetractAggFunction[Double] {
  override def getValueTypeInfo = DataTypes.DOUBLE
}

/**
  * Built-in Big Decimal Sum0 with retract aggregate function
  */
class DecimalSum0WithRetractAggFunction(decimalType: DecimalType)
  extends DecimalSumWithRetractAggFunction(decimalType) {

  override def getValue(acc: DecimalSumWithRetractAccumulator): BigDecimal = {
    if (acc.f1 > 0) {
      acc.f0
    } else {
      BigDecimal.ZERO
    }
  }
}
