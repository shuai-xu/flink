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

package org.apache.flink.table.plan.util

import java.util

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{MultisetTypeInfo, ObjectArrayTypeInfo}
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.{DataType, DataTypes}

import scala.collection.JavaConverters._

class ObjectExplodeTableFunc(componentType: TypeInformation[_]) extends TableFunction[Object] {
  def eval(arr: Array[Object]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Object, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }

  override def getParameterTypes(signature: Array[Class[_]]): Array[DataType] = {
    if (signature.head == classOf[Array[Object]]) {
      Array(DataTypes.createArrayType(DataTypes.of(componentType)))
    } else {
      Array(DataTypes.createMapType(DataTypes.of(componentType), DataTypes.INT))
    }
  }
}

class FloatExplodeTableFunc extends TableFunction[Float] {
  def eval(arr: Array[Float]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Float, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}

class ShortExplodeTableFunc extends TableFunction[Short] {
  def eval(arr: Array[Short]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Short, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}
class IntExplodeTableFunc extends TableFunction[Int] {
  def eval(arr: Array[Int]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Int, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}

class LongExplodeTableFunc extends TableFunction[Long] {
  def eval(arr: Array[Long]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Long, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}

class DoubleExplodeTableFunc extends TableFunction[Double] {
  def eval(arr: Array[Double]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Double, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}

class ByteExplodeTableFunc extends TableFunction[Byte] {
  def eval(arr: Array[Byte]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Byte, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}

class BooleanExplodeTableFunc extends TableFunction[Boolean] {
  def eval(arr: Array[Boolean]): Unit = {
    arr.foreach(collect)
  }

  def eval(map: util.Map[Boolean, Integer]): Unit = {
    CommonCollect.collect(map, collect)
  }
}

object CommonCollect {
  def collect[T](map: util.Map[T, Integer], collectFunc: (T) => Unit): Unit = {
    map.asScala.foreach{ e =>
      for (i <- 0 until e._2) {
        collectFunc(e._1)
      }
    }
  }
}

object ExplodeFunctionUtil {
  def explodeTableFuncFromType(ti: TypeInformation[_]): TableFunction[_] = {
    ti match {
      case pat: PrimitiveArrayTypeInfo[_] => createTableFuncByType(pat.getComponentType)

      case t: ObjectArrayTypeInfo[_, _] => new ObjectExplodeTableFunc(t.getComponentInfo)

      case t: BasicArrayTypeInfo[_, _] => new ObjectExplodeTableFunc(t.getComponentInfo)

      case mt: MultisetTypeInfo[_] => createTableFuncByType(mt.getElementTypeInfo)

      case _ => throw new UnsupportedOperationException(ti.toString + " IS NOT supported")
    }
  }

  def createTableFuncByType(typeInfo: TypeInformation[_]): TableFunction[_] = {
    typeInfo match {
      case BasicTypeInfo.INT_TYPE_INFO => new IntExplodeTableFunc
      case BasicTypeInfo.LONG_TYPE_INFO => new LongExplodeTableFunc
      case BasicTypeInfo.SHORT_TYPE_INFO => new ShortExplodeTableFunc
      case BasicTypeInfo.FLOAT_TYPE_INFO => new FloatExplodeTableFunc
      case BasicTypeInfo.DOUBLE_TYPE_INFO => new DoubleExplodeTableFunc
      case BasicTypeInfo.BYTE_TYPE_INFO => new ByteExplodeTableFunc
      case BasicTypeInfo.BOOLEAN_TYPE_INFO => new BooleanExplodeTableFunc
      case BasicTypeInfo.BOOLEAN_TYPE_INFO => new BooleanExplodeTableFunc
      case _ => new ObjectExplodeTableFunc(typeInfo)
    }
  }
}
