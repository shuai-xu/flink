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

package org.apache.flink.table.sqlgen

import java.lang.reflect.Modifier
import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo
import org.apache.flink.table.expressions.{Alias, EqualTo, WindowEnd, WindowStart}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.serialize
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.schema.ArrayRelDataType
import org.apache.flink.table.sources.{DefinedProctimeAttribute, TableSource}
import org.apache.flink.table.types.{DataTypes, DecimalType, GenericType, InternalType}
import org.apache.flink.util.InstantiationUtil

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

object SqlGenUtil {

  private val counter = new AtomicInteger(0)

  def getNextView(): String = s"TableView_${counter.getAndIncrement()}"

  def getNextTable(tableName: String) = s"${tableName}_${counter.getAndIncrement()}"

  def getNextDataName() = s"T_${counter.getAndIncrement()}"

  def getNextCounter() = s"${counter.getAndIncrement()}"

  def getSqlTypeName(relDataType: RelDataType): String = {

    def isByteArray(relDataType: RelDataType): Boolean = relDataType match {
      case array: ArrayRelDataType
        if array.typeInfo == PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => true
      case _ => false
    }

    relDataType.getSqlTypeName match {
      case SqlTypeName.VARCHAR => "VARCHAR"
      case SqlTypeName.BOOLEAN => "BOOLEAN"
      case SqlTypeName.TINYINT => "TINYINT"
      case SqlTypeName.SMALLINT => "SMALLINT"
      case SqlTypeName.INTEGER => "INT"
      case SqlTypeName.BIGINT => "BIGINT"
      case SqlTypeName.FLOAT => "FLOAT"
      case SqlTypeName.DECIMAL => "DECIMAL"
      case SqlTypeName.DOUBLE => "DOUBLE"
      case SqlTypeName.DATE => "DATE"
      case SqlTypeName.TIME => "TIME"
      case SqlTypeName.TIMESTAMP => "TIMESTAMP"
      case SqlTypeName.VARBINARY => "VARBINARY"
      case SqlTypeName.ARRAY if (isByteArray(relDataType)) => "VARBINARY"
      case SqlTypeName.ANY => "ANY"
      case _ => throw new RuntimeException(s"Unsupported sql type: ${relDataType.getSqlTypeName}" )
    }
  }

  // todo not sure yet
  def getTypeName(dataType: InternalType): String = dataType match {
    case DataTypes.STRING => "VARCHAR"
    case DataTypes.BOOLEAN => "BOOLEAN"
    case DataTypes.SHORT => "SMALLINT"
    case DataTypes.INT => "INT"
    case DataTypes.LONG => "BIGINT"
    case DataTypes.FLOAT => "FLOAT"
    case dt: DecimalType => "DECIMAL"
    case DataTypes.DOUBLE => "DOUBLE"
    case DataTypes.DATE => "DATE"
    case DataTypes.TIME => "TIME"
    case DataTypes.TIMESTAMP => "TIMESTAMP"
    case DataTypes.BYTE_ARRAY => "VARBINARY"
    case _: GenericType[_] => "ANY"
    case DataTypes.PROCTIME_INDICATOR => "PROCTIME"
    case _ => throw new RuntimeException(s"Unsupported TypeInfo: $dataType")
  }

  def getProcTimeAttr(table: TableSource): Option[String] = {
    table match {
      case procTable: DefinedProctimeAttribute => {
        if (procTable.getProctimeAttribute != null && !procTable.getProctimeAttribute.isEmpty) {
          Some(procTable.getProctimeAttribute)
        } else {
          None
        }
      }
      case _ => None
    }
  }

  def getTableProperties(table: AnyRef): Map[String, String] = {
    val properties = collection.mutable.LinkedHashMap[String, String]()
    table match {
      case source: ConnectorSource[_] => {
        val sourceBuilder = source.getSourceBuilder
        properties += "`type`" -> sourceBuilder.getType
        properties ++= sourceBuilder.getProperties.asScala
                       .map {
                         case (key: String, value: Object) =>
                           (s"`$key`", StringEscapeUtils.escapeJava(value.toString))}
        if (sourceBuilder.getSourceCollector != null) {
          properties += "`sourceCollector`" ->
            string2HexString(serializeObject(sourceBuilder.getSourceCollector))
        }
        properties.toMap
      }
      case sink: ConnectorSink[_] => {
        val sinkBuilder = sink.getSinkBuilder
        properties += "`type`" -> sinkBuilder.getType
        properties ++= sinkBuilder.getProperties.asScala
                       .map {case (key: String, value: Object) =>
                         (s"`$key`", StringEscapeUtils.escapeJava(value.toString))}
        if (sinkBuilder.getConverter != null) {
          properties += "`converter`" ->
            string2HexString(serializeObject(sinkBuilder.getConverter))
        }
        properties.toMap
      }
      case dim: ConnectorDimSource[_] => {
        val dimSourceBuilder = dim.getDimSourceBuilder
        properties += "`type`" -> dimSourceBuilder.getType
        properties ++= dimSourceBuilder.getProperties.asScala
                       .map {case (key: String, value: Object) =>
                         (s"`$key`", StringEscapeUtils.escapeJava(value.toString))}
        properties.toMap
      }
      case _ => throw new RuntimeException("SqlGen only support ConnectorSource or ConnectorSink")
    }
  }

  def getPrimaryKeys(tableSource: AnyRef): Array[String] = {
    tableSource match {
      case source: ConnectorSource[_] =>
        source.getSourceBuilder.getPrimaryKeys
      case sink: ConnectorSink[_] =>
        sink.getSinkBuilder.getPrimaryKeys
      case dim: ConnectorDimSource[_] =>
        dim.getDimSourceBuilder.getPrimaryKeys
      case _ => throw new RuntimeException("SqlGen only support ConnectorSource or ConnectorSink")
    }
  }

  def resolveWindowProperties(windowAggregate: WindowAggregate,
      exprVisitor: LogicalExprVisitor[String]): Seq[String] = {

    val windowType: String = windowAggregate.window match {
      case _: TumblingGroupWindow => "TUMBLE"
      case _: SlidingGroupWindow => "HOP"
      case _: SessionGroupWindow => "SESSION"
      case _ => throw new RuntimeException(s"Unknown window type: ${windowAggregate.window}")
    }

    windowAggregate.propertyExpressions.map {
      case Alias(WindowStart(_), name, _) =>
        windowAggregate.window.accept(exprVisitor)
        .replace(windowType, s"${windowType}_START") + s" AS $name"

      case Alias(WindowEnd(_), name, _) =>
        windowAggregate.window.accept(exprVisitor)
        .replace(windowType, s"${windowType}_END") + s" AS $name"
      case _ => throw new RuntimeException("Unknown window property")
    }
  }

  def isNestedTable(tableSource: TableSource): Boolean = {
    def isNestedClass(clazz: Class[_]): Boolean = {
      if (clazz == null) {
        false
      } else {
        clazz.getSimpleName match {
          case "Object" => false
          case "HBaseDimensionTableSource" => true
          case _ =>
            isNestedClass(clazz.getSuperclass) || clazz.getInterfaces.exists(isNestedClass(_))
        }
      }
    }
    isNestedClass(tableSource.getClass)
  }

  // hacking for fun
  def getNestedJoinExpr(tableSource: TableSource, sqlGenVisitor: SqlGenVisitor): String = {
    val text =
      s"LATERAL TABLE(ComplexDimTVFFunction(ComplexDimTVFParameter)) " +
        s"AS T${SqlGenUtil.getNextCounter()}" +
        s"(ComplexDimTVFReturn)_${getNextCounter}"
    sqlGenVisitor.nestedTableMap += text -> tableSource
    text
  }

  def getSideFieldName(join: Join, left: Boolean): String = {
    val side = if (left) {
      "left"
    } else {
      "right"
    }
    if (join.condition.isEmpty) {
      throw new RuntimeException("missing join condition in nesetedJoin")
    }
    join.condition.get match {
      case e: EqualTo => e.children match {
        case (x: JoinFieldReference) :: (y: JoinFieldReference) :: Nil => {
          if (x.isFromLeftInput == left) {
            x.name
          } else if (y.isFromLeftInput == left) {
            y.name
          } else {
            throw new RuntimeException(s"no field found form ${side}")
          }
        }
        case (x: JoinFieldReference) :: (_) :: Nil => {
          if (x.isFromLeftInput == left) {
            x.name
          } else {
            throw new RuntimeException(s"no field found form $side")
          }
        }
        case (_) :: (y: JoinFieldReference) :: Nil => {
          if (y.isFromLeftInput == left) {
            y.name
          } else {
            throw new RuntimeException(s"no field found form $side")
          }
        }
        case _ => throw new RuntimeException("unknown condition")
      }
      case _ => throw new RuntimeException("only EqualTo condition allowed in nested join")
    }
  }

  def serializeObject(ob: Object): String = {
    val byteArray = InstantiationUtil.serializeObject(ob)
    Base64.encodeBase64URLSafeString(byteArray)
  }

  def deSerializeObject(data: String): Object =
    deSerializeObject(data, Thread.currentThread.getContextClassLoader)

  def deSerializeObject(data: String, classLoader: ClassLoader): Object = {
    val byteData = Base64.decodeBase64(data)
    InstantiationUtil
    .deserializeObject[Object](byteData, classLoader)
  }

  def string2HexString(str: String): String = {
    str.map(_.toHexString).mkString("")
  }

  def hexString2String(str: String): String = {
    str.sliding(2, 2).map(s => Integer.parseInt(s, 16).asInstanceOf[Char].toString).mkString("")
  }

  def getFunctionUniqueKey(func: UserDefinedFunction) = {
    val md5 = DigestUtils.md5Hex(serialize(func))
    s"_udx_${func.toString}_$md5".replaceAll("[^0-9a-zA-Z]", "_")
  }

  def hasDefaultConstructor(func: UserDefinedFunction): Boolean = {
    Try {
      func.getClass.getConstructor()
    } match {
      case Success(constructor) if Modifier.isPublic(constructor.getModifiers) => true
      case _ => false
    }
  }

  def getFunctionIdAndContent(func: UserDefinedFunction): (String, String, Option[String]) = {
    val defaultConstructor: Boolean = hasDefaultConstructor(func)
    val funcId = if (defaultConstructor) {
      func.getClass.getSimpleName
    } else {
      getFunctionUniqueKey(func)
    }
    val content = if (defaultConstructor && func.getClass.getCanonicalName != null) {
      func.getClass.getCanonicalName
    } else {
      string2HexString(serialize(func))
    }
    val comment = getFunctionComment(func)
    (funcId, content, comment)
  }

  def getFunctionComment(func: UserDefinedFunction): Option[String] = {
    Try {
      func.getClass.getMethod("explain").invoke(func).asInstanceOf[String]
    } match {
      case Success(value) if value != null =>
        Some(value.replace("\n", "_"))
      case  _ => None
    }
  }

}
