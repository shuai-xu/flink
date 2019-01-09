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

package org.apache.flink.table.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{AtomicType => AtomicTypeInfo, _}
import org.apache.flink.api.common.typeutils.base.{IntSerializer, LongSerializer}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.{PojoField, _}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.types._
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, Decimal}

import scala.collection.mutable

object TypeUtils {

  def getExternalClassForType(t: DataType): Class[_] = DataTypes.toTypeInfo(t).getTypeClass

  def getInternalClassForType(t: DataType): Class[_] = {
    t match {
      // primitives
      case DataTypes.BOOLEAN => BOOLEAN_TYPE_INFO.getTypeClass
      case DataTypes.BYTE => BYTE_TYPE_INFO.getTypeClass
      case DataTypes.SHORT => SHORT_TYPE_INFO.getTypeClass
      case DataTypes.INT | DataTypes.DATE | DataTypes.TIME => INT_TYPE_INFO.getTypeClass
      case DataTypes.LONG | DataTypes.TIMESTAMP => LONG_TYPE_INFO.getTypeClass
      case DataTypes.FLOAT => FLOAT_TYPE_INFO.getTypeClass
      case DataTypes.DOUBLE => DOUBLE_TYPE_INFO.getTypeClass
      case DataTypes.CHAR => CHAR_TYPE_INFO.getTypeClass

      case _: StringType => classOf[BinaryString]
      case dt: DecimalType => classOf[Decimal]
      case DataTypes.BYTE_ARRAY => BYTE_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass

      // temporal types
      case DataTypes.INTERVAL_MONTHS => TimeIntervalTypeInfo.INTERVAL_MONTHS.getTypeClass
      case DataTypes.INTERVAL_MILLIS => TimeIntervalTypeInfo.INTERVAL_MILLIS.getTypeClass
      case DataTypes.ROWTIME_INDICATOR => LONG_TYPE_INFO.getTypeClass
      case DataTypes.PROCTIME_INDICATOR => LONG_TYPE_INFO.getTypeClass
      case DataTypes.INTERVAL_ROWS => RowIntervalTypeInfo.INTERVAL_ROWS.getTypeClass

      // arrays and map types
      case at: ArrayType if at.isPrimitive => at.getElementType match {
        case DataTypes.BOOLEAN =>
          PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.SHORT =>
          PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.INT =>
          PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.LONG =>
          PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.FLOAT =>
          PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.DOUBLE =>
          PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.CHAR =>
          PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass
      }

      case at: ArrayType => at.getElementType match {
        case DataTypes.BOOLEAN => BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.SHORT => BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.INT => BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.LONG =>  BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.FLOAT => BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.DOUBLE => BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.CHAR => BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO.getTypeClass
        case DataTypes.STRING => BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO.getTypeClass
      }

      case mp: MultisetType =>
        new MultisetTypeInfo(createTypeInfoFromDataType(mp.getKeyType)).getTypeClass

      case mp: MapType =>
        new MapTypeInfo(
          createTypeInfoFromDataType(mp.getKeyType),
          createTypeInfoFromDataType(mp.getValueType)).getTypeClass

      // composite types
      case br: BaseRowType => classOf[BaseRow]

      case gt: GenericType[_] => gt.getTypeInfo.getTypeClass

      case et: TypeInfoWrappedDataType => et.getTypeInfo.getTypeClass

      case _ =>
        throw new TableException(s"Type is not supported: $t")
    }
  }

  def isPrimitive(dataType: TypeInformation[_]): Boolean = {
    dataType match {
      case BOOLEAN => true
      case BYTE => true
      case SHORT => true
      case INT => true
      case LONG => true
      case FLOAT => true
      case DOUBLE => true
      case _ => false
    }
  }

  def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case DataTypes.BOOLEAN => true
      case DataTypes.BYTE => true
      case DataTypes.SHORT => true
      case DataTypes.INT => true
      case DataTypes.LONG => true
      case DataTypes.FLOAT => true
      case DataTypes.DOUBLE => true
      case wt: TypeInfoWrappedDataType => isPrimitive(wt.getTypeInfo)
      case _ => false
    }
  }

  def isInternalCompositeType(t: TypeInformation[_]): Boolean = {
    t match {
      case _: BaseRowTypeInfo[_] |
           _: RowTypeInfo |
           _: PojoTypeInfo[_] |
           _: TupleTypeInfo[_] |
           _: CaseClassTypeInfo[_] =>
        true
      case _ => false
    }
  }

  def isInternalArrayType(t: TypeInformation[_]): Boolean = {
    t match {
      case _: PrimitiveArrayTypeInfo[_] |
           _: BasicArrayTypeInfo[_, _] |
           _: ObjectArrayTypeInfo[_, _] if t != BYTE_PRIMITIVE_ARRAY_TYPE_INFO =>
        true
      case _ => false
    }
  }

  def getBaseArraySerializer(t: InternalType): BaseArraySerializer = {
    t match {
      case a: ArrayType => new BaseArraySerializer(a.isPrimitive, a.getElementType)
    }
  }

  def getArrayElementType(t: TypeInformation[_]): TypeInformation[_] = {
    t match {
      case a: PrimitiveArrayTypeInfo[_] => a.getComponentType
      case a: BasicArrayTypeInfo[_, _] => a.getComponentInfo
      case a: ObjectArrayTypeInfo[_, _] => a.getComponentInfo
    }
  }

  def getCompositeTypes(t: TypeInformation[_]): Array[TypeInformation[_]] = {
    t match {
      case c: TupleTypeInfoBase[_] => (0 until t.getArity).map(c.getTypeAt).toArray
      case p: PojoTypeInfo[_] => (0 until p.getArity).map(p.getTypeAt).toArray
      case _ => Array(t)
    }
  }

  def flattenComparatorAndSerializer(
      arity: Int,
      keys: Array[Int],
      orders: Array[Boolean],
      types: Array[InternalType]): (Array[TypeComparator[_]], Array[TypeSerializer[_]]) = {
    val fieldComparators = new mutable.ArrayBuffer[TypeComparator[_]]()
    for (i <- keys.indices) {
      fieldComparators += createComparator(types(keys(i)), orders(i))
    }
    (fieldComparators.toArray, fieldComparators.indices.map((index) =>
      createSerializer(types(keys(index)))).toArray)
  }

  def flattenComparatorAndSerializer(
      arity: Int,
      keys: Array[Int],
      orders: Array[Boolean],
      types: Array[TypeInformation[_]]): (Array[TypeComparator[_]], Array[TypeSerializer[_]]) = {
    val fieldComparators = new mutable.ArrayBuffer[TypeComparator[_]]()
    for (i <- keys.indices) {
      fieldComparators += createComparator(types(keys(i)), orders(i))
    }
    (fieldComparators.toArray, fieldComparators.indices.map((index) =>
      createSerializer(types(keys(index)))).toArray)
  }

  @deprecated
  def createDataTypeFromTypeInfo(typeInfo: TypeInformation[_]): DataType =
    new TypeInfoWrappedDataType(typeInfo)

  def toBaseRowTypeInfo(t: BaseRowType): BaseRowTypeInfo[BaseRow] = {
    new BaseRowTypeInfo[BaseRow](
      t.getInternalTypeClass.asInstanceOf[Class[BaseRow]],
      t.getFieldInternalTypes.map(createTypeInfoFromDataType),
      t.getFieldNames)
  }

  /**
    * Create a TypeInformation from DataType.
    * Be careful, this will lose some information (such as precision).
    */
  def createTypeInfoFromDataType(t: DataType): TypeInformation[_] = {
    t match {
      //primitive types
      case DataTypes.BOOLEAN => BOOLEAN_TYPE_INFO
      case DataTypes.BYTE => BYTE_TYPE_INFO
      case DataTypes.SHORT => SHORT_TYPE_INFO
      case DataTypes.INT => INT_TYPE_INFO
      case DataTypes.LONG =>  LONG_TYPE_INFO
      case DataTypes.FLOAT => FLOAT_TYPE_INFO
      case DataTypes.DOUBLE => DOUBLE_TYPE_INFO
      case DataTypes.CHAR => CHAR_TYPE_INFO

      case _: StringType => STRING_TYPE_INFO
      case dt: DecimalType => BigDecimalTypeInfo.of(dt.precision, dt.scale);
      case DataTypes.BYTE_ARRAY => BYTE_PRIMITIVE_ARRAY_TYPE_INFO

      // temporal types
      case DataTypes.INTERVAL_MONTHS => TimeIntervalTypeInfo.INTERVAL_MONTHS
      case DataTypes.INTERVAL_MILLIS => TimeIntervalTypeInfo.INTERVAL_MILLIS
      case DataTypes.ROWTIME_INDICATOR => TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      case DataTypes.PROCTIME_INDICATOR => TimeIndicatorTypeInfo.PROCTIME_INDICATOR
      case DataTypes.INTERVAL_ROWS => RowIntervalTypeInfo.INTERVAL_ROWS

      case DataTypes.DATE => SqlTimeTypeInfo.DATE
      case DataTypes.TIME => SqlTimeTypeInfo.TIME
      case DataTypes.TIMESTAMP => SqlTimeTypeInfo.TIMESTAMP

      // arrays and map types
      case at: ArrayType if at.isPrimitive => at.getElementType match {
        case DataTypes.BOOLEAN => PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.SHORT => PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.INT => PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.LONG =>  PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.FLOAT => PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.DOUBLE => PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
        case DataTypes.CHAR => PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO
      }

      case at: ArrayType => at.getElementType match {
        case DataTypes.BOOLEAN => BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO
        case DataTypes.SHORT => BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO
        case DataTypes.INT => BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
        case DataTypes.LONG =>  BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
        case DataTypes.FLOAT => BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO
        case DataTypes.DOUBLE => BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO
        case DataTypes.CHAR => BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
        case DataTypes.STRING => BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO

        // object
        case _ => ObjectArrayTypeInfo.getInfoFor(createTypeInfoFromDataType(at.getElementType))
      }

      case mp: MultisetType => new MultisetTypeInfo(createTypeInfoFromDataType(mp.getKeyType))

      case mp: MapType =>
        new MapTypeInfo(
          createTypeInfoFromDataType(mp.getKeyType),
          createTypeInfoFromDataType(mp.getValueType))

      // composite types
      case br: BaseRowType =>
        if (br.isUseBaseRow) {
          new BaseRowTypeInfo(
            br.getInternalTypeClass,
            br.getFieldInternalTypes.map(createTypeInfoFromDataType), br.getFieldNames)
        } else {
          new RowTypeInfo(br.getFieldTypes.map(createTypeInfoFromDataType), br.getFieldNames)
        }

      case gt: GenericType[_] => gt.getTypeInfo

      case et: TypeInfoWrappedDataType => et.getTypeInfo

      case _ =>
        throw new TableException(s"Type is not supported: $t")
    }
  }

  def internalTypeFromTypeInfo(typeInfo: TypeInformation[_]): InternalType = typeInfo match {
    // built-in composite type info. (Need to be converted to BaseRowType)
    case rt: RowTypeInfo =>
      new BaseRowType(
        classOf[BaseRow],
        rt.getFieldTypes.map(DataTypes.of),
        rt.getFieldNames)

    case tt: TupleTypeInfo[_] =>
      new BaseRowType(
        (0 until tt.getArity).map(tt.getTypeAt).map(DataTypes.of).toArray,
        tt.getFieldNames)

    case pt: PojoTypeInfo[_] =>
      val fields = (0 until pt.getArity).map(pt.getPojoFieldAt)
      new BaseRowType(
        fields.map{(field: PojoField) =>
          DataTypes.of(field.getTypeInformation)}.toArray,
        fields.map{(field: PojoField) => field.getField.getName}.toArray)

    case cs: CaseClassTypeInfo[_] => new BaseRowType(
      (0 until cs.getArity).map(cs.getTypeAt).map(DataTypes.of).toArray,
      cs.fieldNames.toArray)

    //primitive types
    case BOOLEAN_TYPE_INFO => DataTypes.BOOLEAN
    case BYTE_TYPE_INFO => DataTypes.BYTE
    case SHORT_TYPE_INFO => DataTypes.SHORT
    case INT_TYPE_INFO => DataTypes.INT
    case LONG_TYPE_INFO => DataTypes.LONG
    case FLOAT_TYPE_INFO => DataTypes.FLOAT
    case DOUBLE_TYPE_INFO => DataTypes.DOUBLE
    case CHAR_TYPE_INFO => DataTypes.CHAR

    case STRING_TYPE_INFO | BinaryStringTypeInfo.INSTANCE => DataTypes.STRING
    case dt: BigDecimalTypeInfo => DataTypes.createDecimalType(dt.precision, dt.scale)
    case dt: DecimalTypeInfo => DataTypes.createDecimalType(dt.precision, dt.scale)
    case BYTE_PRIMITIVE_ARRAY_TYPE_INFO => DataTypes.BYTE_ARRAY

    // temporal types
    case TimeIntervalTypeInfo.INTERVAL_MONTHS => DataTypes.INTERVAL_MONTHS
    case TimeIntervalTypeInfo.INTERVAL_MILLIS => DataTypes.INTERVAL_MILLIS
    case RowIntervalTypeInfo.INTERVAL_ROWS => DataTypes.INTERVAL_ROWS

    // time indicators
    case SqlTimeTypeInfo.TIMESTAMP if typeInfo.isInstanceOf[TimeIndicatorTypeInfo] =>
      val indicator = typeInfo.asInstanceOf[TimeIndicatorTypeInfo]
      if (indicator.isEventTime) {
        DataTypes.ROWTIME_INDICATOR
      } else {
        DataTypes.PROCTIME_INDICATOR
      }

    case SqlTimeTypeInfo.DATE => DataTypes.DATE
    case SqlTimeTypeInfo.TIME => DataTypes.TIME
    case SqlTimeTypeInfo.TIMESTAMP => DataTypes.TIMESTAMP

    // arrays and map types
    case pa: PrimitiveArrayTypeInfo[_] =>
      DataTypes.createPrimitiveArrayType(internalTypeFromTypeInfo(pa.getComponentType))

    case ba: BasicArrayTypeInfo[_, _] =>
      DataTypes.createArrayType(internalTypeFromTypeInfo(ba.getComponentInfo))

    case oa: ObjectArrayTypeInfo[_, _] =>
      DataTypes.createArrayType(internalTypeFromTypeInfo(oa.getComponentInfo))

    case mp: MultisetTypeInfo[_] =>
      DataTypes.createMultisetType(internalTypeFromTypeInfo(mp.getElementTypeInfo))

    case mp: MapTypeInfo[_, _] =>
      DataTypes.createMapType(
        internalTypeFromTypeInfo(mp.getKeyTypeInfo),
        internalTypeFromTypeInfo(mp.getValueTypeInfo))

    case br: BaseRowTypeInfo[_] =>
      new BaseRowType(
        br.getTypeClass,
        br.getFieldTypes.map(DataTypes.of),
        br.getFieldNames,
        true)

    // unknown type info, treat as generic.
    case _ => DataTypes.createGenericType(typeInfo)
  }

  def createSerializer(t: TypeInformation[_]): TypeSerializer[_] = t match {
    case rt: RowTypeInfo => new BaseRowSerializer(rt.getFieldTypes: _*)
    case pj: PojoTypeInfo[_] => new BaseRowSerializer((0 until pj.getArity).map(
      pj.getPojoFieldAt).map{field: PojoField => field.getTypeInformation}: _*)
    case tt: TupleTypeInfo[_] => new BaseRowSerializer(
      (0 until tt.getArity).map(tt.getTypeAt): _*)
    case cc: CaseClassTypeInfo[_] => new BaseRowSerializer(
      (0 until cc.getArity).map(cc.getTypeAt): _*)
    case STRING_TYPE_INFO => BinaryStringSerializer.INSTANCE
    case SqlTimeTypeInfo.TIMESTAMP => LongSerializer.INSTANCE
    case SqlTimeTypeInfo.DATE => IntSerializer.INSTANCE
    case SqlTimeTypeInfo.TIME => IntSerializer.INSTANCE
    case _ if isInternalArrayType(t) => getBaseArraySerializer(DataTypes.internal(t))
    case mi: MapTypeInfo[_, _] =>
      new BaseMapSerializer(
        DataTypes.internal(mi.getKeyTypeInfo), DataTypes.internal(mi.getValueTypeInfo))
    case d: BigDecimalTypeInfo => new DecimalSerializer(d.precision(), d.scale())
    case ti => ti.createSerializer(new ExecutionConfig)
  }

  def createComparator(t: TypeInformation[_], order: Boolean)
    : TypeComparator[_] = t match {
    case rt: BaseRowTypeInfo[_] => new BaseRowComparator(rt.getFieldTypes, order)
    case rt: RowTypeInfo => new BaseRowComparator(rt.getFieldTypes, order)
    case pj: PojoTypeInfo[_] => new BaseRowComparator((0 until pj.getArity).map(
      pj.getPojoFieldAt).map{field: PojoField => field.getTypeInformation}.toArray, order)
    case tt: TupleTypeInfo[_] => new BaseRowComparator(
      (0 until tt.getArity).map(tt.getTypeAt).toArray, order)
    case cc: CaseClassTypeInfo[_] => new BaseRowComparator(
      (0 until cc.getArity).map(cc.getTypeAt).toArray, order)
    case STRING_TYPE_INFO => new BinaryStringComparator(order)
    case SqlTimeTypeInfo.TIMESTAMP => createComparator(LONG_TYPE_INFO, order)
    case SqlTimeTypeInfo.DATE => createComparator(INT_TYPE_INFO, order)
    case SqlTimeTypeInfo.TIME => createComparator(INT_TYPE_INFO, order)
    case d: BigDecimalTypeInfo => new DecimalComparator(order, d.precision(), d.scale())
    case at: AtomicTypeInfo[_] => at.createComparator(order, new ExecutionConfig)
  }

  def createSerializer(t: InternalType): TypeSerializer[_] =
    createSerializer(DataTypes.toTypeInfo(t))

  def createComparator(t: InternalType, order: Boolean): TypeComparator[_] =
    createComparator(DataTypes.toTypeInfo(t), order)

  def isGeneric(tp: DataType): Boolean = tp match {
    case _: GenericType[_] => true
    case wt: TypeInfoWrappedDataType =>
      wt.getTypeInfo match {
        // TODO special to trust it.
        case _: BinaryStringTypeInfo | _: DecimalTypeInfo => false
        case _ => isGeneric(wt.toInternalType)
      }
    case _ => false
  }
}
