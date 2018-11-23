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

package org.apache.flink.table.calcite

import java.util

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName, SqlTypeUtil}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, _}
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.java.typeutils.{MapTypeInfo, MultisetTypeInfo, ObjectArrayTypeInfo, RowTypeInfo, PojoField => _}
import org.apache.flink.table.api.types._
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory.typeInfoToSqlTypeName
import org.apache.flink.table.dataformat.{BaseRow, Decimal}
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.typeutils._
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink specific type factory that represents the interface between Flink's [[TypeInformation]]
  * and Calcite's [[RelDataType]].
  */
class FlinkTypeFactory(typeSystem: RelDataTypeSystem) extends JavaTypeFactoryImpl(typeSystem) {

  // NOTE: for future data types it might be necessary to
  // override more methods of RelDataTypeFactoryImpl

  private val seenTypes = mutable.HashMap[(TypeInformation[_], Boolean), RelDataType]()

  def isAdvanced(dataType: TypeInformation[_]): Boolean = dataType match {
    case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => false
    case _: TimeIndicatorTypeInfo => false
    case _: BasicTypeInfo[_] => false
    case _: SqlTimeTypeInfo[_] => false
    case _: TimeIntervalTypeInfo[_] => false
    case _ => true
  }

  def createTypeFromInternalType(
      t: InternalType,
      isNullable: Boolean)
  : RelDataType = {
    createTypeFromTypeInfo(DataTypes.toTypeInfo(t), isNullable)
  }

  def createTypeFromTypeInfo(
      typeInfo: TypeInformation[_],
      isNullable: Boolean)
  : RelDataType = {

    // we cannot use seenTypes for simple types,
    // because time indicators and timestamps would be the same

    val relType = if (!isAdvanced(typeInfo)) {
      // simple types can be converted to SQL types and vice versa
      val sqlType = typeInfoToSqlTypeName(typeInfo)
      sqlType match {

        case DECIMAL =>
          val dec = typeInfo.asInstanceOf[BigDecimalTypeInfo]
          createSqlType(DECIMAL, dec.precision, dec.scale)

        case INTERVAL_YEAR_MONTH =>
          createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO))

        case INTERVAL_DAY_SECOND =>
          createSqlIntervalType(
            new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))

        case TIMESTAMP if typeInfo.isInstanceOf[TimeIndicatorTypeInfo] =>
          if (typeInfo.asInstanceOf[TimeIndicatorTypeInfo].isEventTime) {
            createRowtimeIndicatorType()
          } else {
            createProctimeIndicatorType()
          }

        case _ =>
          createSqlType(sqlType)
      }
    } else {
      // advanced types require specific RelDataType
      // for storing the original TypeInformation
      seenTypes.getOrElseUpdate((typeInfo, isNullable), createAdvancedType(typeInfo, isNullable))
    }

    createTypeWithNullability(relType, isNullable)
  }

  /**
    * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
    */
  def createProctimeIndicatorType(): RelDataType = {
    val originalType = createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isEventTime = false)
    )
  }

  /**
    * Creates a indicator type for event-time, but with similar properties as SQL timestamp.
    */
  def createRowtimeIndicatorType(): RelDataType = {
    val originalType = createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)
    canonize(
      new TimeIndicatorRelDataType(
        getTypeSystem,
        originalType.asInstanceOf[BasicSqlType],
        isEventTime = true)
    )
  }

  /**
    * Creates types that create custom [[RelDataType]]s that wrap Flink's [[TypeInformation]].
    */
  private def createAdvancedType(
      typeInfo: TypeInformation[_],
      isNullable: Boolean): RelDataType = {

    val relType = typeInfo match {

      case ct: CompositeType[_] =>
        new CompositeRelDataType(ct, isNullable, this)

      case pa: PrimitiveArrayTypeInfo[_] =>
        new ArrayRelDataType(
          pa,
          createTypeFromTypeInfo(pa.getComponentType, isNullable = false),
          isNullable)

      case ba: BasicArrayTypeInfo[_, _] =>
        new ArrayRelDataType(
          ba,
          createTypeFromTypeInfo(ba.getComponentInfo, isNullable = true),
          isNullable)

      case oa: ObjectArrayTypeInfo[_, _] =>
        new ArrayRelDataType(
          oa,
          createTypeFromTypeInfo(oa.getComponentInfo, isNullable = true),
          isNullable)

      case mp: MapTypeInfo[_, _] =>
        new MapRelDataType(
          mp,
          createTypeFromTypeInfo(mp.getKeyTypeInfo, isNullable = true),
          createTypeFromTypeInfo(mp.getValueTypeInfo, isNullable = true),
          isNullable)

      case mts: MultisetTypeInfo[_] =>
        new MultisetRelDataType(
          mts,
          createTypeFromTypeInfo(mts.getElementTypeInfo, isNullable = true),
          isNullable
        )

      case ti: TypeInformation[_] =>
        new GenericRelDataType(
          ti,
          isNullable,
          getTypeSystem.asInstanceOf[FlinkTypeSystem])
    }

    canonize(relType)
  }

  /**
    * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory
    *
    * @param fieldNames field names
    * @param fieldTypes field types, every element is Flink's [[TypeInformation]]
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[TypeInformation[_]])
  : RelDataType = {
    buildLogicalRowType(
      fieldNames,
      fieldTypes,
      fieldTypes.map(!FlinkTypeFactory.isTimeIndicatorType(_)))
  }

  /**
    * Creates a struct type with the input fieldNames, input fieldTypes and input fieldNullables
    * using FlinkTypeFactory
    *
    * @param fieldNames     field names
    * @param fieldTypes     field types, every element is Flink's [[TypeInformation]]
    * @param fieldNullables field nullable properties
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(
      fieldNames: Seq[String],
      fieldTypes: Seq[TypeInformation[_]],
      fieldNullables: Seq[Boolean])
  : RelDataType = {
    val logicalRowTypeBuilder = builder
    val fields = fieldNames.zip(fieldTypes).zip(fieldNullables)
    fields foreach {
      case ((fieldName, fieldType), fieldNullable) =>
        if (FlinkTypeFactory.isTimeIndicatorType(fieldType) && fieldNullable) {
          throw new TableException(
            s"$fieldName can not be nullable because it is TimeIndicatorType!")
        }
        logicalRowTypeBuilder.add(fieldName, createTypeFromTypeInfo(fieldType, fieldNullable))
    }
    logicalRowTypeBuilder.build
  }

  /**
    * Created a struct type with the input table schema using FlinkTypeFactory
    * @param tableSchema  the table schema
    * @return a struct type with the input fieldNames, input fieldTypes, and system fields
    */
  def buildLogicalRowType(tableSchema: TableSchema, isStreaming: Boolean): RelDataType = {
    buildRelDataType(
      tableSchema.getColumnNames,
      tableSchema.getTypes map {
        case DataTypes.PROCTIME_INDICATOR if !isStreaming => DataTypes.TIMESTAMP
        case DataTypes.ROWTIME_INDICATOR if !isStreaming => DataTypes.TIMESTAMP
        case tpe: InternalType => tpe
      },
      tableSchema.getNullables)
  }

  def buildRelDataType(
      fieldNames: Seq[String],
      fieldTypes: Seq[InternalType])
  : RelDataType = {
    buildRelDataType(
      fieldNames,
      fieldTypes,
      fieldTypes.map(!FlinkTypeFactory.isTimeIndicatorType(_)))
  }

  def buildRelDataType(
      fieldNames: Seq[String],
      fieldTypes: Seq[InternalType],
      fieldNullables: Seq[Boolean])
  : RelDataType = {
    val b = builder
    val fields = fieldNames.zip(fieldTypes).zip(fieldNullables)
    fields foreach {
      case ((fieldName, fieldType), fieldNullable) =>
        if (FlinkTypeFactory.isTimeIndicatorType(fieldType) && fieldNullable) {
          throw new TableException(
            s"$fieldName can not be nullable because it is TimeIndicatorType!")
        }
        b.add(fieldName, createTypeFromInternalType(fieldType, fieldNullable))
    }
    b.build
  }

  // ----------------------------------------------------------------------------------------------


  override def getJavaClass(`type`: RelDataType): java.lang.reflect.Type = {
    if (`type`.getSqlTypeName == FLOAT) {
      if (`type`.isNullable) {
        classOf[java.lang.Float]
      } else {
        java.lang.Float.TYPE
      }
    } else {
      super.getJavaClass(`type`)
    }
  }

  override def createSqlType(typeName: SqlTypeName, precision: Int): RelDataType = {
    // it might happen that inferred VARCHAR types overflow as we set them to Int.MaxValue
    // Calcite will limit the length of the VARCHAR type to 65536.
    if (typeName == VARCHAR && precision < 0) {
      createSqlType(typeName, getTypeSystem.getDefaultPrecision(typeName))
    } else {
      super.createSqlType(typeName, precision)
    }
  }

  override def createSqlType(typeName: SqlTypeName): RelDataType = {
    if (typeName == DECIMAL) {
      // if we got here, the precision and scale are not specified, here we
      // keep precision/scale in sync with our type system's default value,
      // see DecimalType.USER_DEFAULT.
      createSqlType(typeName, DecimalType.USER_DEFAULT.precision(),
        DecimalType.USER_DEFAULT.scale())
    } else {
      super.createSqlType(typeName)
    }
  }

  override def createArrayType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    val arrayType = FlinkTypeFactory.toInternalType(elementType) match {
      case DataTypes.BOOLEAN => BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO
      case DataTypes.SHORT => BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO
      case DataTypes.INT => BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
      case DataTypes.LONG =>  BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO
      case DataTypes.FLOAT => BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO
      case DataTypes.DOUBLE => BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO
      case DataTypes.CHAR => BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO
      case DataTypes.STRING => BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO

      // object
      case _ => ObjectArrayTypeInfo.getInfoFor(FlinkTypeFactory.toTypeInfo(elementType))
    }
    val relType = new ArrayRelDataType(
      arrayType,
      elementType,
      isNullable = false)
    canonize(relType)
  }

  override def createMapType(keyType: RelDataType, valueType: RelDataType): RelDataType = {
    val relType = new MapRelDataType(
      new MapTypeInfo(
        FlinkTypeFactory.toTypeInfo(keyType),
        FlinkTypeFactory.toTypeInfo(valueType)),
      keyType,
      valueType,
      isNullable = false)
    canonize(relType)
  }

  override def createMultisetType(elementType: RelDataType, maxCardinality: Long): RelDataType = {
    val relType = new MultisetRelDataType(
      MultisetTypeInfo.getInfoFor(FlinkTypeFactory.toTypeInfo(elementType)),
      elementType,
      isNullable = false)
    canonize(relType)
  }

  override def createTypeWithNullability(
      relDataType: RelDataType,
      isNullable: Boolean): RelDataType = {

    // nullability change not necessary
    if (relDataType.isNullable == isNullable) {
      return canonize(relDataType)
    }

    // change nullability
    val newType = relDataType match {

      case composite: CompositeRelDataType =>
        new CompositeRelDataType(composite.compositeType, isNullable, this)

      case array: ArrayRelDataType =>
        new ArrayRelDataType(array.typeInfo, array.getComponentType, isNullable)

      case map: MapRelDataType =>
        new MapRelDataType(map.typeInfo, map.keyType, map.valueType, isNullable)

      case multiSet: MultisetRelDataType =>
        new MultisetRelDataType(multiSet.typeInfo, multiSet.getComponentType, isNullable)

      case generic: GenericRelDataType =>
        new GenericRelDataType(generic.typeInfo, isNullable, typeSystem)

      case timeIndicator: TimeIndicatorRelDataType =>
        timeIndicator

      case _ =>
        super.createTypeWithNullability(relDataType, isNullable)
    }

    canonize(newType)
  }

  override def leastRestrictive(types: util.List[RelDataType]): RelDataType = {
    val type0 = types.get(0)
    if (type0.getSqlTypeName != null) {
      val resultType = resolveAllIdenticalTypes(types)
      if (resultType.isDefined) {
        // result type for identical types
        return resultType.get
      }
    }
    // fall back to super
    super.leastRestrictive(types)
  }

  private def resolveAllIdenticalTypes(types: util.List[RelDataType]): Option[RelDataType] = {
    val allTypes = types.asScala

    val head = allTypes.head
    // check if all types are the same
    if (allTypes.forall(_ == head)) {
      // types are the same, check nullability
      val nullable = allTypes
        .exists(sqlType => sqlType.isNullable || sqlType.getSqlTypeName == SqlTypeName.NULL)
      // return type with nullability
      Some(createTypeWithNullability(head, nullable))
    } else {
      // types are not all the same
      if (allTypes.exists(_.getSqlTypeName == SqlTypeName.ANY)) {
        // one of the type was ANY.
        // we cannot generate a common type if it differs from other types.
        throw new TableException("Generic ANY types must have a common type information.")
      } else {
        // cannot resolve a common type for different input types
        None
      }
    }
  }

  // Calcite's default impl for division is apparently borrowed from T-SQL,
  //   but the details are a little different, e.g. when Decimal(34,0)/Decimal(10,0)
  //   To avoid confusion, follow the exact T-SQL behavior.
  // Note that for (+-*), Calcite is also different from T-SQL;
  //   however, Calcite conforms to SQL2003 while T-SQL does not.
  //   therefore we keep Calcite's behavior on (+-*)
  override def createDecimalQuotient(type1: RelDataType, type2: RelDataType): RelDataType = {
    if (SqlTypeUtil.isExactNumeric(type1) && SqlTypeUtil.isExactNumeric(type2) &&
      (SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
      val result = Decimal.inferDivisionType(
        type1.getPrecision, type1.getScale,
        type2.getPrecision, type2.getScale)
      createSqlType(SqlTypeName.DECIMAL, result.precision, result.scale)
    }
    else {
      null
    }
  }

}

object FlinkTypeFactory {

  private[flink] def typeInfoToSqlTypeName(typeInfo: TypeInformation[_]): SqlTypeName =
    typeInfo match {
      case BOOLEAN_TYPE_INFO => BOOLEAN
      case BYTE_TYPE_INFO => TINYINT
      case SHORT_TYPE_INFO => SMALLINT
      case INT_TYPE_INFO => INTEGER
      case LONG_TYPE_INFO => BIGINT
      case FLOAT_TYPE_INFO => FLOAT
      case DOUBLE_TYPE_INFO => DOUBLE
      case STRING_TYPE_INFO => VARCHAR
      case _: BigDecimalTypeInfo => DECIMAL

      // temporal types
      case SqlTimeTypeInfo.DATE => DATE
      case SqlTimeTypeInfo.TIME => TIME
      case SqlTimeTypeInfo.TIMESTAMP => TIMESTAMP
      case TimeIntervalTypeInfo.INTERVAL_MONTHS => INTERVAL_YEAR_MONTH
      case TimeIntervalTypeInfo.INTERVAL_MILLIS => INTERVAL_DAY_SECOND

      case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => VARBINARY

      case CHAR_TYPE_INFO | CHAR_VALUE_TYPE_INFO =>
        throw new TableException("Character type is not supported.")

      case _@t =>
        throw new TableException(s"Type is not supported: $t")
    }

  /**
    * Converts a Calcite logical record into a Flink type information.
    */
  @deprecated("Use the RowSchema class instead because it handles both logical and physical rows.")
  def toInternalRowTypeInfo(logicalRowType: RelDataType): TypeInformation[Row] = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList.asScala map { relDataType =>
      FlinkTypeFactory.toTypeInfo(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new RowTypeInfo(logicalFieldTypes.toArray, logicalFieldNames.toArray)
  }

  /**
    * Converts a Calcite logical record into a Flink BaseRow information.
    */
  def toInternalBaseRowTypeInfo[T <: BaseRow](
      logicalRowType: RelDataType, referType: Class[T]): BaseRowTypeInfo[T] = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList.asScala map { relDataType =>
      FlinkTypeFactory.toTypeInfo(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new BaseRowTypeInfo(referType, logicalFieldTypes.toArray, logicalFieldNames.toArray)
  }

  def toInternalBaseRowType[T <: BaseRow](
      logicalRowType: RelDataType, referType: Class[T]): BaseRowType = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList.asScala map { relDataType =>
      FlinkTypeFactory.toInternalType(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new BaseRowType(referType, logicalFieldTypes.toArray, logicalFieldNames.toArray)
  }

  def toInternalFieldTypes(logicalRowType: RelDataType): Seq[InternalType] = {
    logicalRowType.getFieldList.asScala map { relDataType =>
      FlinkTypeFactory.toInternalType(relDataType.getType)
    }
  }

  def newBaseRowTypeInfo[T <: BaseRow](
      types: Array[TypeInformation[_]], referType: Class[T]): BaseRowTypeInfo[T] = {
    new BaseRowTypeInfo(referType, types: _*)
  }

  def isProctimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if !ti.isEventTime => true
    case _ => false
  }

  def isProctimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if !ti.isEventTime => true
    case _ => false
  }

  def isProctimeIndicatorType(dataType: DataType): Boolean = {
    isProctimeIndicatorType(DataTypes.to(dataType))
  }

  def isRowtimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case ti: TimeIndicatorRelDataType if ti.isEventTime => true
    case _ => false
  }

  def isRowtimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case ti: TimeIndicatorTypeInfo if ti.isEventTime => true
    case _ => false
  }

  def isRowtimeIndicatorType(dataType: DataType): Boolean = {
    isRowtimeIndicatorType(DataTypes.to(dataType))
  }

  def isTimeIndicatorType(relDataType: RelDataType): Boolean = relDataType match {
    case _: TimeIndicatorRelDataType => true
    case _ => false
  }

  def isTimeIndicatorType(typeInfo: TypeInformation[_]): Boolean = typeInfo match {
    case _: TimeIndicatorTypeInfo => true
    case _ => false
  }

  def isTimeIndicatorType(t: InternalType): Boolean = t match {
    case DataTypes.ROWTIME_INDICATOR | DataTypes.PROCTIME_INDICATOR => true
    case _ => false
  }

  def toTypeInfo(relDataType: RelDataType): TypeInformation[_] = relDataType.getSqlTypeName match {
    case BOOLEAN => BOOLEAN_TYPE_INFO
    case TINYINT => BYTE_TYPE_INFO
    case SMALLINT => SHORT_TYPE_INFO
    case INTEGER => INT_TYPE_INFO
    case BIGINT => LONG_TYPE_INFO
    case FLOAT => FLOAT_TYPE_INFO
    case DOUBLE => DOUBLE_TYPE_INFO
    case VARCHAR | CHAR => STRING_TYPE_INFO
    case DECIMAL => BigDecimalTypeInfo.of(relDataType.getPrecision, relDataType.getScale)

    // time indicators
    case TIMESTAMP if relDataType.isInstanceOf[TimeIndicatorRelDataType] =>
      val indicator = relDataType.asInstanceOf[TimeIndicatorRelDataType]
      if (indicator.isEventTime) {
        TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      } else {
        TimeIndicatorTypeInfo.PROCTIME_INDICATOR
      }

    // temporal types
    case DATE => SqlTimeTypeInfo.DATE
    case TIME => SqlTimeTypeInfo.TIME
    case TIMESTAMP => SqlTimeTypeInfo.TIMESTAMP
    case typeName if YEAR_INTERVAL_TYPES.contains(typeName) => TimeIntervalTypeInfo.INTERVAL_MONTHS
    case typeName if DAY_INTERVAL_TYPES.contains(typeName) => TimeIntervalTypeInfo.INTERVAL_MILLIS

    case NULL =>
      throw new TableException(
        "Type NULL is not supported. Null values must have a supported type.")

    // symbol for special flags e.g. TRIM's BOTH, LEADING, TRAILING
    // are represented as integer
    case SYMBOL => INT_TYPE_INFO

    // extract encapsulated TypeInformation
    case ANY if relDataType.isInstanceOf[GenericRelDataType] =>
      val genericRelDataType = relDataType.asInstanceOf[GenericRelDataType]
      genericRelDataType.typeInfo

    case ROW if relDataType.isInstanceOf[CompositeRelDataType] =>
      val compositeRelDataType = relDataType.asInstanceOf[CompositeRelDataType]
      compositeRelDataType.compositeType

    case ROW if relDataType.isInstanceOf[RelRecordType] =>
      val relRecordType = relDataType.asInstanceOf[RelRecordType]
      new BaseRowSchema(relRecordType).typeInfo(classOf[BaseRow])

    // CURSOR for UDTF case, whose type info will never be used, just a placeholder
    case CURSOR => new NothingTypeInfo

    case ARRAY if relDataType.isInstanceOf[ArrayRelDataType] =>
      val arrayRelDataType = relDataType.asInstanceOf[ArrayRelDataType]
      arrayRelDataType.typeInfo

    case MAP if relDataType.isInstanceOf[MapRelDataType] =>
      val mapRelDataType = relDataType.asInstanceOf[MapRelDataType]
      mapRelDataType.typeInfo

    case VARBINARY | BINARY => PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO

    case MULTISET if relDataType.isInstanceOf[MultisetRelDataType] =>
      val multisetRelDataType = relDataType.asInstanceOf[MultisetRelDataType]
      multisetRelDataType.typeInfo

    case _@t =>
      throw new TableException(s"Type is not supported: $t")
  }

  def toInternalType(relDataType: RelDataType): InternalType =
    DataTypes.internal(FlinkTypeFactory.toTypeInfo(relDataType))

  def toDataType(relDataType: RelDataType): DataType =
    DataTypes.of(FlinkTypeFactory.toTypeInfo(relDataType))
}
