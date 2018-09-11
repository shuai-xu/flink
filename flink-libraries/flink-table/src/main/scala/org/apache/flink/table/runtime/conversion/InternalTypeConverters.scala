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

package org.apache.flink.table.runtime.conversion

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.util
import java.util.{Map => JavaMap}
import javax.annotation.Nullable

import org.apache.commons.codec.binary.Base64
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.table.api.Types
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BinaryArray.calculateElementSize
import org.apache.flink.table.dataformat._
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions
import org.apache.flink.table.types.{DataTypes, _}
import org.apache.flink.table.typeutils.TypeUtils._
import org.apache.flink.table.typeutils.{BinaryStringTypeInfo, DecimalTypeInfo, TypeUtils}
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil

import scala.collection.JavaConverters._

/**
 * Functions to convert Scala types to internal types.
 */
object InternalTypeConverters {

  def getConverterForType(t: DataType): InternalTypeConverter[Any, Any, Any] = {
    val converter = t match {
      case DataTypes.STRING => StringConverter
      case DataTypes.INTERVAL_MILLIS => LongConverter
      case DataTypes.INTERVAL_MONTHS => IntConverter
      case _: TimestampType => TimestampConverter
      case _: DateType => DateConverter
      case DataTypes.TIME => TimeConverter
      case DataTypes.BOOLEAN => BooleanConverter
      case DataTypes.BYTE => ByteConverter
      case DataTypes.SHORT => ShortConverter
      case DataTypes.CHAR => CharConverter
      case DataTypes.INT => IntConverter
      case DataTypes.LONG => LongConverter
      case DataTypes.FLOAT => FloatConverter
      case DataTypes.DOUBLE => DoubleConverter
      case dt: DecimalType => DecimalConverter(dt)
      case DataTypes.BYTE_ARRAY => ByteArrayConverter

      case at: ArrayType if at.isPrimitive => PrimitiveArrayConverter(at.getElementType)
      case at: ArrayType => ObjectArrayConverter(
        TypeUtils.getExternalClassForType(at),
        at.getElementType)
      case mt: MapType => MapConverter(mt.getKeyType, mt.getValueType)

      case br: BaseRowType if br.getTypeClass == classOf[GenericRow] => GenericRowConverter(br)
      case br: BaseRowType => BaseRowConverter(br)
      case gt: GenericType[_] => GenericConverter(gt)

      case tw: TypeInfoWrappedType =>
        tw.getTypeInfo match {
          case pa: PrimitiveArrayTypeInfo[_] if pa != BYTE_PRIMITIVE_ARRAY_TYPE_INFO =>
            PrimitiveArrayConverter(DataTypes.of(pa.getComponentType))
          case ba: BasicArrayTypeInfo[_, _] =>
            ObjectArrayConverter(ba.getTypeClass, DataTypes.of(ba.getComponentInfo))
          case oa: ObjectArrayTypeInfo[_, _] =>
            ObjectArrayConverter(oa.getTypeClass, DataTypes.of(oa.getComponentInfo))
          case mt: MapTypeInfo[_, _] =>
            MapConverter(DataTypes.of(mt.getKeyTypeInfo), DataTypes.of(mt.getValueTypeInfo))
          case rt: RowTypeInfo => RowConverter(rt)
          case pj: PojoTypeInfo[_] => PojoConverter(pj)
          case tt: TupleTypeInfo[_] => TupleConverter(tt)
          case cc: CaseClassTypeInfo[Product] => CaseClassConverter(cc)
          case _: BinaryStringTypeInfo => BinaryStringConverter
          case d: DecimalTypeInfo => InternalDecimalConverter(d)
          case _ => getConverterForType(tw.toInternalType)
        }
    }
    converter.asInstanceOf[InternalTypeConverter[Any, Any, Any]]
  }

  abstract class InternalTypeConverter[ScalaInputType, ExternalOutputType, InternalType]
    extends Serializable {

    /**
     * Converts a Scala type to its internal equivalent while automatically handling nulls.
     */
    final def toInternal(@Nullable value: Any): InternalType =
      if (value == null) {
        null.asInstanceOf[InternalType]
      } else {
        toInternalImpl(value.asInstanceOf[ScalaInputType])
      }

    /**
      * Convert a internal value to its Java/Scala equivalent while automatically handling nulls.
      */
    final def toExternal(@Nullable internalValue: InternalType): ExternalOutputType =
      if (internalValue == null) {
        null.asInstanceOf[ExternalOutputType]
      } else {
        toExternalImpl(internalValue)
      }

    final def toExternal(row: BaseRow, column: Int): ExternalOutputType = {
      if (row.isNullAt(column)) {
        null.asInstanceOf[ExternalOutputType]
      } else {
        toExternalImpl(row, column)
      }
    }

    /**
     * Converts a Scala value to its internal equivalent.
     * @param scalaValue the Scala value, guaranteed not to be null.
     * @return the internal value.
     */
    protected def toInternalImpl(scalaValue: ScalaInputType): InternalType

    /**
      * Convert a internal value to its Java/Scala equivalent.
      */
    def toExternalImpl(internalValue: InternalType): ExternalOutputType

    /**
      * Given a internal row, convert the value at column `column` to its Java/Scala equivalent.
      * This method will only be called on non-null columns.
      */
    protected def toExternalImpl(row: BaseRow, column: Int): ExternalOutputType
  }

  abstract class AbstractBaseRowConverter[T](numField: Int)
      extends InternalTypeConverter[T, T, BaseRow] {
    override def toExternalImpl(row: BaseRow, column: Int): T =
      toExternalImpl(row.getBaseRow(column, numField))
  }

  case class RowConverter(t: RowTypeInfo)
      extends AbstractBaseRowConverter[Row](t.getArity) {

    private[this] val converters = t.getFieldTypes.map(DataTypes.of).map(getConverterForType)

    override def toInternalImpl(row: Row): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(row.getField(idx)))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Row = {
      val row = new Row(baseRow.getArity)
      var idx = 0
      while (idx < baseRow.getArity) {
        row.setField(idx, converters(idx).toExternal(baseRow, idx))
        idx += 1
      }
      row
    }
  }

  case class TupleConverter(t: TupleTypeInfo[_])
      extends AbstractBaseRowConverter[Tuple](t.getArity) {

    private[this] val converters =
      (0 until t.getArity).map(t.getTypeAt).map(DataTypes.of).map(getConverterForType)

    override def toInternalImpl(tuple: Tuple): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(tuple.getField(idx)))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Tuple = {
      val tuple = t.getTypeClass.newInstance().asInstanceOf[Tuple]
      var idx = 0
      while (idx < baseRow.getArity) {
        tuple.setField(converters(idx).toExternal(baseRow, idx), idx)
        idx += 1
      }
      tuple
    }
  }

  case class CaseClassConverter(t: CaseClassTypeInfo[Product])
      extends AbstractBaseRowConverter[Product](t.getArity) {

    private[this] val converters =
      (0 until t.getArity).map(t.getTypeAt).map(DataTypes.of).map(getConverterForType)
    private val serializer = t.createSerializer(new ExecutionConfig)
        .asInstanceOf[CaseClassSerializer[_]]

    override def toInternalImpl(p: Product): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      val iter = p.productIterator
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(iter.next()))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Product = {
      val fields = new Array[AnyRef](t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        fields(idx) = converters(idx).toExternal(baseRow, idx).asInstanceOf[AnyRef]
        idx += 1
      }
      serializer.createInstance(fields).asInstanceOf[Product]
    }
  }

  case class BaseRowConverter(t: BaseRowType)
      extends AbstractBaseRowConverter[BaseRow](t.getArity) {
    override def toInternalImpl(row: BaseRow): BaseRow = row

    override def toExternalImpl(baseRow: BaseRow): BaseRow = baseRow
  }

  case class GenericRowConverter(t: BaseRowType)
    extends AbstractBaseRowConverter[GenericRow](t.getArity) {

    private[this] val converters =
      (0 until t.getArity).map(t.getTypeAt).map(getConverterForType)

    override def toInternalImpl(row: GenericRow): BaseRow = row

    override def toExternalImpl(baseRow: BaseRow): GenericRow = {
      baseRow match {
        case row: GenericRow =>
          row
        case _ =>
          val row = new GenericRow(baseRow.getArity)
          var idx = 0
          while (idx < baseRow.getArity) {
            row.update(idx, converters(idx).toExternal(baseRow, idx))
            idx += 1
          }
          row
      }
    }
  }

  case class PojoConverter(t: PojoTypeInfo[_])
      extends AbstractBaseRowConverter[Any](t.getArity) {

    private[this] val converters =
      (0 until t.getArity).map(t.getTypeAt).map(DataTypes.of).map(getConverterForType)
    private val fields = (0 until t.getArity).map {idx =>
      val field = t.getPojoFieldAt(idx)
      field.getField.setAccessible(true)
      field
    }

    override def toInternalImpl(pojo: Any): BaseRow = {
      val genericRow = new GenericRow(t.getArity)
      var idx = 0
      while (idx < t.getArity) {
        genericRow.update(idx, converters(idx).toInternal(
          fields(idx).getField.get(pojo)))
        idx += 1
      }
      genericRow
    }

    override def toExternalImpl(baseRow: BaseRow): Any = {
      val pojo = t.getTypeClass.newInstance()
      var idx = 0
      while (idx < baseRow.getArity) {
        fields(idx).getField.set(pojo, converters(idx).toExternal(baseRow, idx))
        idx += 1
      }
      pojo
    }
  }

  abstract class ArrayConverter(val elementType: DataType)
      extends InternalTypeConverter[Any, Any, BinaryArray] {

    val internalEleT: InternalType = DataTypes.internal(elementType)
    val elementConverter: InternalTypeConverter[Any, Any, Any] = getConverterForType(elementType)
    private val elementSize = calculateElementSize(internalEleT)

    override def toInternalImpl(scalaValue: Any): BinaryArray = {
      scalaValue match {
        case a: Array[_] =>
          val array = new BinaryArray
          val arrayWriter = new BinaryArrayWriter(array, a.length, elementSize)
          var i = 0
          while (i < a.length) {
            val o = a(i)
            if (o == null) {
              arrayWriter.setNullAt(i, internalEleT)
            } else {
              arrayWriter.write(i, elementConverter.toInternal(o), internalEleT)
            }
            i += 1
          }
          arrayWriter.complete()
          array
        case s: Seq[_] =>
          val array = new BinaryArray
          val arrayWriter = new BinaryArrayWriter(array, s.length, elementSize)
          var i = 0
          while (i < s.length) {
            val o = s(i)
            if (o == null) {
              arrayWriter.setNullAt(i, internalEleT)
            } else {
              arrayWriter.write(i, elementConverter.toInternal(o), internalEleT)
            }
            i += 1
          }
          arrayWriter.complete()
          array
      }
    }

    override def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getArray(column))
  }

  case class PrimitiveArrayConverter(eleType: DataType) extends ArrayConverter(eleType) {
    override def toExternalImpl(internalValue: BinaryArray): Any = {
      internalValue.toPrimitiveArray(DataTypes.internal(eleType))
    }
  }

  case class ObjectArrayConverter(
      typeClass: Class[_],
      eleType: DataType) extends ArrayConverter(eleType) {
    override def toExternalImpl(internalValue: BinaryArray): Any = {
      val array = internalValue.toArray(internalEleT)
          .map(elementConverter.toExternal)
      if (typeClass eq classOf[Array[AnyRef]]) {
        array
      } else {
        util.Arrays.copyOf(
          array.asInstanceOf[Array[AnyRef]],
          internalValue.numElements(),
          typeClass.asInstanceOf[Class[Array[AnyRef]]])
      }
    }
  }

  case class MapConverter(keyType: DataType, valueType: DataType)
      extends InternalTypeConverter[Any, Any, BinaryMap] {

    private val internalKeyT = DataTypes.internal(keyType)
    private val internalValueT = DataTypes.internal(valueType)

    private val keyConverter = getConverterForType(keyType)
    private val valueConverter = getConverterForType(valueType)

    private val keyElementSize = calculateElementSize(internalKeyT)
    private val valueElementSize = calculateElementSize(internalValueT)

    private def toBinaryArray(
        a: Array[_],
        elementSize: Int,
        elementType: InternalType,
        elementConverter: InternalTypeConverter[_, _, _]) = {
      val array = new BinaryArray
      val arrayWriter = new BinaryArrayWriter(array, a.length, elementSize)
      a.zipWithIndex.foreach { case (o, index: Int) =>
        if (o == null) {
          arrayWriter.setNullAt(index, elementType)
        } else {
          arrayWriter.write(index, elementConverter.toInternal(o), elementType)
        }
      }
      arrayWriter.complete()
      array
    }

    override def toInternalImpl(scalaValue: Any): BinaryMap = {

      val (keys, values) = scalaValue match {
        case map: Map[_, _] =>
          val keys: Array[Any] = new Array[Any](map.size)
          val values: Array[Any] = new Array[Any](map.size)

          var i = 0
          for ((key, value) <- map.iterator) {
            keys(i) = key
            values(i) = value
            i += 1
          }
          (keys, values)
        case javaMap: JavaMap[_, _] =>
          val keys: Array[Any] = new Array[Any](javaMap.size())
          val values: Array[Any] = new Array[Any](javaMap.size())

          var i: Int = 0
          val iterator = javaMap.entrySet().iterator()
          while (iterator.hasNext) {
            val entry = iterator.next()
            keys(i) = entry.getKey
            values(i) = entry.getValue
            i += 1
          }
          (keys, values)
      }

      BinaryMap.valueOf(
        toBinaryArray(keys, keyElementSize, internalKeyT, keyConverter),
        toBinaryArray(values, valueElementSize, internalValueT, valueConverter))
    }

    override def toExternalImpl(internalValue: BinaryMap): Any = {
      val keys = internalValue.keyArray().toArray(internalKeyT)
      val values = internalValue.valueArray().toArray(internalValueT)
      val convertedKeys =
        if (isPrimitive(keyType)) keys else keys.map(keyConverter.toExternal)
      val convertedValues =
        if (isPrimitive(valueType)) values else values.map(valueConverter.toExternal)
      // avoid use scala toMap here, because this may change the order of map values
      val map = new util.HashMap[Any, Any]()
      for (i <- keys.indices) {
        map.put(convertedKeys(i), convertedValues(i))
      }
      map
    }

    override def toExternalImpl(row: BaseRow, column: Int): Any =
      toExternalImpl(row.getMap(column))
  }

  object StringConverter extends InternalTypeConverter[Any, String, BinaryString] {
    override def toInternalImpl(scalaValue: Any): BinaryString = scalaValue match {
      case str: String => BinaryString.fromString(str)
      case utf8: BinaryString => utf8
    }
    override def toExternalImpl(internalValue: BinaryString): String = internalValue.toString
    override def toExternalImpl(row: BaseRow, column: Int): String =
      row.getBinaryString(column).toString
  }

  object TimestampConverter extends InternalTypeConverter[Timestamp, Timestamp, Any] {
    override def toInternalImpl(scalaValue: Timestamp): Long =
      BuildInScalarFunctions.toLong(scalaValue)
    override def toExternalImpl(internalValue: Any): Timestamp =
      BuildInScalarFunctions.internalToTimestamp(internalValue.asInstanceOf[Long])
    override def toExternalImpl(row: BaseRow, column: Int): Timestamp =
      BuildInScalarFunctions.internalToTimestamp(row.getLong(column))
  }

  object DateConverter extends InternalTypeConverter[Date, Date, Any] {
    override def toInternalImpl(scalaValue: Date): Int = BuildInScalarFunctions.toInt(scalaValue)
    override def toExternalImpl(internalValue: Any): Date =
      BuildInScalarFunctions.internalToDate(internalValue.asInstanceOf[Int])
    override def toExternalImpl(row: BaseRow, column: Int): Date =
      BuildInScalarFunctions.internalToDate(row.getInt(column))
  }

  object TimeConverter extends InternalTypeConverter[Time, Time, Any] {
    override def toInternalImpl(scalaValue: Time): Int = BuildInScalarFunctions.toInt(scalaValue)
    override def toExternalImpl(internalValue: Any): Time =
      BuildInScalarFunctions.internalToTime(internalValue.asInstanceOf[Int])
    override def toExternalImpl(row: BaseRow, column: Int): Time =
      BuildInScalarFunctions.internalToTime(row.getInt(column))
  }

  abstract class IdentityConverter[T] extends InternalTypeConverter[T, Any, Any] {
    final override def toExternalImpl(internalValue: Any): Any = internalValue
    final override def toInternalImpl(scalaValue: T): Any = scalaValue
  }

  object BooleanConverter extends IdentityConverter[Boolean] {
    override def toExternalImpl(row: BaseRow, column: Int): Boolean = row.getBoolean(column)
  }

  object ByteConverter extends IdentityConverter[Byte] {
    override def toExternalImpl(row: BaseRow, column: Int): Byte = row.getByte(column)
  }

  object ShortConverter extends IdentityConverter[Short] {
    override def toExternalImpl(row: BaseRow, column: Int): Short = row.getShort(column)
  }

  object CharConverter extends IdentityConverter[Character] {
    override def toExternalImpl(row: BaseRow, column: Int): Character = row.getChar(column)
  }

  object IntConverter extends IdentityConverter[Int] {
    override def toExternalImpl(row: BaseRow, column: Int): Int = row.getInt(column)
  }

  object LongConverter extends IdentityConverter[Long] {
    override def toExternalImpl(row: BaseRow, column: Int): Long = row.getLong(column)
  }

  object FloatConverter extends IdentityConverter[Float] {
    override def toExternalImpl(row: BaseRow, column: Int): Float = row.getFloat(column)
  }

  object DoubleConverter extends IdentityConverter[Double] {
    override def toExternalImpl(row: BaseRow, column: Int): Double = row.getDouble(column)
  }

  object ByteArrayConverter extends IdentityConverter[Array[Byte]] {
    override def toExternalImpl(row: BaseRow, column: Int): Array[Byte] = row.getByteArray(column)
  }

  object BinaryStringConverter extends IdentityConverter[BinaryString] {
    override def toExternalImpl(row: BaseRow, column: Int): BinaryString =
    row.getBinaryString(column)
  }

  case class InternalDecimalConverter(dt: DecimalTypeInfo) extends IdentityConverter[Decimal] {
    override def toExternalImpl(row: BaseRow, column: Int): Decimal =
      row.getDecimal(column, dt.precision(), dt.scale())
  }

  case class DecimalConverter(dt: DecimalType)
    extends InternalTypeConverter[Any, JBigDecimal, Decimal] {
    override def toInternalImpl(scalaValue: Any): Decimal = scalaValue match {
      case bd: JBigDecimal => Decimal.fromBigDecimal(bd, dt.precision(), dt.scale())
      case sd: Decimal => sd
    }
    override def toExternalImpl(internalValue: Decimal): JBigDecimal =
      internalValue.toBigDecimal
    override def toExternalImpl(row: BaseRow, column: Int): JBigDecimal =
      row.getDecimal(column, dt.precision(), dt.scale()).toBigDecimal
  }

  case class GenericConverter(t: GenericType[_]) extends IdentityConverter[Any] {
    override def toExternalImpl(row: BaseRow, column: Int): Any = row.getGeneric(column, t)
  }

  def createToInternalConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      identity
    } else {
      getConverterForType(dataType).toInternal
    }
  }

  /**
   * Creates a converter function that will convert internal types to Java/Scala type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  def createToExternalConverter(t: DataType): Any => Any = {
    if (isPrimitive(t)) {
      identity
    } else {
      getConverterForType(t).toExternal
    }
  }

  private def genConvertField(ctx: CodeGeneratorContext, converter: Any => Any): String = {
    val convertField = CodeGenUtils.newName("converter")
    val convertTypeTerm = classOf[(_) => _].getCanonicalName

    val initCode = if (ctx.supportReference) {
      s"$convertField = ${ctx.addReferenceObj(converter)};"
    } else {
      val byteArray = InstantiationUtil.serializeObject(converter)
      val serializedData = Base64.encodeBase64URLSafeString(byteArray)
      s"""
         | $convertField = ($convertTypeTerm)
         |      ${classOf[InstantiationUtil].getCanonicalName}.deserializeObject(
         |         ${classOf[Base64].getCanonicalName}.decodeBase64("$serializedData"),
         |         Thread.currentThread().getContextClassLoader());
           """.stripMargin
    }

    ctx.addReusableMember(s"$convertTypeTerm $convertField = null;", initCode)
    convertField
  }

  def genToExternal(ctx: CodeGeneratorContext, t: DataType, term: String): String = {
    val iTerm = boxedTypeTermForType(DataTypes.internal(t))
    val eTerm = externalBoxedTermForType(t)
    if (isIdentity(t)) {
      s"($eTerm) $term"
    } else {
      val scalarFuncTerm = classOf[BuildInScalarFunctions].getCanonicalName
      DataTypes.to(t) match {
        case Types.STRING => s"$BINARY_STRING.safeToString(($iTerm) $term)"
        case Types.SQL_DATE => s"$scalarFuncTerm.safeInternalToDate(($iTerm) $term)"
        case Types.SQL_TIME => s"$scalarFuncTerm.safeInternalToTime(($iTerm) $term)"
        case Types.SQL_TIMESTAMP => s"$scalarFuncTerm.safeInternalToTimestamp(($iTerm) $term)"
        case _ =>
          s"($eTerm) ${genConvertField(ctx, createToExternalConverter(t))}.apply($term)"
      }
    }
  }

  def genToInternal(ctx: CodeGeneratorContext, t: DataType, term: String): String =
    genToInternal(ctx, t)(term)

  def genToInternal(ctx: CodeGeneratorContext, t: DataType): String => String = {
    val iTerm = boxedTypeTermForType(DataTypes.internal(t))
    val eTerm = externalBoxedTermForType(t)
    if (isIdentity(t)) {
      term => s"($iTerm) $term"
    } else {
      val scalarFuncTerm = classOf[BuildInScalarFunctions].getCanonicalName
      DataTypes.to(t) match {
        case Types.STRING => term => s"$BINARY_STRING.fromString($term)"
        case Types.SQL_DATE | Types.SQL_TIME =>
          term => s"$scalarFuncTerm.safeToInt(($eTerm) $term)"
        case Types.SQL_TIMESTAMP => term => s"$scalarFuncTerm.safeToLong(($eTerm) $term)"
        case _ =>
          val converter = genConvertField(ctx, createToInternalConverter(t))
          term => s"($iTerm) $converter.apply($term)"
      }
    }
  }

  private def isIdentity(t: DataType): Boolean = {
    isPrimitive(t) || getConverterForType(t).isInstanceOf[IdentityConverter[_]]
  }

  def genToExternalIfNeeded(
      ctx: CodeGeneratorContext,
      t: DataType,
      clazz: Class[_],
      term: String): String = {
    if (isInternalClass(clazz, DataTypes.internal(t))) {
      s"(${boxedTypeTermForType(DataTypes.internal(t))}) $term"
    } else {
      genToExternal(ctx, t, term)
    }
  }

  def genToInternalIfNeeded(
      ctx: CodeGeneratorContext,
      t: DataType,
      clazz: Class[_],
      term: String): String = {
    if (isInternalClass(clazz, DataTypes.internal(t))) {
      s"(${boxedTypeTermForType(DataTypes.internal(t))}) $term"
    } else {
      genToInternal(ctx, t, term)
    }
  }
}
