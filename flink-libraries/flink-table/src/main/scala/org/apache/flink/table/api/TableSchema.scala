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
package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, PojoTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.sources._
import org.apache.flink.table.types.{DataTypes, _}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeCheckUtils, TypeUtils}
import org.apache.flink.types.Row

import _root_.scala.collection.mutable.ArrayBuffer

/**
 * A TableSchema represents a Table's structure.
 */
class TableSchema(private val columns: Array[Column]) {

  def this(names: Array[String], types: Array[InternalType], nullables: Array[Boolean]) = {
    this {
      if (names.length != types.length) {
        throw TableException(
          s"Number of column indexes and column names must be equal." +
              s"\nColumn names count is [${names.length}]" +
              s"\nColumn types count is [${types.length}]" +
              s"\nColumn names:${names.mkString("[ ", ", ", " ]")}" +
              s"\nColumn types:${types.mkString("[ ", ", ", " ]")}")
      }

      if (names.length != nullables.length) {
        throw TableException(
          s"Number of column indexes and column names must be equal." +
              s"\nColumn names count is [${names.length}]" +
              s"\nColumn types count is [${nullables.length}]"
        )
      }

      // check uniqueness of field names
      if (names.toSet.size != types.length) {
        val columnNameBuffer = names.toBuffer
        val duplicate = names.filter(
          name => columnNameBuffer.-=(name).contains(name))

        throw TableException(
          s"Table column names must be unique." +
              s"\nThe duplicate columns are: ${duplicate.mkString("[ ", ", ", " ]")}" +
              s"\nAll column names: ${names.mkString("[ ", ", ", " ]")}")
      }

      names.zip(types).zipWithIndex.map {
        case ((name, typeInfo), index) =>
          Column(name, index, typeInfo, nullables(index))
      }
    }
  }

  def this(names: Array[String], types: Array[InternalType]) = {
    this(names, types, types.map(
      {
        case typ: ExternalType => typ.toInternalType
        case typ: InternalType => typ
      }).map(
      typ =>
        typ!= TimestampType.ROWTIME_INDICATOR &&
            typ != TimestampType.PROCTIME_INDICATOR))
  }

  val columnNameToIndex: Map[String, Int] = columns.zipWithIndex.map {
    case (column, index) => (column.name, index)
  }.toMap

  def getColumns: Array[Column] = columns

  def getPrimaryKeys: Array[Column] = columns.filter(_.isPrimaryKey)

  def getPhysicalIndices: Array[Int] = columns.map(_.index)

  def getNullables: Array[Boolean] = columns.map(_.isNullable)

  def getTypes: Array[InternalType] = columns.map(_.internalType)

  def getColumnNames: Array[String] = columns.map(_.name)

  /**
   * Returns the specified column for the given column index.
   *
   * @param columnIndex the index of the field
   */
  def getColumn(columnIndex: Int): Column = {
    require(columnIndex >= 0 && columnIndex < columns.length)
    columns(columnIndex)
  }

  /**
   * Returns the specified type information for the given column index.
   *
   * @param columnIndex the index of the field
   */
  def getType(columnIndex: Int): InternalType = {
    require(columnIndex >= 0 && columnIndex < columns.length)
    columns(columnIndex).internalType
  }

  /**
   * Returns the specified type information for the given column name.
   *
   * @param columnName the name of the field
   */
  def getType(columnName: String): Option[InternalType] = {
    if (columnNameToIndex.contains(columnName)) {
      Some(columns(columnNameToIndex(columnName)).internalType)
    } else {
      None
    }
  }

  /**
   * Returns the specified column name for the given column index.
   *
   * @param columnIndex the index of the field
   */
  def getColumnName(columnIndex: Int): String = {
    require(columnIndex >= 0 && columnIndex < columns.length)
    columns(columnIndex).name
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    columns.foreach {
      case column: Column =>
        builder.append(s" |--    name: ${column.name}\n")
        builder.append(s"    |-- type: ${column.internalType}\n")
        builder.append(s"    |-- physicalIndex: ${column.index}\n")
        builder.append(s"    |-- isNullable: ${column.isNullable}\n")
        builder.append(s"    |-- isPrimaryKey: ${column.isPrimaryKey}\n")
    }
    builder.toString()
  }

}

/**
 * Table Columns.
 */
case class Column(
    name: String, // column name
    index: Int, // index in physical input
    internalType: InternalType, // physical type of column
    isNullable: Boolean = true, // is nullable
    isPrimaryKey: Boolean = false // is part of primary key
)


object TableSchema {

  /**
   * Creates a table schema based on type information.
   */
  def fromTypeInfo(
      typeInfo: TypeInformation[_],
      fieldNullables: Option[Array[Boolean]] = None): TableSchema = {
    fromDataType(DataTypes.of(typeInfo), fieldNullables)
  }

  /**
   * Create table schema from a data type.
   */
  def fromDataType(
      dataType: DataType,
      fieldNullables: Option[Array[Boolean]] = None): TableSchema = {
    DataTypes.internal(dataType) match {
      case bt: BaseRowType =>
        val fieldNames = bt.getFieldNames
        val fieldTypes = bt.getFieldTypes
        if (fieldNullables.isDefined) {
          new TableSchema(fieldNames, fieldTypes, fieldNullables.get)
        } else {
          new TableSchema(fieldNames, fieldTypes)
        }
      case t =>
        val fieldNames = Array("f0")
        val fieldTypes = Array(t)
        if (fieldNullables.isDefined) {
          new TableSchema(fieldNames, fieldTypes, fieldNullables.get)
        } else {
          new TableSchema(fieldNames, fieldTypes)
        }
    }
  }

  /**
   * Create table schema from a data type.
   */
  def fromTypeInfo(
      dataType: DataType,
      fieldNullables: Option[Array[Boolean]]): TableSchema = {
    fromTypeInfo(TypeUtils.createTypeInfoFromDataType(dataType), fieldNullables)
  }

  /**
   * Create table schema for a table source.
   */
  def fromTableSource(tableSource: TableSource): TableSchema = {

    var result = fromDataType(tableSource.getReturnType, None)

    result = tableSource match {
      case source: DefinedFieldNames =>
        val fieldIndices = source.getFieldIndices
        val fieldNames = source.getFieldNames
        if (fieldIndices.length != fieldNames.length) {
          throw TableException("The length of field name and field indices should be the same. " +
              s"\ncurrent field names: ${fieldNames.mkString("[", ", ", "]")}" +
              s"\ncurrent field indices: ${fieldIndices.mkString("[", ", ", "]")}")
        }
        val columns = result.columns
        val newColumns = fieldIndices.zipWithIndex.map {
          case (index, configIndex) =>
            if (index < 0 || index >= columns.length) {
              throw TableException(s"Selected index is invalid, expected index is $index" +
                  s"\nbut ${columns.length} columns given")
            }
            val oldColumn = columns(index)
            Column(
              fieldNames(configIndex),
              oldColumn.index,
              oldColumn.internalType,
              oldColumn.isNullable,
              oldColumn.isPrimaryKey)
        }
        new TableSchema(newColumns.asInstanceOf[Array[Column]])
      case _ => result
    }

    result = tableSource match {
      case source: DefinedRowtimeAttribute if source.getRowtimeAttribute != null =>
        val columns = result.columns
        var transFormed = false
        val newColumns = columns.map {
          case column: Column if column.name.equals(source.getRowtimeAttribute) =>
            transFormed = true
            column.internalType match {
              case TimestampType.ROWTIME_INDICATOR =>
                column
              case DataTypes.LONG =>
                Column(
                  column.name, column.index, TimestampType.ROWTIME_INDICATOR, false)
              case TimestampType.TIMESTAMP =>
                Column(
                  column.name, column.index, TimestampType.ROWTIME_INDICATOR, false)
              case _ =>
                throw TableException(
                  s"The rowtime attribute can only replace a field with a valid time type, " +
                      s"such as Timestamp or Long. But was: ${column.internalType}")
            }
          case column: Column => column
        }
        if (!transFormed) {
          throw TableException(
            s"The rowtime attribute not Found. " +
                s"\n${
                  tableSource.asInstanceOf[DefinedRowtimeAttribute].getRowtimeAttribute
                } expected," +
                s"\n but ${result.getColumnNames.mkString("[", ", ", "]")} found.")
        }
        new TableSchema(newColumns.asInstanceOf[Array[Column]])
      case _ => result
    }

    result = tableSource match {
      case source: DefinedProctimeAttribute if source.getProctimeAttribute != null =>
        if (source.getProctimeAttribute.trim.equals("")) {
          throw TableException("The name of the proctime attribute must not be empty.")
        }
        val columns = result.columns
        var transFormed = false
        var newColumns = columns.map {
          case column: Column if column.name.equals(source.getProctimeAttribute) =>
            transFormed = true
            column.internalType match {
              case TimestampType.PROCTIME_INDICATOR =>
                column
              case _ =>
                throw TableException(s"The proctime attribute already exited in table schema, " +
                    s"but type is ${column.internalType}")
            }
          case column: Column => column
        }
        if (!transFormed) {
          newColumns = newColumns :+ Column(
            tableSource.asInstanceOf[DefinedProctimeAttribute].getProctimeAttribute,
            DataTypes.PROCTIME_MARKER,
            TimestampType.PROCTIME_INDICATOR,
            false)
        }
        new TableSchema(newColumns)
      case _ => result
    }

    result = tableSource match {
      case source: DefinedFieldNullables =>
        val columns = result.columns
        val nullables = source.getFieldNullables
        if (columns.length != nullables.length) {
          throw TableException(
            "the length of nullables is not equals to the length of table schema")
        }
        val newColumns = columns.zip(nullables).map {
          case (column, nullable) =>
            Column(
              column.name,
              column.index,
              column.internalType,
              nullable && ! FlinkTypeFactory.isTimeIndicatorType(column.internalType),
              column.isPrimaryKey)
        }
        new TableSchema(newColumns.asInstanceOf[Array[Column]])
      case _ => result
    }

    // return new generated table schema
    result
  }

  /**
   * Injects field information from external configuration.
   */
  def fromTypeInfo(
      typeInfo: TypeInformation[_],
      fieldIndexes: Array[Int],
      fieldNames: Array[String]): TableSchema = {
    fromTypeInfo(typeInfo, fieldIndexes, fieldNames, None)
  }

  /**
   * Injects field information from external configuration.
   */
  def fromTypeInfo(
      typeInfo: TypeInformation[_],
      fieldIndexes: Array[Int],
      fieldNames: Array[String],
      fieldNullables: Option[Array[Boolean]]): TableSchema = {

    val origin = TableSchema.fromTypeInfo(typeInfo)

    if (fieldIndexes.length != fieldNames.length) {
      throw TableException(
        s"Number of field indexes and field names must be equal. "
            + s"\nField names count is [${fieldNames.length}]"
            + s"\nField indexs count is [${fieldIndexes.length}]"
            + s"\nField names: ${fieldNames.mkString("[ ", ", ", " ]")}"
            + s"\nField indexs: ${fieldIndexes.mkString("[ ", ", ", " ]")}")
    }

    if (fieldNullables.isDefined && fieldIndexes.length != fieldNullables.get.length) {
      throw TableException(
        s"Number of field indexes and field nullables must be equal. "
            + s"\nField nullables count is [${fieldNullables.get.length}]"
            + s"\nField indexs count is [${fieldIndexes.length}]"
            + s"\nField nullables: ${fieldNullables.mkString("[ ", ", ", " ]")}"
            + s"\nField indexs: ${fieldIndexes.mkString("[ ", ", ", " ]")}")
    }

    // check uniqueness of field names
    if (fieldNames.length != fieldNames.toSet.size) {
      val fieldNameBuffer = fieldNames.toBuffer
      val duplicate = fieldNames.filter(name => fieldNameBuffer.-=(name).contains(name))
      throw TableException(
        s"Table field names must be unique."
            + s"\nThe duplicate fields are: ${duplicate.mkString("[ ", ", ", " ]")}"
            + s"\nAll field names: ${fieldNames.mkString("[ ", ", ", " ]")}")
    }

    val selectedIndices = fieldIndexes.filter {
      case DataTypes.ROWTIME_MARKER => false
      case DataTypes.PROCTIME_MARKER => false
      case _ => true
    }
    if (selectedIndices.length > origin.columns.length) {
      throw TableException(
        s"Too many as fields."
            + s"\n The fields expected are ${selectedIndices.mkString("[ ", ", ", "]")}"
            + s"\n All fields size are ${origin.columns.length}"
      )
    }

    val newColumns = fieldIndexes.zipWithIndex.map {
      case (DataTypes.ROWTIME_MARKER, index) =>
        Column(
          fieldNames(index),
          DataTypes.ROWTIME_MARKER,
          TimestampType.ROWTIME_INDICATOR,
          false)
      case (DataTypes.PROCTIME_MARKER, index) =>
        Column(
          fieldNames(index),
          DataTypes.PROCTIME_MARKER,
          TimestampType.PROCTIME_INDICATOR,
          false)
      case (columnIndex, nameIndex) =>
        val originColumn = origin.getColumn(columnIndex)
        Column(
          fieldNames(nameIndex),
          originColumn.index,
          originColumn.internalType,
          originColumn.isNullable,
          originColumn.isPrimaryKey)
    }

    new TableSchema(newColumns.asInstanceOf[Array[Column]])
  }

  /**
   * Create TableSchema with field expression.
   */
  def fromTypeInfo(
      typeInfo: TypeInformation[_],
      exprs: Array[_ <: Expression]): TableSchema = {
    fromTypeInfo(typeInfo, exprs, None)
  }

  /**
   * Create TableSchema with field expression.
   */
  def fromTypeInfo(
      typeInfo: TypeInformation[_],
      exprs: Array[_ <: Expression],
      fieldNullables: Option[Array[Boolean]]): TableSchema = {

    val (fieldNames, fieldIndices) = getFieldInfo(typeInfo, exprs)
    val (rowtime, proctime) = validateAndExtractTimeAttributes(typeInfo, exprs)

    fromTypeInfo(
      typeInfo,
      adjustFieldIndexes(fieldIndices, rowtime, proctime),
      adjustFieldNames(fieldNames, rowtime, proctime),
      Some(adjustFieldNullables(
        fieldNullables.getOrElse(fieldNames.map(_ => true)),
        rowtime,
        proctime)))
  }

  /**
   * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
   * [[Expression]]. It does not handle time attributes but considers them in indices.
   *
   * @param inputType The [[TypeInformation]] against which the [[Expression]]s are evaluated.
   * @param exprs     The expressions that define the field names.
   * @tparam A The type of the TypeInformation.
   * @return A tuple of two arrays holding the field names and corresponding field positions.
   */
  protected[flink] def getFieldInfo[A](
      inputType: TypeInformation[A],
      exprs: Array[_ <: Expression]): (Array[String], Array[Int]) = {

    TableEnvironment.validateType(DataTypes.of(inputType))

    val indexedNames: Array[(Int, String)] = inputType match {
      case g: GenericTypeInfo[A] if g.getTypeClass == classOf[Row] =>
        throw new TableException(
          "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
              "Please specify the type of the input with a RowTypeInfo.")
      case a: AtomicType[_] =>
        exprs.zipWithIndex flatMap {
          case (_: TimeAttribute, _) =>
            None
          case (UnresolvedFieldReference(name), idx) if idx > 0 =>
            // only accept the first field for an atomic type
            throw new TableException("Only the first field can reference an atomic type.")
          case (UnresolvedFieldReference(name), idx) =>
            // first field reference is mapped to atomic type
            Some((0, name))
          case _ => throw new TableException("Field reference expression requested.")
        }
      case t: TupleTypeInfo[A] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = t.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $t")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case c: CaseClassTypeInfo[A] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = c.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $c")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case p: PojoTypeInfo[A] =>
        exprs flatMap {
          case (UnresolvedFieldReference(name)) =>
            val idx = p.getFieldIndex(name)
            if (idx < 0) {
              throw new TableException(s"$name is not a field of type $p")
            }
            Some((idx, name))
          case Alias(UnresolvedFieldReference(origName), name, _) =>
            val idx = p.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $p")
            }
            Some((idx, name))
          case _: TimeAttribute =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case r: RowTypeInfo =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = r.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $r")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case r: BaseRowTypeInfo[_] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = r.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $r")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case tpe => throw new TableException(
        s"Source of type $tpe cannot be converted into Table.")
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames.toArray, fieldIndexes.toArray) // build fails in Scala 2.10 if not converted
  }

  /**
   * Checks for at most one rowtime and proctime attribute.
   * Returns the time attributes.
   *
   * @return rowtime attribute and proctime attribute
   */
  private def validateAndExtractTimeAttributes(
      streamType: TypeInformation[_],
      exprs: Array[_ <: Expression])
  : (Option[(Int, String)], Option[(Int, String)]) = {

    val fieldTypes: Array[TypeInformation[_]] = streamType match {
      case c: CompositeType[_] => (0 until c.getArity).map(i => c.getTypeAt(i)).toArray
      case a: AtomicType[_] => Array(a)
    }

    var fieldNames: List[String] = Nil
    var rowtime: Option[(Int, String)] = None
    var proctime: Option[(Int, String)] = None

    def extractRowtime(idx: Int, name: String, origName: Option[String]): Unit = {
      if (rowtime.isDefined) {
        throw new TableException(
          "The rowtime attribute:[" + rowtime.get._2 + "] can only be defined or referenced once" +
            " in a table schema.")
      } else {
        val mappedIdx = streamType match {
          case pti: PojoTypeInfo[_] =>
            pti.getFieldIndex(origName.getOrElse(name))
          case _ => idx;
        }
        // check type of field that is replaced
        if (mappedIdx < 0) {
          throw new TableException(
            s"The rowtime attribute can only replace a valid field. " +
                s"${origName.getOrElse(name)} is not a field of type $streamType.")
        }
        else if (mappedIdx < fieldTypes.length &&
            !(TypeCheckUtils.isLong(DataTypes.internal(fieldTypes(mappedIdx))) ||
              TypeCheckUtils.isRowTime(DataTypes.internal(fieldTypes(mappedIdx))))) {
          throw new TableException(
            s"The rowtime attribute can only replace a field with a valid time type, " +
                s"such as Timestamp or Long. But was: ${fieldTypes(mappedIdx)}")
        }

        rowtime = Some(idx, name)
      }
    }

    def extractProctime(idx: Int, name: String): Unit = {
      if (proctime.isDefined) {
        throw new TableException(
          "The proctime attribute can only be defined once in a table schema.")
      } else {
        // check that proctime is only appended
        if (idx < fieldTypes.length
            && !TypeCheckUtils.isProcTime(DataTypes.internal(fieldTypes(idx)))) {
          throw new TableException(
            "The proctime attribute can only be appended to the table schema and not replace " +
                "an existing field. Please move it to the end of the schema.")
        }
        proctime = Some(idx, name)
      }
    }

    exprs.zipWithIndex.foreach {
      case (RowtimeAttribute(UnresolvedFieldReference(name)), idx) =>
        extractRowtime(idx, name, None)

      case (RowtimeAttribute(Alias(UnresolvedFieldReference(origName), name, _)), idx) =>
        extractRowtime(idx, name, Some(origName))

      case (ProctimeAttribute(UnresolvedFieldReference(name)), idx) =>
        extractProctime(idx, name)

      case (ProctimeAttribute(Alias(UnresolvedFieldReference(_), name, _)), idx) =>
        extractProctime(idx, name)

      case (UnresolvedFieldReference(name), _) => fieldNames = name :: fieldNames

      case (Alias(UnresolvedFieldReference(_), name, _), _) => fieldNames = name :: fieldNames

      case (e, _) =>
        throw new TableException(s"Time attributes can only be defined on field references or " +
            s"aliases of field references. But was: $e")
    }

    if (rowtime.isDefined && fieldNames.contains(rowtime.get._2)) {
      throw new TableException(
        "The rowtime attribute may not have the same name as an another field.")
    }

    if (proctime.isDefined && fieldNames.contains(proctime.get._2)) {
      throw new TableException(
        "The proctime attribute may not have the same name as an another field.")
    }

    (rowtime, proctime)
  }

  /**
   * Injects markers for time indicator fields into the field indexes.
   *
   * @param fieldIndexes The field indexes into which the time indicators markers are injected.
   * @param rowtime      An optional rowtime indicator
   * @param proctime     An optional proctime indicator
   * @return An adjusted array of field indexes.
   */
  private def adjustFieldIndexes(
      fieldIndexes: Array[Int],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[Int] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) => fieldIndexes.patch(rt._1, Seq(DataTypes.ROWTIME_MARKER), 0)
      case _ => fieldIndexes
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) => withRowtime.patch(pt._1, Seq(DataTypes.PROCTIME_MARKER), 0)
      case _ => withRowtime
    }

    withProctime
  }

  /**
   * Injects names of time indicator fields into the list of field names.
   *
   * @param fieldNames The array of field names into which the time indicator field names are
   *                   injected.
   * @param rowtime    An optional rowtime indicator
   * @param proctime   An optional proctime indicator
   * @return An adjusted array of field names.
   */
  private def adjustFieldNames(
      fieldNames: Array[String],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[String] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) => fieldNames.patch(rt._1, Seq(rowtime.get._2), 0)
      case _ => fieldNames
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) => withRowtime.patch(pt._1, Seq(proctime.get._2), 0)
      case _ => withRowtime
    }

    withProctime
  }

  /**
   * Injects nullable setting of time indicator fields into the list of field nullables.
   *
   * @param fieldNullables The array of field nullables into which the time indicator
   *                       field names are injected.
   * @param rowtime        An optional rowtime indicator
   * @param proctime       An optional proctime indicator
   * @return An adjusted array of field names.
   */
  private def adjustFieldNullables(
      fieldNullables: Array[Boolean],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[Boolean] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) => fieldNullables.patch(rt._1, Seq(false), 0)
      case _ => fieldNullables
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) => withRowtime.patch(pt._1, Seq(false), 0)
      case _ => withRowtime
    }
    withProctime
  }

  def builder(): TableSchemaBuilder = {
    new TableSchemaBuilder
  }

}

class TableSchemaBuilder {

  private val fieldNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  private val fieldTypes: ArrayBuffer[InternalType] = new ArrayBuffer[InternalType]()
  private val columns: ArrayBuffer[Column] = new ArrayBuffer[Column]()

  def field(name: String, tpe: InternalType): TableSchemaBuilder = {
    fieldNames.append(name)
    fieldTypes.append(tpe)
    this
  }

  def build(): TableSchema = {
    fieldNames.zipWithIndex foreach {
      case(name, idx) =>
        columns.append(Column.apply(name, idx, fieldTypes(idx)))
    }

    new TableSchema(columns.toArray)
  }
}
