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

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.api.types.{DataTypes, _}

import _root_.scala.collection.mutable.ArrayBuffer

/**
 * A TableSchema represents a Table's structure.
 */
class TableSchema(
    private val columns: Array[Column],
    private val primaryKey: Array[String],
    private val uniqueKeys: Array[Array[String]],
    private val computedColumns: Array[ComputedColumn],
    private val watermarks: Array[Watermark]) {

  val columnNameToIndex: Map[String, Int] = columns.zipWithIndex.map {
    case (column, index) => (column.name, index)
  }.toMap

  primaryKey.foreach {
    case field: String if columnNameToIndex.contains(field) =>
    case field: String => new TableException(s"Primary key field: $field not found in table schema")
  }

  uniqueKeys.foreach {
    case uniqueKey: Array[String] if uniqueKey.isEmpty =>
      new TableException("Unique key should not be empty.")
    case uniqueKey: Array[String] => uniqueKey.foreach {
      case field: String if columnNameToIndex.contains(field) =>
      case field: String =>
        new TableException(s"Unique key field: $field not found in table schema")
    }
  }

  def this(columns: Array[Column]) = {
    this(
      columns,
      new Array[String](0),
      new Array[Array[String]](0),
      new Array[ComputedColumn](0),
      new Array[Watermark](0))
  }

  def this(names: Array[String], types: Array[InternalType], nullables: Array[Boolean]) = {
    this {
      if (names.length != types.length) {
        throw new TableException(
          s"Number of column indexes and column names must be equal." +
              s"\nColumn names count is [${names.length}]" +
              s"\nColumn types count is [${types.length}]" +
              s"\nColumn names:${names.mkString("[ ", ", ", " ]")}" +
              s"\nColumn types:${types.mkString("[ ", ", ", " ]")}")
      }

      if (names.length != nullables.length) {
        throw new TableException(
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

        throw new TableException(
          s"Table column names must be unique." +
              s"\nThe duplicate columns are: ${duplicate.mkString("[ ", ", ", " ]")}" +
              s"\nAll column names: ${names.mkString("[ ", ", ", " ]")}")
      }

      names.zip(types).zipWithIndex.map {
        case ((name, typeInfo), index) =>
          Column(name, typeInfo, nullables(index))
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

  def getColumns: Array[Column] = columns

  def getPrimaryKeys: Array[String] = primaryKey

  def getUniqueKeys: Array[Array[String]] = uniqueKeys

  def getNullables: Array[Boolean] = columns.map(_.isNullable)

  def getTypes: Array[InternalType] = columns.map(_.internalType)

  def getColumnNames: Array[String] = columns.map(_.name)

  def getComputedColumns: Array[ComputedColumn] = computedColumns

  def getWatermarks: Array[Watermark] = watermarks

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

  /**
    * Converts a table schema into a schema that represents the result that would be written
    * into a table sink or operator outside of the Table & SQL API. Time attributes are replaced
    * by proper TIMESTAMP data types.
    *
    * @return a table schema with no time attributes
    */
  def withoutTimeAttributes: TableSchema = {
    val converted = columns.map { t =>
      if (FlinkTypeFactory.isTimeIndicatorType(t.internalType)) {
        Column(t.name, TimestampType.TIMESTAMP, false)
      } else {
        t
      }
    }
    new TableSchema(converted)
  }

  /**
    * Converts a table schema into a (nested) type information describing a Row.
    *
    * @return type information where columns are fields of a row
    */
  def toRowType: DataType = {
    DataTypes.createRowType(getTypes, getColumnNames)
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    columns.foreach {
      column: Column =>
        builder.append(s" |--    name: ${column.name}\n")
        builder.append(s"    |-- type: ${column.internalType}\n")
        builder.append(s"    |-- isNullable: ${column.isNullable}\n")
    }
    if (!primaryKey.isEmpty) {
      builder.append(s"primary key\n")
      builder.append(s" |--  ${primaryKey.mkString(",")}\n")
    }
    if (!uniqueKeys.isEmpty) {
      builder.append(s"unique keys\n")
          uniqueKeys.foreach {
            case uniqueKey: Array[String] =>
              builder.append(s" |--  ${uniqueKey.mkString(",")}\n")
          }
    }
    builder.toString()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case t:TableSchema => toString.equals(t.toString)
      case _ => false
    }
  }

}

/**
 * Table Columns.
 */
case class Column(
    name: String, // column name
    internalType: InternalType, // physical type of column
    isNullable: Boolean = true  // is nullable
)

case class ComputedColumn(name: String, expression: String)

case class Watermark(name: String, eventTime: String, offset: Long)

object TableSchema {

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

  def builder(): TableSchemaBuilder = {
    new TableSchemaBuilder
  }
}

class TableSchemaBuilder {

  private val columns: ArrayBuffer[Column] = new ArrayBuffer[Column]()
  private val primaryKey: ArrayBuffer[String] = new ArrayBuffer[String]()
  private val uniqueKeys: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
  private val computedColumns: ArrayBuffer[ComputedColumn] = new ArrayBuffer[ComputedColumn]()
  private val watermarks: ArrayBuffer[Watermark] = new ArrayBuffer[Watermark]()

  def fromDataType(dataType: DataType): TableSchemaBuilder = {
    columns.append(TableSchema.fromDataType(dataType).getColumns:_*)
    this
  }

  @deprecated
  def field(name: String, tpe: DataType): TableSchemaBuilder = {
    columns.append(Column(name, DataTypes.internal(tpe)))
    this
  }

  @deprecated
  def field(name: String, tpe: DataType, nullable: Boolean): TableSchemaBuilder = {
    columns.append(Column(name, DataTypes.internal(tpe), nullable))
    this
  }

  def column(name: String, tpe: DataType): TableSchemaBuilder = {
    columns.append(Column(name, DataTypes.internal(tpe)))
    this
  }

  def column(name: String, tpe: DataType, nullable: Boolean): TableSchemaBuilder = {
    columns.append(Column(name, DataTypes.internal(tpe), nullable))
    this
  }

  def computedColumn(name: String, expression: String): TableSchemaBuilder = {
    computedColumns.append(ComputedColumn(name, expression))
    this
  }

  def watermark(name: String, from: String, offset: Long): TableSchemaBuilder = {
    watermarks.append(Watermark(name, from, offset))
    this
  }

  def primaryKey(field: String*): TableSchemaBuilder = {
    field.foreach(f => primaryKey.append(f))
    this
  }

  def uniqueKey(field: String*): TableSchemaBuilder = {
    val newUniqueKey = new ArrayBuffer[String]()
    field.foreach(f => newUniqueKey.append(f))
    uniqueKeys.append(newUniqueKey)
    this
  }

  def build(): TableSchema = {
    new TableSchema(
      columns.toArray,
      primaryKey.toArray,
      uniqueKeys.map(_.toArray).toArray,
      computedColumns.toArray,
      watermarks.toArray
    )
  }
}
