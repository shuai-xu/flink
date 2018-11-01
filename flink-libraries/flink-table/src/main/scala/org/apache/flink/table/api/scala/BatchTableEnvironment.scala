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
package org.apache.flink.table.api.scala

import org.apache.flink.annotation.VisibleForTesting

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.scala.getCallLocationName
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.dataformat.BoxedWrapperRow
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.table.sources.RangeInputFormat
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.typeutils.BaseRowTypeInfo

/**
  * The [[TableEnvironment]] for a Scala [[StreamExecutionEnvironment]].
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Scala [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config  The configuration of the TableEnvironment.
  */
class BatchTableEnvironment(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.BatchTableEnvironment(
    execEnv.getWrappedStreamExecutionEnvironment,
    config) {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](boundedStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream.javaStream)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean]): Table = {

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream.javaStream, fieldNullables)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: BoundedStream[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromBoundedStream(stream, 'a, 'b)
    * }}}
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fields         The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](boundedStream: DataStream[T], fields: Expression*): Table = {

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream.javaStream, fields.toArray)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val fieldNullables: Array[Boolean] = ...
    *   val tab: Table = tableEnv.fromBoundedStream(stream, fieldNullables, 'a, 'b)
    * }}}
    *
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @param fields         The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean],
      fields: Expression*): Table = {

    val name = createUniqueTableName()
    registerBoundedStreamInternal(name, boundedStream.javaStream, fields.toArray, fieldNullables)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromBoundedStream(stream, 'a, 'b)
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fields         The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](name: String, boundedStream: DataStream[T], fields: Expression*):
  Table = {
    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream.javaStream, fields.toArray)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val fieldNullables: Array[Boolean] = ...
    *   val tab: Table = tableEnv.fromBoundedStream(stream, fieldNullables, 'a, 'b)
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to be converted.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @param fields         The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean],
      fields: Expression*):
  Table = {
    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream.javaStream, fields.toArray, fieldNullables)
    scan(name)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](name: String, boundedStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream.javaStream)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean]): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream.javaStream, fieldNullables)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataStream[(String, Long)] = ...
    *   tableEnv.registerBoundedStream("myTable", set, 'a, 'b)
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fields         The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fields: Expression*): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream.javaStream, fields.toArray)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: BoundedStream[(String, Long)] = ...
    *   val fieldNullables: Array[Boolean] = ...
    *   tableEnv.registerBoundedStream("myTable", set, fieldNullables, 'a, 'b)
    * }}}
    *
    * @param name           The name under which the [[DataStream]] is registered in the
    *                       catalog.
    * @param boundedStream The [[DataStream]] to register.
    * @param fieldNullables The field isNullables attributes of boundedStream.
    * @param fields         The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerBoundedStream[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean],
      fields: Expression*): Unit = {

    checkValidTableName(name)
    registerBoundedStreamInternal(name, boundedStream.javaStream, fields.toArray, fieldNullables)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param fields field names expressions, eg: 'a, 'b, 'c
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T : ClassTag : TypeInformation](
    tableName: String, data: Iterable[T], fields: Expression*): Unit = {
    val typeInfo = implicitly[TypeInformation[T]]
    val fieldArray = if (fields == null || fields.isEmpty) null else fields.toArray
    registerCollection(tableName, data, typeInfo, null, fieldArray)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param fieldNullables The field isNullables attributes of data.
    * @param fields field names, eg: 'a, 'b, 'c
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T : ClassTag : TypeInformation](tableName: String, data: Iterable[T],
    fieldNullables: Iterable[Boolean], fields: Expression*): Unit = {
    val typeInfo = implicitly[TypeInformation[T]]
    val fieldArray = if (fields == null || fields.isEmpty) null else fields.toArray
    registerCollection(tableName, data, typeInfo, null, fieldArray)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param information information of [[Iterable]].
    * @param fields field names expressions, eg: 'a, 'b, 'c
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](tableName: String, data: Iterable[T], information: TypeInformation[T],
    fields: Expression*): Unit = {
    val fieldArray = if (fields == null || fields.isEmpty) null else fields.toArray
    registerCollection(tableName, data, information, null, fieldArray)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param information information of [[Iterable]].
    * @param fieldNullables The field isNullables attributes of data.
    * @param fields field name expressions, eg: 'a, 'b, 'c
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  def registerCollection[T](
    tableName: String,
    data: Iterable[T],
    information: TypeInformation[T],
    fieldNullables: Iterable[Boolean],
    fields: Expression*): Unit = {
    val fieldArray = if (fields == null || fields.isEmpty) null else fields.toArray
    registerCollection(tableName, data, information, fieldNullables, fieldArray)
  }

  /**
    * Registers the given [[Iterable]] as table in the
    * [[TableEnvironment]]'s catalog.
    *
    * @param tableName name of table.
    * @param data The [[Iterable]] to be converted.
    * @param typeInfo information of [[Iterable]].
    * @param fieldNullables The field isNullables attributes of data.
    * @param fields field name expressions, eg: 'a, 'b, 'c
    * @tparam T The type of the [[Iterable]].
    * @return The converted [[Table]].
    */
  @VisibleForTesting
  private [table] def registerCollection[T](
    tableName: String,
    data: Iterable[T],
    typeInfo: TypeInformation[T],
    fieldNullables: Iterable[Boolean],
    fields: Array[Expression]): Unit = {
    val boundedStream = streamEnv.createInput(new CollectionInputFormat[T](
      data.asJavaCollection,
      typeInfo.createSerializer(execEnv.getConfig)),
      typeInfo, tableName)
    (fields == null, fieldNullables == null) match {
      case (true, true) => registerBoundedStreamInternal(tableName, boundedStream)
      case (false, true) => registerBoundedStreamInternal(tableName, boundedStream, fields)
      case (false, false) => registerBoundedStreamInternal(tableName, boundedStream, fields,
        fieldNullables.toArray)
      case (true, false) => throw new IllegalArgumentException("Can not register collection with" +
        "empty field names while fieldNullables non empty.")
    }
  }

  /**
    * Create a [[Table]] from sequence of elements. Typical, user can pass in a sequence of tuples,
    * the table schema type would be inferred from the tuple type: e.g.
    * {{{
    *   tEnv.fromElements((1, 2, "abc"), (3, 4, "def"))
    * }}}
    * Then the schema type would be (_1:int, _2:int, _3:varchar)
    *
    * Caution that use must pass a ''Scala'' type data elements, or the inferred type
    * would be unexpected.
    *
    * @param data row data sequence
    * @tparam T row data class type
    * @return table from the data with default fields names
    */
  def fromElements[T: ClassTag : TypeInformation](data: T*): Table = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    fromCollection(data)(implicitly[ClassTag[T]], typeInfo)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. The default fields names
    * would be like _1, _2, _3 and so on. The table schema type would be inferred from the
    * [[Iterable]] element type.
    */
  def fromCollection[T: ClassTag : TypeInformation](data: Iterable[T]): Table = {
    val typeInfo = implicitly[TypeInformation[T]]
    fromCollection(null, data, typeInfo, null)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. The table schema type would be inferred
    * from the [[Iterable]] element type.
    */
  def fromCollection[T: ClassTag : TypeInformation](data: Iterable[T],
    fields: Expression*): Table = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    val fieldArray = if (fields == null || fields.isEmpty) null else fields.toArray
    fromCollection(null, data, typeInfo, fieldArray)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. The table schema type would be inferred
    * from the [[Iterable]] element type.
    *
    * Only used for testing now.
    */
  @VisibleForTesting
  private [table] def fromCollection[T: ClassTag : TypeInformation](data: Iterable[T],
    fields: Array[Expression]): Table = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    fromCollection(null, data, typeInfo, fields)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. Would infer table schema from the passed in
    * typeInfo.
    */
  def fromCollection[T](data: Iterable[T], typeInfo: TypeInformation[T],
    fields: Expression*): Table = {
    val fieldArray = if (fields == null || fields.isEmpty) null else fields.toArray
    fromCollection(null, data, typeInfo, fieldArray)
  }

  /**
    * Create a [[Table]] from a scala [[Iterable]]. Would infer table schema from the passed in
    * typeInfo.
    */
  @VisibleForTesting
  private [table] def fromCollection[T](
    tableName: String,
    data: Iterable[T],
    typeInfo: TypeInformation[T],
    fields: Array[Expression]): Table = {
    CollectionInputFormat.checkCollection(data.asJavaCollection, typeInfo.getTypeClass)
    val boundedStream = streamEnv.createInput(new CollectionInputFormat[T](
      data.asJavaCollection,
      typeInfo.createSerializer(execEnv.getConfig)),
      typeInfo, getCallLocationName())
    val name = if (tableName == null) createUniqueTableName() else tableName
    if (fields == null) {
      registerBoundedStreamInternal(name, boundedStream)
    } else {
      registerBoundedStreamInternal(name, boundedStream, fields)
    }
    scan(name)
  }

  /**
    * Creates a [[Table]] with a single `DataTypes.Long` column named `id`, containing elements
    * in a range from 0 to `end` (exclusive) with step value 1.
    */
  def range(end: Long): Table = {
    range(0, end)
  }

  /**
    * Creates a [[Table]] with a single `DataTypes.Long` column named `id`, containing elements
    * in a range from `start` to `end` (exclusive) with step value 1.
    */
  def range(start: Long, end: Long): Table = {
    val typeInfo = new BaseRowTypeInfo[BoxedWrapperRow](classOf[BoxedWrapperRow], Types.LONG)
    val boundedStream = streamEnv.createInput(new RangeInputFormat(start, end),
      typeInfo, getCallLocationName())
    fromBoundedStream(new DataStream[BoxedWrapperRow](boundedStream), 'id)
  }

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    * @tparam T The type of the output row, it can be a scala class.
    */
  def registerScalaTableFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    registerTableFunction(name, tf, DataTypes.of(implicitly[TypeInformation[T]]))
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String, f: AggregateFunction[T, ACC]): Unit = {
    registerAggregateFunction[T, ACC](
      name,
      f,
      DataTypes.of(implicitly[TypeInformation[T]]),
      DataTypes.of(implicitly[TypeInformation[ACC]]))
  }

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    registerTableFunction(name, tf, DataTypes.of(implicitly[TypeInformation[T]]))
  }
}
