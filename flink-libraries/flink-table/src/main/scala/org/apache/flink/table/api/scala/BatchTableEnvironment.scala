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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.types.DataTypes

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
    registerBoundedStreamInternal(name, boundedStream)
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
    registerBoundedStreamInternal(name, boundedStream, fieldNullables)
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
    registerBoundedStreamInternal(name, boundedStream, fields.toArray)
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
    registerBoundedStreamInternal(name, boundedStream, fields.toArray, fieldNullables)
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
    registerBoundedStreamInternal(name, boundedStream, fields.toArray)
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
    registerBoundedStreamInternal(name, boundedStream, fields.toArray, fieldNullables)
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
    registerBoundedStreamInternal(name, boundedStream)
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
    registerBoundedStreamInternal(name, boundedStream, fieldNullables)
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
    registerBoundedStreamInternal(name, boundedStream, fields.toArray)
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
    registerBoundedStreamInternal(name, boundedStream, fields.toArray, fieldNullables)
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
      name: String,
      f: AggregateFunction[T, ACC])
  : Unit = {
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
