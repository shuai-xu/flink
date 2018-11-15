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
package com.alibaba.blink.table.api

import java.util

import com.alibaba.blink.table.api.BlinkTableUtils.injectBlinkRules
import com.alibaba.blink.table.sources.{HBaseDimensionTableSource => HTableSource}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{datastream, environment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.sources.{ProjectableTableSource, StreamTableSource, TableSource}
import org.apache.flink.table.util.TableTestBase
import org.apache.flink.types.Row
import org.junit.Test

class JoinHTableValidationTest extends TableTestBase {

  @Test(expected = classOf[UnsupportedOperationException])
  def validateUnsupportedSelectionOnHBaseTable(): Unit = {
    val data = List(
      (1, 2, "Hi"),
      (9, 12, "Hello world!"))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    injectBlinkRules(tEnv)
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content)
    // specify physical table name in HBase
    val tableName = "blink_test_1"
    val tableSource = new ProjectableHTableSource(
      "rk_1", new RowTypeInfo(
        Array(
          Types.INT,
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("rk_1", "a", "b")))
    tEnv.registerTableSource(tableName, tableSource)

    // unsupported operation on HBaseTableSource
    val hbaseTable: Table = tEnv.scan(tableName).select('a, 'b, 'rk_1)
    val joinTable = streamTable
      .join(hbaseTable, 'id === 'rk_1) // specify the join key of left table
      .select('id, 'len, 'content, 'a, 'b)
      .toAppendStream[Row]
  }

  @Test
  def validateMultiJoin(): Unit = {
    val util = streamTestUtil()
    val tEnv = util.tableEnv
    injectBlinkRules(tEnv)
    val streamTable = util.addTable[(Int, Int, String)]('id, 'len, 'nid)

    val tableSource = new ProjectableHTableSource(
      "rk_1", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("rk_1", "a", "b")))
    tEnv.registerTableSource("t0", tableSource)
    val hbaseTable: Table = tEnv.scan("t0")

    val tableSource1 = new ProjectableHTableSource(
      "nid", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        Array("nid", "category", "user_id")))
    tEnv.registerTableSource("t1", tableSource1)
    val hbaseTable1 = tEnv.scan("t1")

    val tableSource2 = new ProjectableHTableSource(
      "nid2", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("nid2", "pricegap")))
    tEnv.registerTableSource("t2", tableSource2)
    val hbaseTable2 = tEnv.scan("t2")

    val tableSource3 = new ProjectableHTableSource(
      "nid3", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("nid3", "user_type")))
    tEnv.registerTableSource("t3", tableSource3)
    val hbaseTable3 = tEnv.scan("t3")

    val joinTable1 = streamTable
      .join(hbaseTable, 'nid === 'rk_1)
      .select('id, 'len, 'a, 'b, 'nid as 'join_key, 'nid as 'join_key2, 'nid as 'join_key3)
      .leftOuterJoin(hbaseTable1, 'join_key === 'nid)
      .leftOuterJoin(hbaseTable2, 'join_key2 === 'nid2)
      .leftOuterJoin(hbaseTable3, 'join_key3 === 'nid3)
      .select('id, 'len, 'b, 'join_key2, 'category, 'pricegap, 'user_type)

    util.verifyPlan(joinTable1)
  }

  @Test
  def validateMultiJoinWithDependency(): Unit = {
    val util = streamTestUtil()
    val tEnv = util.tableEnv
    injectBlinkRules(tEnv)
    val streamTable = util.addTable[(Int, Int, String)]('id, 'len, 'nid)

    val tableSource = new ProjectableHTableSource(
      "rk_1", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("rk_1", "a", "b")))
    tEnv.registerTableSource("t0", tableSource)
    val hbaseTable: Table = tEnv.scan("t0")

    val tableSource1 = new ProjectableHTableSource(
      "nid", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        Array("nid", "category", "user_id")))
    tEnv.registerTableSource("t1", tableSource1)
    val hbaseTable1 = tEnv.scan("t1")

    val tableSource2 = new ProjectableHTableSource(
      "nid2", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("nid2", "pricegap")))
    tEnv.registerTableSource("t2", tableSource2)
    val hbaseTable2 = tEnv.scan("t2")

    val tableSource3 = new ProjectableHTableSource(
      "nid3", new RowTypeInfo(
        Array(
          Types.STRING,
          Types.STRING).asInstanceOf[Array[TypeInformation[_]]], Array("nid3", "user_type")))
    tEnv.registerTableSource("t3", tableSource3)
    val hbaseTable3 = tEnv.scan("t3")

    val joinTable1 = streamTable
      .join(hbaseTable, 'nid === 'rk_1)
      .select('id, 'len, 'a, 'b, 'nid as 'join_key, 'nid as 'join_key2, 'nid as 'join_key3)
      .leftOuterJoin(hbaseTable1, 'join_key === 'nid)
      //      .select('id, 'len, 'b, 'join_key, 'join_key2, 'join_key3, 'category, 'user_id)
      .leftOuterJoin(hbaseTable2, 'user_id === 'nid2)
      .leftOuterJoin(hbaseTable3, 'join_key3 === 'nid3)
      .select('id, 'len, 'b, 'join_key2, 'category, 'pricegap, 'user_type)

    util.verifyPlan(joinTable1)
  }
}

class ProjectableHTableSource(val key: String, val schema: RowTypeInfo) extends HTableSource[Row]
  with StreamTableSource[Row] with ProjectableTableSource with Serializable {
  var keySelected = true

  override def getKeyName: String = key

  override def isSelectKey: Boolean = keySelected

  override def setSelectKey(selectKey: Boolean): Unit = keySelected = selectKey

  override def getRowKeyIndex: Int = 0

  override def isStrongConsistency: Boolean = true

  override def isOrderedMode: Boolean = true

  override def getAsyncTimeoutMs: Long = 10 * 1000

  override def getAsyncBufferCapacity: Int = 3

  override def getOutputFieldsNumber: Int = getFieldNames.length

  override def getFieldNames: Array[String] = schema.getFieldNames

  override def getFieldTypes: Array[TypeInformation[_]] = schema.getFieldTypes

  override def getAsyncFetchFunction[IN, OUT](
      resultType: TypeInformation[OUT],
      joinType: JoinRelType,
      sourceKeyIndex: Int,
      keyType: TypeInformation[_],
      strongConsistency: Boolean,
      objectReuseEnabled: Boolean): AsyncFunction[IN, OUT] = ???

  override def getAsyncMultiFetchFunction[IN, OUT](
      resultType: TypeInformation[OUT],
      requiredLeftFieldIdx: Array[Int],
      requiredLeftFieldTypes: Array[TypeInformation[_]],
      chainedSources: util.List[HTableSource[_]],
      chainedJoinTypes: util.List[JoinRelType],
      chainedLeftJoinKeyIndexes: util.List[Integer],
      chainedLeftJoinKeyTypes: util.List[TypeInformation[_]],
      strongConsistencyOnChain: Boolean,
      objectReuseEnabled: Boolean): AsyncFunction[IN, OUT] = ???

  override def getReturnType: DataType = {
    DataTypes.of(new RowTypeInfo(getFieldTypes, getFieldNames))
  }

  override def getDataStream(execEnv: environment.StreamExecutionEnvironment): datastream
  .DataStream[Row] = throw new UnsupportedOperationException("unsupported")

  // Used for PPD.
  override def explainSource(): String = {
    s"selectedFields=[${getFieldNames.mkString(", ")}]"
  }

  override def projectFields(fields: Array[Int]): TableSource = {
    // key cannot be projected
    val newNames = schema.getFieldNames.zipWithIndex.filter(f => f._2 == 0 || fields.contains(f._2))
      .map(_._1)
    val newTypes = schema.getFieldTypes.zipWithIndex.filter(f => f._2 == 0 || fields.contains(f._2))
      .map(_._1)
    new ProjectableHTableSource(key, new RowTypeInfo(newTypes, newNames))
  }

  override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType, None)
}
