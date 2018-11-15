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

package com.alibaba.blink.table.api.stream.sql

import java.util

import com.alibaba.blink.table.api.BlinkTableUtils.injectBlinkRules
import com.alibaba.blink.table.sources.HBaseDimensionTableSource
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.util.{ComplexDimTVF, MemoryTableSourceSinkUtil, StreamTableTestUtil, TableTestBase}
import org.junit.Test

class JoinComplexDimTVFTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  injectBlinkRules(streamUtil.tableEnv)
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proc.proctime, 'rt.rowtime)

  @Test(expected = classOf[TableException])
  def testBatchTF(): Unit = {
    val conf = new TableConfig
    conf.setSubsectionOptimization(true)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, conf)
    val mytable = StreamTestData.get3TupleDataStream(env)
                  .toTable(tableEnv, 'a, 'c, 'b)
    tableEnv.registerTable("MyTable", mytable)
    tableEnv.registerTableSource("dimHBase", new TestHBaseDimensionTable("dim1"))
    val tvf1 = new ComplexDimTVF(new TestHBaseDimensionTable("dim2"), "name")
    val tvf2 = new ComplexDimTVF(new TestHBaseDimensionTable("dim3"), "id")
    tableEnv.registerFunction("batchJoinHBaseTF1", tvf1)
    tableEnv.registerFunction("batchJoinHBaseTF2", tvf2)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes: Array[DataType] = Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tableEnv.registerTableSink("targetTable1", fieldNames, fieldTypes, sink)
    val sink2 = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tableEnv.registerTableSink("targetTable2", fieldNames, fieldTypes, sink2)

    val sql = """
                |select a, c, b, t.name as na, T2.names as names
                |from ( SELECT a, c, b, d.name
                |FROM MyTable m LEFT JOIN dimHBase d on m.a = d.id
                |) t join LATERAL TABLE(batchJoinHBaseTF1(b)) as T2(ids, names, ages) on true
              """.stripMargin

    val res = tableEnv.sqlQuery(sql)
    tableEnv.registerTable("tmp_1", res)
    tableEnv.sqlUpdate("insert into targetTable1 select a, c, names from tmp_1")

    val sql2 = """
                 |insert into targetTable2
                 |SELECT a, c, D.names
                 |FROM tmp_1, LATERAL TABLE(batchJoinHBaseTF2(c)) as D(ids, names, ages)
               """.stripMargin
    tableEnv.sqlUpdate(sql2)

    tableEnv.execute
  }
}

class TestHBaseDimensionTable(tableName: String)
  extends HBaseDimensionTableSource[BaseRow] with Serializable {
  override def explainSource(): String = tableName

  override def getKeyName: String = ???

  override def setSelectKey(selectKey: Boolean): Unit = ???

  override def isSelectKey: Boolean = ???

  override def getRowKeyIndex: Int = ???

  override def isStrongConsistency: Boolean = ???

  override def isOrderedMode: Boolean = ???

  override def getAsyncTimeoutMs: Long = ???

  override def getAsyncBufferCapacity: Int = ???

  /** Returns the number of fields of the table. */
  override def getOutputFieldsNumber: Int = ???

  /** Returns the names of the table fields. */
  override def getFieldNames: Array[String] = ???

  /** Returns the names of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = ???

  override def getAsyncFetchFunction[IN, OUT](
      resultType: TypeInformation[OUT],
      joinType: JoinRelType,
      sourceKeyIndex: Int,
      keyType: TypeInformation[_],
      strongConsistency: Boolean,
      objectReuseEnabled: Boolean): AsyncFunction[IN, OUT] = {
    null
  }

  override def getAsyncMultiFetchFunction[IN, OUT](
      resultType: TypeInformation[OUT],
      requiredLeftFieldIdx: Array[Int],
      requiredLeftFieldTypes: Array[TypeInformation[_]],
      chainedSources: util.List[HBaseDimensionTableSource[_]],
      chainedJoinTypes: util.List[JoinRelType],
      chainedLeftJoinKeyIndexes: util.List[Integer],
      chainedLeftJoinKeyTypes: util.List[TypeInformation[_]],
      strongConsistencyOnChain: Boolean,
      objectReuseEnabled: Boolean): AsyncFunction[IN, OUT] = {
    null
  }

  override def getReturnType: DataType = {
    DataTypes.createBaseRowType(
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.INT),
      Array("id", "name", "age"))
  }

  override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType, None)
}
