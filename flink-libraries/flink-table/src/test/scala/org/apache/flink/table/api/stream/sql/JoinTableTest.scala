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
package org.apache.flink.table.api.stream.sql

import java.util

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.api.{SqlParserException, TableException, TableSchema, ValidationException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.{AsyncConfig, DimensionTableSource, IndexKey}
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

class JoinTableTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proc.proctime, 'rt.rowtime)
  streamUtil.addTable[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
  streamUtil.tableEnv.registerTableSource("dimTemporal", new TestDimensionTable(true))
  streamUtil.tableEnv.registerTableSource("dimStatic", new TestDimensionTable(false))

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN dimTemporal T.proc AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // can't as of non-proctime field
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF T.rt AS D ON T.a = D.id",
      "Currently only support join temporal table as of on left table's proctime field",
      classOf[AssertionError])

    // can't query a dim table directly
    expectExceptionThrown(
      "SELECT * FROM dimTemporal FOR SYSTEM_TIME AS OF TIMESTAMP '2017-08-09 14:36:11'",
      "Cannot generate a valid execution plan for the given query",
      classOf[TableException]
    )

    // can't on non-key fields
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.age",
      "Join Dimension table requires an equality condition on ALL of table's primary key(s) or " +
        "unique key(s) or index field(s).",
      classOf[AssertionError]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id",
      "Join[type: stream to table join] only support LEFT JOIN or INNER JOIN, but was RIGHT",
      classOf[AssertionError]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON concat('rk:', T.a) = concat(D.id, '=rk')",
      "Join Dimension table requires an equality condition on ALL of table's primary key(s) or " +
        "unique key(s) or index field(s).",
      classOf[AssertionError]
    )
  }

  @Test
  def testJoinOnDifferentKeyTypes(): Unit = {
    // Will do implicit type coercion.
    streamUtil.verifyPlan("SELECT * FROM MyTable AS T JOIN LATERAL dimTemporal "
      + "FOR SYSTEM_TIME AS OF T.proc AS D ON T.b = D.id")
  }

  @Test
  def testJoinInvalidStaticTable(): Unit = {
    // can't follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LATERAL dimStatic " +
        "FOR SYSTEM_TIME AS OF T.rt AS D ON T.a = D.id",
      "Table 'dimStatic' is not a temporal table",
      classOf[ValidationException])

    // dimension table join dimension table
    expectExceptionThrown(
      "SELECT * FROM dimStatic AS T JOIN dimStatic AS D ON T.id = D.id",
      "Cannot generate a valid execution plan for the given query",
      classOf[TableException])
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN LATERAL dimTemporal " +
      "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN LATERAL dimTemporal " +
      "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proc FROM MyTable WHERE c > 1000) AS T " +
      "JOIN LATERAL dimTemporal " +
      "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN dimTemporal FOR SYSTEM_TIME AS OF PROCTIME() AS D
        |ON T.a = D.id
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LATERAL dimTemporal FOR SYSTEM_TIME AS OF T.proc AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithDimensionTableCalcPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LATERAL dimTemporal FOR SYSTEM_TIME AS OF T.proc AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE cast(D.name as bigint) > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiIndexColumn(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LATERAL dimTemporal FOR SYSTEM_TIME AS OF T.proc AS D
        |ON T.a = D.id AND D.age = 10 AND D.name = 'AAA'
        |WHERE T.c > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinStaticTable(): Unit = {
    val sql = "SELECT T.a, T.b, D.name FROM MyTable AS T JOIN dimStatic AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testAvoidAggregatePushDown(): Unit = {
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
        |SELECT T.* FROM ($sql1) AS T
        |JOIN LATERAL dimTemporal FOR SYSTEM_TIME AS OF PROCTIME() AS D
        |ON T.a = D.id
        |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
        |SELECT b, count(a), sum(c), sum(d)
        |FROM ($sql2) AS T
        |GROUP BY b
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  // ==========================================================================================

  private def expectExceptionThrown(
    sql: String,
    keywords: String,
    clazz: Class[_ <: Throwable] = classOf[ValidationException])
  : Unit = {
    try {
      streamUtil.tableEnv.explain(streamUtil.tableEnv.sqlQuery(sql))
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The exception message '${e.getMessage}' doesn't contain keyword '$keywords'",
            e.getMessage.contains(keywords))
        }
      case e: Throwable =>
        e.printStackTrace()
        fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }

  class TestDimensionTable(temporal: Boolean) extends DimensionTableSource[BaseRow] {

    override def getIndexes: util.Collection[IndexKey] = {
      val indexes = new util.ArrayList[IndexKey]()
      // index (name)
      indexes.add(IndexKey.of(false, 1))
      // unique key (id)
      indexes.add(IndexKey.of(true, 0))
      indexes
    }

    override def isTemporal: Boolean = temporal

    override def getLookupFunction(index: IndexKey): FlatMapFunction[BaseRow, BaseRow] = null

    override def getReturnType: DataType = {
      new BaseRowType(
        classOf[BaseRow],
        Array[InternalType](DataTypes.INT, DataTypes.STRING, DataTypes.INT),
        Array("id", "name", "age"))
    }

    override def isAsync: Boolean = false

    override def getAsyncLookupFunction(
        index: IndexKey): AsyncFunction[BaseRow, BaseRow] = {
      null
    }

    override def getAsyncConfig: AsyncConfig = new AsyncConfig

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType)

    override def explainSource(): String = ""
  }

}
