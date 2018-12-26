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
package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.AsyncFunction

import java.util
import java.util.Collections
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.calcite.CalciteConfigBuilder
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms
import org.apache.flink.table.sources.{AsyncConfig, DimensionTableSource, IndexKey}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{TableSchemaUtil, TableTestBatchExecBase}

import org.junit.Assert.{assertTrue, fail}
import org.junit.{Before, Test}

class JoinTableTest extends TableTestBatchExecBase {
  private val testUtil = batchExecTestUtil()

  @Before
  def before(): Unit = {
    testUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)
    testUtil.addTable[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    testUtil.tableEnv.registerTableSource("dimTemporal", new TestDimensionTable(true))
    testUtil.tableEnv.registerTableSource("dimStatic", new TestDimensionTable(false))
  }

  @Test
  def testLogicalPlan(): Unit = {
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
    val util = batchExecTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)
    util.addTable[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    util.tableEnv.registerTableSource("dimTemporal", new TestDimensionTable(true))
    util.tableEnv.registerTableSource("dimStatic", new TestDimensionTable(false))

    val programs = FlinkBatchPrograms.buildPrograms(util.tableEnv.getConfig.getConf)
    programs.remove(FlinkBatchPrograms.PHYSICAL)
    val calciteConfig = new CalciteConfigBuilder().setBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.verifyPlan(sql)
  }

  @Test
  def testLogicalPlanWithImplicitTypeCast(): Unit = {
    val util = batchExecTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)
    util.addTable[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    util.tableEnv.registerTableSource("dimTemporal", new TestDimensionTable(true))

    val programs = FlinkBatchPrograms.buildPrograms(util.tableEnv.getConfig.getConf)
    programs.remove(FlinkBatchPrograms.PHYSICAL)
    val calciteConfig = new CalciteConfigBuilder().setBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.verifyPlan("SELECT * FROM MyTable AS T JOIN LATERAL dimTemporal "
      + "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.b = D.id")
  }

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN dimTemporal T.proc AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // can't query a dim table directly
    expectExceptionThrown(
      "SELECT * FROM dimTemporal FOR SYSTEM_TIME AS OF TIMESTAMP '2017-08-09 14:36:11'",
      "Cannot generate a valid execution plan for the given query",
      classOf[TableException]
    )

    // can't on non-key fields
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.b = D.name",
      "Join Dimension table requires an equality condition on ALL of table's primary key(s) or " +
        "unique key(s) or index field(s).",
      classOf[AssertionError]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id",
      "Join[type: stream to table join] only support LEFT JOIN or INNER JOIN, but was RIGHT",
      classOf[AssertionError]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON concat('rk:', T.a) = concat(D.id, '=rk')",
      "Join Dimension table requires an equality condition on ALL of table's primary key(s) or " +
        "unique key(s) or index field(s).",
      classOf[AssertionError]
    )
  }

  @Test
  def testJoinInvalidStaticTable(): Unit = {
    // can't follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LATERAL dimStatic " +
          "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id",
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
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
        "(SELECT a, b FROM MyTable WHERE c > 1000) AS T " +
        "JOIN LATERAL dimTemporal " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
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
    testUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LATERAL dimTemporal FOR SYSTEM_TIME AS OF PROCTIME() AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin
    testUtil.verifyPlan(sql)
  }

  @Test
  def testJoinStaticTable(): Unit = {
    val sql = "SELECT T.a, T.b, D.name FROM MyTable AS T JOIN dimStatic AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
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
    testUtil.verifyPlan(sql)
  }

  @Test
  def testReusing(): Unit = {
    testUtil.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_REUSE_SUB_PLAN_ENABLED, true)
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT * FROM ($sql1) AS T
         |JOIN LATERAL dimTemporal FOR SYSTEM_TIME AS OF PROCTIME() AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin
    val sql3 =
      s"""
         |SELECT id as a, b FROM ($sql2) AS T
       """.stripMargin
    val sql =
      s"""
         |SELECT count(T1.a), count(T1.id), sum(T2.a)
         |FROM ($sql2) AS T1, ($sql3) AS T2
         |WHERE T1.a = T2.a
         |GROUP BY T1.b, T2.b
      """.stripMargin

    testUtil.verifyPlan(sql)
  }

  // ==========================================================================================

  private def expectExceptionThrown(
      sql: String,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException])
  : Unit = {
    try {
      testUtil.tableEnv.explain(testUtil.tableEnv.sqlQuery(sql))
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
      Collections.singleton(IndexKey.of(true, 0))
    }

    override def isTemporal: Boolean = temporal

    override def getLookupFunction(index: IndexKey): FlatMapFunction[BaseRow, BaseRow] = null

    override def getReturnType: DataType = {
      DataTypes.internal(
        new BaseRowTypeInfo(classOf[BaseRow],
          Array(Types.INT, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
          Array("id", "name", "age")))
    }

    /**
     * Returns true if this dimension table could be queried asynchronously
     */
    override def isAsync: Boolean = false

    override def getAsyncLookupFunction(
        index: IndexKey): AsyncFunction[BaseRow, BaseRow] = {
      null
    }

    /**
     * Returns config that defines the runtime behavior of async join table
     */
    override def getAsyncConfig: AsyncConfig = new AsyncConfig

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = TableSchemaUtil.fromDataType(getReturnType)

    override def explainSource(): String = ""
  }

}
