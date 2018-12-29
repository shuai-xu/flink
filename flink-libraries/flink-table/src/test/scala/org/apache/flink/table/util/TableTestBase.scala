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

package org.apache.flink.table.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.api.java.{StreamTableEnvironment => JStreamTableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSet
import org.apache.calcite.util.ImmutableBitSet
import org.apache.commons.lang3.SystemUtils
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.rules.{ExpectedException, TestName}
import org.mockito.Mockito.{mock, when}

import java.util
import java.util.{ArrayList => JArrayList}

/**
  * Test base for testing Table API / SQL plans.
  */
abstract class TableTestBase {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  // used for get test case method name
  val testName: TestName = new TestName

  def streamTestUtil(): StreamTableTestUtil = StreamTableTestUtil(this)

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def name: TestName = testName

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(FlinkRelOptUtil.toString(expected.getRelNode)),
      LogicalPlanFormatUtils.formatTempTableId(FlinkRelOptUtil.toString(actual.getRelNode)))
  }

  def injectRules(tEnv: TableEnvironment, phase: String, injectRuleSet: RuleSet): Unit = {
    val programs = FlinkStreamPrograms.buildPrograms(tEnv.getConfig.getConf)
    programs.get(phase) match {
      case Some(groupProg: FlinkGroupProgram[StreamOptimizeContext]) =>
        groupProg.addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(injectRuleSet).build(), "test rules")
      case Some(ruleSetProgram: FlinkHepRuleSetProgram[StreamOptimizeContext]) =>
        ruleSetProgram.add(injectRuleSet)
      case _ =>
        throw new RuntimeException(s"$phase does not exist")
    }
    val builder = CalciteConfig.createBuilder(tEnv.getConfig.getCalciteConfig)
      .replaceStreamPrograms(programs)
    tEnv.getConfig.setCalciteConfig(builder.build())
  }
}

abstract class TableTestUtil {

  private var counter = 0

  def addTable[T: TypeInformation](fields: Expression*): Table = {
    counter += 1
    addTable[T](s"Table$counter", fields: _*)
  }

  def addTable[T: TypeInformation](name: String, fields: Expression*): Table

  def addFunction[T: TypeInformation](name: String, function: TableFunction[T]): TableFunction[T]

  def addFunction(name: String, function: ScalarFunction): Unit

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit

  def verifyPlan(sql: String): Unit

  def verifyPlan(table: Table): Unit

  def verifyPlanAndTrait(sql: String): Unit

  def verifyPlanAndTrait(table: Table): Unit

  def explainSql(query: String): String

  def explain(resultTable: Table): String

  def verifySchema(resultTable: Table, fields: Seq[(String, InternalType)]): Unit = {
    val actual = resultTable.getSchema
    val expected = new TableSchema(fields.map(_._1).toArray, fields.map(_._2).toArray)
    assertEquals(expected, actual)
  }
}

case class StreamTableTestUtil(test: TableTestBase) extends TableTestUtil {

  private lazy val diffRepository = DiffRepository.lookup(test.getClass)
  val javaEnv: JStreamExecutionEnvironment = mock(classOf[JStreamExecutionEnvironment])
  when(javaEnv.getStreamTimeCharacteristic).thenReturn(TimeCharacteristic.EventTime)
  val javaTableEnv: JStreamTableEnvironment = TableEnvironment.getTableEnvironment(javaEnv)
  val env: StreamExecutionEnvironment = mock(classOf[StreamExecutionEnvironment])
  when(env.getWrappedStreamExecutionEnvironment).thenReturn(javaEnv)
  val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
  : Table = {

    val ds = mock(classOf[DataStream[T]])
    val jDs = mock(classOf[JDataStream[T]])
    val jTrans = mock(classOf[StreamTransformation[T]])
    when(ds.javaStream).thenReturn(jDs)
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    when(jDs.getType).thenReturn(typeInfo)
    when(jDs.getTransformation).thenReturn(jTrans)
    when(jTrans.getParallelism).thenReturn(1)

    val t = ds.toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: String): Table = {

    val jDs = mock(classOf[JDataStream[T]])
    when(jDs.getType).thenReturn(typeInfo)

    val t = javaTableEnv.fromDataStream(jDs, fields)
    javaTableEnv.registerTable(name, t)
    t
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def verifyPlan(sql: String): Unit = {
    val resultTable = tableEnv.sqlQuery(sql)
    verifyPlan(resultTable)
  }

  def verifySqlPlansIdentical(query1: String, queries: String*): Unit = {
    val resultTable1 = tableEnv.sqlQuery(query1)
    queries.foreach(s => verify2Tables(resultTable1, tableEnv.sqlQuery(s)))
  }

  def verify2Tables(resultTable1: Table, resultTable2: Table): Unit = {
    val relNode1 = resultTable1.getRelNode
    val optimized1 = tableEnv.optimize(relNode1, updatesAsRetraction = false)
    val relNode2 = resultTable2.getRelNode
    val optimized2 = tableEnv.optimize(relNode2, updatesAsRetraction = false)
    assertEquals(FlinkRelOptUtil.toString(optimized1), FlinkRelOptUtil.toString(optimized2))
  }

  def verifyPlan(table: Table): Unit = {
    val relNode = table.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    val actual = SystemUtils.LINE_SEPARATOR + FlinkRelOptUtil.toString(optimized)

    verifyPlan(test.name.getMethodName, actual)
  }

  def verifyPlan(name: String, plan: String): Unit = {
    diffRepository.assertEquals(name, "plan", "${plan}", plan)
  }

  def verifyUniqueKeys(sql: String, expect: Set[Int]*): Unit = {
    val table = tableEnv.sqlQuery(sql)
    verifyUniqueKeys(table, expect: _*)
  }

  def verifyUniqueKeys(table: Table, expect: Set[Int]*): Unit = {
    val node = tableEnv.optimize(table.getRelNode, updatesAsRetraction = false)
    val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
    val actual = mq.getUniqueKeys(node)
    val expectSet = new util.HashSet[ImmutableBitSet]
    expect.filter(_.nonEmpty).foreach { array =>
      val keys = new JArrayList[Integer]()
      array.foreach(keys.add(_))
      expectSet.add(ImmutableBitSet.of(keys))
    }
    if (actual == null) {
      assert(expectSet == null || expectSet.isEmpty)
    } else {
      assertEquals(expectSet, actual)
    }
  }

  def verifyPlanAndTrait(sql: String): Unit = {
    val resultTable = tableEnv.sqlQuery(sql)
    verifyPlanAndTrait(resultTable)
  }

  def verifyPlanAndTrait(table: Table): Unit = {
    val relNode = table.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    val actualPlan = SystemUtils.LINE_SEPARATOR +
      FlinkRelOptUtil.toString(optimized, withRetractTraits = true)
    assertEqualsOrExpand("plan", actualPlan)
  }

  def explainSql(query: String): String = {
    val relNode = tableEnv.sqlQuery(query).getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    FlinkRelOptUtil.toString(optimized)
  }

  def explain(resultTable: Table): String = {
    tableEnv.explain(resultTable)
  }

  private def assertEqualsOrExpand(tag: String, actual: String, expand: Boolean = true): Unit = {
    val expected = s"$${$tag}"
    if (!expand) {
      diffRepository.assertEquals(test.name.getMethodName, tag, expected, actual)
      return
    }
    val expanded = diffRepository.expand(test.name.getMethodName, tag, expected)
    if (expanded != null && !expanded.equals(expected)) {
      // expected does exist, check result
      diffRepository.assertEquals(test.name.getMethodName, tag, expected, actual)
    } else {
      // expected does not exist, update
      diffRepository.expand(test.name.getMethodName, tag, actual)
    }
  }
}
