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

import org.apache.calcite.sql.SqlExplainLevel
import org.apache.commons.lang3.SystemUtils
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, DataStream => ScalaStream}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.api.{Table, TableException, _}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecRel, BatchExecSink}
import org.apache.flink.table.plan.subplan.BatchDAGOptimizer
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.resource.batch.RunningUnitKeeper
import org.apache.flink.table.sources.{BatchTableSource, LimitableTableSource, TableSource}
import org.apache.flink.types.Row

import org.apache.calcite.sql.SqlExplainLevel

import org.apache.commons.lang3.SystemUtils

import _root_.scala.collection.mutable
import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

import org.junit.Assert._
import org.junit.Rule
import org.junit.rules.{ExpectedException, TestName}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

/**
  * Test batch exec base for testing Table API / SQL plans.
  */
class TableTestBatchExecBase {
  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  // used for get test case method name
  val testName: TestName = new TestName

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def name: TestName = testName

  def batchExecTestUtil(): BatchExecTableTestUtil = {
    BatchExecTableTestUtil(this)
  }

  def batchTestUtil(): BatchExecTableTestUtil = {
    BatchExecTableTestUtil(this)
  }

  def nullableBatchExecTestUtil(fieldsNullable: Boolean): NullableBatchExecTableTestUtil = {
    new NullableBatchExecTableTestUtil(fieldsNullable, this)
  }

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(FlinkRelOptUtil.toString(expected.getRelNode)),
      LogicalPlanFormatUtils.formatTempTableId(FlinkRelOptUtil.toString(actual.getRelNode)))
  }
}

abstract class TableTestBatchExecUtil {

  private var counter = 0

  def addTable[T: TypeInformation](fields: Expression*): Table = {
    counter += 1
    addTable[T](s"Table$counter", fields: _*)
  }

  def addTable[T: TypeInformation](name: String, fields: Expression*): Table

  def addTable[T: TypeInformation](
      name: String,
      uniqueKeys: Set[Set[String]],
      fields: Expression*): Table

  def verifySqlNotExpected(query: String, notExpected: String*): Unit

  def verifyTableNotExpected(resultTable: Table, notExpected: String*): Unit

  def verifyPlan(): Unit

  def verifyPlan(sql: String): Unit

  def verifyPlan(sql: String, explainLevel: SqlExplainLevel): Unit

  def verifyPlan(sql: String, explainLevel: SqlExplainLevel, printPlanBefore: Boolean): Unit

  def verifyPlan(resultTable: Table): Unit

  def verifyPlan(resultTable: Table, explainLevel: SqlExplainLevel): Unit

  def verifyPlan(resultTable: Table, explainLevel: SqlExplainLevel, printPlanBefore: Boolean): Unit

  def verifyResultPartitionCount(): Unit

  def verifyResource(sql: String): Unit

  def verifyResultPartitionCount(resultTable: Table): Unit

  def verifyPlanWithRunningUnit(sql: String): Unit

  // the print methods are for debugging purposes only
  def printTable(
      resultTable: Table,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES): Unit

  def printSql(
      query: String,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES): Unit
}

case class BatchExecTableTestUtil(test: TableTestBatchExecBase) extends TableTestBatchExecUtil {
  def answer[T](f: InvocationOnMock => T) = new Answer[T] {
    override def answer(i: InvocationOnMock): T = f(i)
  }

  private lazy val diffRepository = DiffRepository.lookup(test.getClass)
  val javaEnv = new LocalStreamEnvironment()
  val javaTableEnv = TableEnvironment.getBatchTableEnvironment(javaEnv)
  val env = new StreamExecutionEnvironment(javaEnv)
  val tableEnv = TableEnvironment.getBatchTableEnvironment(env)
  tableEnv.getConfig.setCalciteConfig(CalciteConfig.createBuilder().build())
  tableEnv.getConfig.setSubsectionOptimization(true)

  def disableBroadcastHashJoin(): Unit = {
    tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD, -1)
  }

  def setJoinReorderEnabled(joinReorderEnabled: Boolean): Unit = {
    tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, joinReorderEnabled)
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T])
  : TableFunction[T] = {
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

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*): Table = {

    val bs = env.fromElements()
    val t = tableEnv.fromBoundedStream(bs, fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addTableSource(name: String,
      tableSchema: TableSchema,
      limitPushDown: Boolean = false,
      stats: TableStats = null): TableSource = {

    val table = new TestBatchTableSource(tableSchema, limitPushDown, stats)
    addTable(name, table)
    table
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: Expression*): Table = {
    val stream = javaEnv.addSource(new EmptySource[T], typeInfo)
    val t = tableEnv.fromBoundedStream(new ScalaStream[T](stream), fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addTable(name: String, t: TableSource): Unit = {
    tableEnv.registerTableSource(name, t)
  }

  def addTable[T: TypeInformation](
      name: String,
      uniqueKeys: Set[Set[String]],
      fields: Expression*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val physicalSchema = TableSchemaUtil.fromDataType(DataTypes.of(typeInfo))
    val (fieldNames, fieldIdxs) =
      tableEnv.getFieldInfo(DataTypes.of(typeInfo), fields.toArray)
    val fieldTypes = fieldIdxs.map(physicalSchema.getType)
    val tableSchema = new TableSchema(fieldNames, fieldTypes)
    val mapping = fieldNames.zipWithIndex.map {
      case (name:String, idx:Int) =>
        (name, physicalSchema.getColumnName(fieldIdxs.apply(idx)))
    }.toMap
    val ts = new TestTableSourceWithTime(tableSchema, typeInfo, Seq(), mapping = mapping)
    tableEnv.registerTableSource(name, ts, uniqueKeys.map(_.asJava).asJava)
    tableEnv.scan(name)
  }

  def getTableEnv: BatchTableEnvironment = tableEnv

  def verifySqlNotExpected(query: String, notExpected: String*): Unit = {
    verifyTableNotExpected(tableEnv.sqlQuery(query), notExpected: _*)
  }

  def verifyTableNotExpected(resultTable: Table, notExpected: String*): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode)
    val actual = FlinkRelOptUtil.toString(optimized)
    val result = notExpected.forall(!actual.contains(_))
    assertTrue(s"\n actual: \n$actual \n not expected: \n${notExpected.mkString(", ")}", result)
  }

  override def verifyPlan(): Unit = {
    doVerifyPlanWithSubsectionOptimization(explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  override def verifyPlan(sql: String): Unit = {
    verifyPlan(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  override def verifyPlan(sql: String, explainLevel: SqlExplainLevel): Unit = {
    verifyPlan(sql, explainLevel, printPlanBefore = true)
  }

  def verifyPlan(sql: String, explainLevel: SqlExplainLevel, printPlanBefore: Boolean): Unit = {
    val resultTable = tableEnv.sqlQuery(sql)
    assertEqualsOrExpand("sql", sql)
    verifyPlan(resultTable, explainLevel = explainLevel, printPlanBefore = printPlanBefore)
  }

  override def verifyPlan(resultTable: Table): Unit = {
    verifyPlan(resultTable, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  override def verifyPlan(resultTable: Table, explainLevel: SqlExplainLevel): Unit = {
    doVerifyPlan(resultTable, explainLevel = explainLevel)
  }

  override def verifyPlan(
    resultTable: Table,
    explainLevel: SqlExplainLevel,
    printPlanBefore: Boolean): Unit = {
    doVerifyPlan(resultTable, explainLevel = explainLevel, printPlanBefore = printPlanBefore)
  }

  override def verifyResultPartitionCount(): Unit = {
    doVerifyPlanWithSubsectionOptimization(
      explainLevel = SqlExplainLevel.NO_ATTRIBUTES,
      printResultPartitionCount = true,
      printPlanBefore = false)
  }

  override def verifyResource(sql: String): Unit = {
    assertEqualsOrExpand("sql", sql)
    doVerifyPlan(
      tableEnv.sqlQuery(sql),
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource = true,
      printPlanBefore = false)
  }

  override def verifyResultPartitionCount(resultTable: Table): Unit = {
    doVerifyPlan(
      resultTable,
      explainLevel = SqlExplainLevel.NO_ATTRIBUTES,
      printResource = true,
      printPlanBefore = false)
  }

  def verifyPlanWithRunningUnit(sql: String): Unit = {
    assertEqualsOrExpand("sql", sql)
    doVerifyPlan(
      tableEnv.sqlQuery(sql),
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printPlanBefore = false,
      printRunningUnit = true)
  }

  private def doVerifyPlan(
      resultTable: Table,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource: Boolean = false,
      printPlanBefore: Boolean = true,
      printRunningUnit: Boolean = false): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode)

    val ruKeeper = new RunningUnitKeeper(tableEnv)
    optimized match {
      case batchExecRel: BatchExecRel[_] => ruKeeper.buildRUs(batchExecRel)
      case _ => Unit
    }

    if (printResource) {
      ruKeeper.calculateRelResource(optimized.asInstanceOf[BatchExecRel[_]])
    }

    if (printPlanBefore) {
      val planBefore = SystemUtils.LINE_SEPARATOR + FlinkRelOptUtil.toString(
        relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false)
      assertEqualsOrExpand("planBefore", planBefore)
    }

    if (printRunningUnit) {
      val ruList = ruKeeper.getRunningUnits.map(x => x.toString)
      ruList.sorted
      val ruString = SystemUtils.LINE_SEPARATOR + String.join("\n", ruList)
      assertEqualsOrExpand("runningUnit", ruString)
    }

    val actual = SystemUtils.LINE_SEPARATOR + FlinkRelOptUtil.toString(
      optimized, explainLevel, printResource)
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  private def doVerifyPlanWithSubsectionOptimization(
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResultPartitionCount: Boolean = false,
      printPlanBefore: Boolean = true): Unit = {
    if (!tableEnv.getConfig.getSubsectionOptimization) {
      throw new TableException(
        "subsection optimization is false, please use other method to verify result.")
    }

    if (tableEnv.sinkNodes.isEmpty) {
      throw new TableException("No table sinks have been created yet. " +
                                 "A program needs at least one sink that consumes data. ")
    }
    if (printPlanBefore) {
      val planBefore = new StringBuilder
      tableEnv.sinkNodes.foreach { sink =>
        val table = new Table(tableEnv, sink.children.head)
        val ast = table.getRelNode
        planBefore.append(System.lineSeparator)
        planBefore.append(FlinkRelOptUtil.toString(
          ast, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false))
      }
      assertEqualsOrExpand("planBefore", planBefore.toString())
    }

    val optSinkNodes = tableEnv.tableServiceManager.cachePlanBuilder.
                       buildPlanIfNeeded(tableEnv.sinkNodes)


    val dagOptimizer = new BatchDAGOptimizer(optSinkNodes, tableEnv)
    // optimize dag
    val sinks = dagOptimizer.getOptimizedDag()
    tableEnv.sinkNodes.clear()

    // set resource
    val planAfter = if (printResultPartitionCount) {
      val planWithResource = new StringBuilder()
      val ruKeeper = new RunningUnitKeeper(tableEnv)
      sinks.map {
        case node: BatchExecSink[_] =>
          ruKeeper.buildRUs(node)
          ruKeeper.calculateRelResource(node)
          planWithResource.append(System.lineSeparator)
          planWithResource.append("[[Sink]]")
          planWithResource.append(System.lineSeparator)
          planWithResource.append(FlinkRelOptUtil.toString(
            node,
            detailLevel = explainLevel,
            printResultPartitionCount))
          planWithResource.append(System.lineSeparator)
        case _ => // ignore
      }
      planWithResource.deleteCharAt(planWithResource.length - 1)
      planWithResource.toString
    } else {
      dagOptimizer.explain()
    }
    assertEqualsOrExpand("planAfter", planAfter, expand = false)
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

  def printTable(
      resultTable: Table,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode)
    println(FlinkRelOptUtil.toString(optimized, detailLevel = explainLevel))
  }

  def printSql(
      query: String,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES): Unit = {
    printTable(tableEnv.sqlQuery(query), explainLevel)
  }
}

class TestBatchTableSource(tableSchema: TableSchema,
    limitPushDown: Boolean = false,
    stats: TableStats = null)
    extends BatchTableSource[Row] with LimitableTableSource {

  override def getReturnType: DataType =
    DataTypes.createRowType(
      tableSchema.getTypes.asInstanceOf[Array[DataType]],
      tableSchema.getColumnNames)

  override def getTableStats: TableStats = if (stats == null) {
    new TableStats(10L, new mutable.HashMap[String, ColumnStats]())
  } else {
    stats
  }

  /** Returns the table schema of the table source */
  override def getTableSchema: TableSchema = TableSchemaUtil.fromDataType(getReturnType)

  override def explainSource(): String = ""

  /**
    * Returns the data of the table as a [[DataStream]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    * Do not use it in Table API programs.
    */
  override def getBoundedStream(streamEnv: JavaEnv): DataStream[Row] = {
    val transformation = mock(classOf[StreamTransformation[Row]])
    when(transformation.getMaxParallelism).thenReturn(-1)
    val bs = mock(classOf[DataStream[Row]])
    when(bs.getTransformation).thenReturn(transformation)
    when(transformation.getOutputType).thenReturn(
      DataTypes.toTypeInfo(getReturnType).asInstanceOf[TypeInformation[Row]])
    bs
  }

  /**
    * Check and push down the limit to the table source.
    *
    * @param limit the value which limit the number of records.
    * @return A new cloned instance of [[TableSource]]
    */
  override def applyLimit(limit: Long): TableSource = this

  /**
    * Return the flag to indicate whether limit push down has been tried. Must return true on
    * the returned instance of [[applyLimit]].
    */
  override def isLimitPushedDown = limitPushDown
}

class NullableBatchExecTableTestUtil(fieldsNullable: Boolean, test: TableTestBatchExecBase)
  extends BatchExecTableTestUtil(test) {

  override def addTable[T: TypeInformation](name: String, fields: Expression*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val fieldTypes = typeInfo match {
      case tt: TupleTypeInfo[_] => tt.getGenericParameters.values().asScala.toArray
      case ct: CaseClassTypeInfo[_] => ct.getGenericParameters.values().asScala.toArray
      case bt: AtomicType[_] => Array[TypeInformation[_]](bt)
      case _ => throw new TableException(s"Unsupported type info: $typeInfo")
    }
    val fieldNullables = Array.fill(fields.size)(fieldsNullable)
    val (fieldNames, _) = tableEnv.getFieldInfo(DataTypes.of(typeInfo), fields.toArray)
    val ts = new TestTableSourceWithFieldNullables(fieldNames, fieldTypes, fieldNullables)
    tableEnv.registerTableSource(name, ts)
    tableEnv.scan(name)
  }
}
