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

package org.apache.flink.table.plan.cost

import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}
import java.util

import org.apache.calcite.plan.{AbstractRelOptPlanner, Context, Contexts, RelOptCluster}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.api.{TableConfig, TableSchema}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.{ColumnStats, FlinkStatistic, TableStats}
import org.apache.flink.table.sources.{Partition, PartitionableTableSource, TableSource}
import org.apache.flink.util.Preconditions
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Before, BeforeClass, Test}
import org.powermock.api.mockito.PowerMockito._
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner

import scala.collection.JavaConverters._

/**
  * Tests for [[SelectivityEstimator]].
  *
  * We use PowerMockito instead of Mockito here, because [[TableScan#getRowType]] is a final method.
  */
@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[TableScan]))
class SelectivityEstimatorTest {
  val allFieldNames = Seq("name", "amount", "price", "flag", "partition",
    "coldate", "coltime", "coltimestamp")
  val allFieldTypes = Seq(VARCHAR, INTEGER, DOUBLE, BOOLEAN, VARCHAR,
    DATE, TIME, TIMESTAMP)
  val (name_idx, amount_idx, price_idx, flag_idx, partition_idx,
  date_idx, time_idx, timestamp_idx) = (0, 1, 2, 3, 4, 5, 6, 7)

  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  var rexBuilder = new RexBuilder(typeFactory)
  val relDataType: RelDataType = typeFactory.createStructType(
    allFieldTypes.map(typeFactory.createSqlType).asJava,
    allFieldNames.asJava)

  val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
  var scan: TableScan = _

  @Before
  def setup(): Unit = {
    scan = mockScan()
  }

  private def mockScan(
    statistic: FlinkStatistic = FlinkStatistic.UNKNOWN,
    isPartitionableTableSource: Boolean = false,
    isFilterPushedDown: Boolean = false,
    tableConfig: Option[TableConfig] = None): TableScan = {
    val tableScan = mock(classOf[TableScan])
    val cluster = mock(classOf[RelOptCluster])
    val planner = mock(classOf[AbstractRelOptPlanner])
    val context = tableConfig match {
      case Some(config) => Contexts.of(config)
      case _ => mock(classOf[Context])
    }
    when(tableScan, "getCluster").thenReturn(cluster)
    when(cluster, "getRexBuilder").thenReturn(rexBuilder)
    when(cluster, "getPlanner").thenReturn(planner)
    when(planner, "getContext").thenReturn(context)
    when(tableScan, "getRowType").thenReturn(relDataType)
    val innerTable = if (isPartitionableTableSource && isFilterPushedDown) {
      // partitionableTableSource which filter already pushed down
      new TableSourceTable(new TestPartitionableTableSource(true)) {
        /**
         * Creates a copy of this table, changing statistic.
         *
         * @param statistic A new FlinkStatistic.
         * @return Copy of this table, substituting statistic.
         */
        override def copy(statistic: FlinkStatistic) = ???

        override def getRowType(relDataTypeFactory: RelDataTypeFactory) = relDataType

        override def replaceTableSource(tableSource: TableSource) = ???
      }
    } else if (isPartitionableTableSource) {
      // partitionableTableSource which filter is not push down
      new TableSourceTable(new TestPartitionableTableSource(false)) {
        /**
         * Creates a copy of this table, changing statistic.
         *
         * @param statistic A new FlinkStatistic.
         * @return Copy of this table, substituting statistic.
         */
        override def copy(statistic: FlinkStatistic) = ???

        override def getRowType(relDataTypeFactory: RelDataTypeFactory) = relDataType

        override def replaceTableSource(tableSource: TableSource) = ???
      }
    } else {
      mock(classOf[TableSourceTable])
    }
    val flinkTable = mock(classOf[FlinkRelOptTable])
    when(flinkTable, "unwrap", classOf[FlinkTable]).thenReturn(innerTable)
    when(flinkTable, "getFlinkStatistic").thenReturn(statistic)
    when(flinkTable, "getRowType").thenReturn(relDataType)
    when(tableScan, "getTable").thenReturn(flinkTable)
    val rowCount: java.lang.Double = if (statistic != null && statistic.getTableStats != null) {
      statistic.getTableStats.rowCount.doubleValue()
    } else {
      100D
    }
    when(tableScan, "estimateRowCount", mq).thenReturn(rowCount)
    tableScan
  }

  private def createNumericLiteral(num: Long): RexLiteral = {
    rexBuilder.makeExactLiteral(BigDecimal.valueOf(num))
  }

  private def createNumericLiteral(num: Double): RexLiteral = {
    rexBuilder.makeExactLiteral(BigDecimal.valueOf(num))
  }

  private def createBooleanLiteral(b: Boolean): RexLiteral = {
    rexBuilder.makeLiteral(b)
  }

  private def createStringLiteral(str: String): RexLiteral = {
    rexBuilder.makeLiteral(str)
  }

  private def createDateLiteral(str: String): RexLiteral = {
    rexBuilder.makeDateLiteral(new DateString(str))
  }

  private def createTimeLiteral(str: String): RexLiteral = {
    rexBuilder.makeTimeLiteral(new TimeString(str), 0)
  }

  private def createTimeStampLiteral(str: String): RexLiteral = {
    rexBuilder.makeTimestampLiteral(new TimestampString(str), 0)
  }

  private def createTimeStampLiteral(millis: Long): RexLiteral = {
    rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), 0)
  }

  private def createInputRef(index: Int): RexInputRef = {
    createInputRefWithNullability(index, false)
  }

  private def createInputRefWithNullability(index: Int, isNullable: Boolean): RexInputRef = {
    val relDataType = typeFactory.createSqlType(allFieldTypes(index))
    val relDataTypeWithNullability = typeFactory.createTypeWithNullability(relDataType, isNullable)
    rexBuilder.makeInputRef(relDataTypeWithNullability, index)
  }

  private def createCall(operator: SqlOperator, exprs: RexNode*): RexNode = {
    Preconditions.checkArgument(exprs.nonEmpty)
    rexBuilder.makeCall(operator, exprs: _*)
  }

  private def createCast(expr: RexNode, indexInAllFieldTypes: Int): RexNode = {
    val relDataType = typeFactory.createSqlType(allFieldTypes(indexInAllFieldTypes))
    rexBuilder.makeCast(relDataType, expr)
  }

  private def createColumnStats(
    ndv: Option[java.lang.Long] = None,
    nullCount: Option[java.lang.Long] = None,
    avgLen: Option[java.lang.Double] = None,
    maxLen: Option[Integer] = None,
    min: Option[Any] = None,
    max: Option[Any] = None): ColumnStats = ColumnStats(
    ndv.getOrElse(null.asInstanceOf[java.lang.Long]),
    nullCount.getOrElse(null.asInstanceOf[java.lang.Long]),
    avgLen.getOrElse(null.asInstanceOf[java.lang.Double]),
    maxLen.getOrElse(null.asInstanceOf[java.lang.Integer]),
    max.orNull,
    min.orNull)

  private def createFlinkStatistic(
    rowCount: Option[java.lang.Long] = None,
    colStats: Option[Map[String, ColumnStats]] = None): FlinkStatistic = {
    val tableStats = if (colStats.isDefined) {
      TableStats(rowCount.getOrElse(null.asInstanceOf[java.lang.Long]), colStats.get.asJava)
    } else {
      TableStats(rowCount.getOrElse(null.asInstanceOf[java.lang.Long]), null)
    }
    FlinkStatistic.of(tableStats)
  }

  @Test
  def testEqualsWithLiteralOfNumericType(): Unit = {
    // amount = 50
    val predicate1 = createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic1), mq)

    // [10, 200] contains 50
    assertEquals(Some(1.0 / 80.0), estimator2.evaluate(predicate1))

    // amount = 5
    val predicate2 = createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(5))
    // [10, 200] does not contain 5
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))

    val estimator3 = new SelectivityEstimator(mockScan(statistic2), mq)
    // [10, 200] contains 50, but ndv is null
    assertEquals(estimator3.defaultEqualsSelectivity, estimator3.evaluate(predicate1))

    // min or max is null
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), None))))
    val estimator4 = new SelectivityEstimator(mockScan(statistic3), mq)
    // [10, +INF) contains 50
    assertEquals(Some(1.0 / 80.0), estimator4.evaluate(predicate1))
    // [10, +INF) does not contain 5
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // ndv is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator5 = new SelectivityEstimator(mockScan(statistic4), mq)
    // [10, +INF) contains 50
    assertEquals(estimator5.defaultEqualsSelectivity, estimator5.evaluate(predicate1))
    // [10, +INF) does not contain 5
    assertEquals(Some(0.0), estimator5.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfStringType(): Unit = {
    // name = "abc"
    val predicate1 = createCall(EQUALS, createInputRef(name_idx), createStringLiteral("abc"))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate1))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("name" ->
      createColumnStats(Some(800L), Some(0L), Some(16.0), Some(32), Some("aaa"), Some("max")))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    // ["aaa", "max"] contains "abc"
    assertEquals(Some(0.00125), estimator2.evaluate(predicate1))

    // name = "xyz"
    val predicate2 = createCall(EQUALS, createInputRef(name_idx), createStringLiteral("xyz"))
    // ["aaa", "max"] contains "xyz"
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfBooleanType(): Unit = {
    // flag = true
    val predicate1 = createCall(EQUALS, createInputRef(flag_idx), createBooleanLiteral(true))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate1))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("flag" ->
      createColumnStats(Some(2L), Some(0L), Some(1.0), Some(1), Some(false), Some(true)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    // [false, true] contains true
    assertEquals(Some(0.5), estimator2.evaluate(predicate1))

    // flag = false
    val predicate2 = createCall(EQUALS, createInputRef(flag_idx), createBooleanLiteral(false))
    // [false, true] contains false
    assertEquals(Some(0.5), estimator2.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfDateType(): Unit = {
    // coldate = "2017-10-11"
    val predicate = createCall(
      EQUALS,
      createInputRef(date_idx),
      createDateLiteral("2017-10-11"))
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "coldate" -> createColumnStats(
        Some(80L),
        None,
        None,
        None,
        Some(Date.valueOf("2017-10-01")),
        Some(Date.valueOf("2018-10-01"))))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)
    // ["2017-10-01", "2018-10-01"] contains "2017-10-11"
    assertEquals(Some(1.0 / 80.0), estimator.evaluate(predicate))

    // coldate = "2018-10-02"
    val predicate2 = createCall(
      EQUALS,
      createInputRef(date_idx),
      createDateLiteral("2018-10-02")
    )

    // ["2017-10-01", "2018-10-01"] does not contain "2018-10-02"
    assertEquals(Some(0.0), estimator.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfTimeType(): Unit = {
    // coltime = "11:00:00"
    val predicate = createCall(
      EQUALS,
      createInputRef(time_idx),
      createTimeLiteral("11:00:00"))
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "coltime" -> createColumnStats(
        Some(80L),
        None,
        None,
        None,
        Some(Time.valueOf("10:00:00")),
        Some(Time.valueOf("12:00:00"))))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)
    // ["10:00:00", "12:00:00"] contains "11:00:00"
    assertEquals(Some(1.0 / 80.0), estimator.evaluate(predicate))

    // coltime = "13:00:00"
    val predicate2 = createCall(
      EQUALS,
      createInputRef(time_idx),
      createTimeLiteral("13:00:00"))

    // ["10:00:00", "12:00:00"] does not contains "13:00:00"
    assertEquals(Some(0.0), estimator.evaluate(predicate2))
  }

  @Test
  def testEqualsWithLiteralOfTimestampType(): Unit = {
    val predicate = createCall(
      EQUALS,
      createInputRef(timestamp_idx),
      createTimeStampLiteral(1000L))
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "coltimestamp" -> createColumnStats(
        Some(80L),
        None,
        None,
        None,
        Some(new Timestamp(0L)),
        Some(new Timestamp(2000L))))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(Some(1.0 / 80.0), estimator.evaluate(predicate))

    val predicate2 = createCall(
      EQUALS,
      createInputRef(timestamp_idx),
      createTimeStampLiteral(3000L))

    // ["2017-10-01 10:00:00", "2018-10-01 10:00:00"] does not contains "2018-10-01 10:00:01"
    assertEquals(Some(0.0), estimator.evaluate(predicate2))
  }

  @Test
  def testEqualsWithoutLiteral(): Unit = {
    // amount = price
    val predicate = createCall(EQUALS, createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultEqualsSelectivity, estimator1.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(2), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))

    // min or max is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), None, Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(estimator1.defaultEqualsSelectivity,
      new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate))

    // ndv is null
    val statistic5 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(estimator1.defaultEqualsSelectivity,
      new SelectivityEstimator(mockScan(statistic5), mq).evaluate(predicate))
  }

  @Test
  def testNotEqualsWithLiteral(): Unit = {
    // amount <> 50
    val predicate1 = createCall(NOT_EQUALS, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(Some(1.0 - estimator1.defaultEqualsSelectivity.get),
      estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // [10, 200] contains 50
    assertEquals(Some(1.0 - 1.0 / 80.0), estimator2.evaluate(predicate1))

    // amount <> 5
    val predicate2 = createCall(NOT_EQUALS, createInputRef(amount_idx), createNumericLiteral(5))
    // [10, 200] does not contain 5
    assertEquals(Some(1.0), estimator2.evaluate(predicate2))
  }

  @Test
  def testComparisonWithLiteralOfStringType(): Unit = {
    // name > "abc"
    val predicate = createCall(GREATER_THAN, createInputRef(name_idx), createStringLiteral("abc"))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("name" ->
      createColumnStats(Some(800L), Some(0L), Some(16.0), Some(32), Some("aaa"), Some("max")))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    assertEquals(estimator2.defaultComparisonSelectivity, estimator2.evaluate(predicate))
  }

  @Test
  def testGreaterThanWithLiteral(): Unit = {
    // amount > 50
    val predicate1 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount > 200
    val predicate2 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(200))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount > 5
    val predicate3 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(5))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount > 10
    val predicate4 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(10))
    // partial overlap(excluding min value)
    assertEquals(Some(1.0 - 1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 > amount
    val predicate5 = createCall(GREATER_THAN, createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))

    // min or max is null
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), None, Some(200)))))
    val estimator3 = new SelectivityEstimator(mockScan(statistic1), mq)
    // no overlap
    assertEquals(Some(0.0), estimator3.evaluate(predicate2))
    // partial overlap, default value
    assertEquals(estimator3.defaultComparisonSelectivity, estimator3.evaluate(predicate3))

    // ndv is null
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator4 = new SelectivityEstimator(mockScan(statistic2), mq)
    // no overlap
    assertEquals(Some(0.0), estimator4.evaluate(predicate2))
    // complete overlap
    assertEquals(Some(1.0), estimator4.evaluate(predicate3))
    // partial overlap, default value
    assertEquals(Some((200D - 50) / (200D - 10)), estimator4.evaluate(predicate1))
  }

  @Test
  def testGreaterThanWithoutLiteral(): Unit = {
    // amount > price
    val predicate = createCall(GREATER_THAN, createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(2), Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(19)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))

    // min or max is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), None, Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), None))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate))
    val statistic5 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(11), None),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), None, Some(10)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic5), mq).evaluate(predicate))
    // partial overlap
    val statistic6 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), None),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), None, Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic6), mq).evaluate(predicate))
  }

  @Test
  def testGreaterThanOrEqualsToWithLiteral(): Unit = {
    // amount >= 50
    val predicate1 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount >= 201
    val predicate2 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(201))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount >= 10
    val predicate3 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(10))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount >= 200
    val predicate4 = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(200))
    // partial overlap(including max value)
    assertEquals(Some(1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 >= amount
    val predicate5 = createCall(GREATER_THAN_OR_EQUAL,
      createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))
  }

  @Test
  def testGreaterThanOrEqualsToWithoutLiteral(): Unit = {
    // amount >= price
    val predicate = createCall(GREATER_THAN_OR_EQUAL,
      createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(2), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(20), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(20)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))
  }

  @Test
  def testLessThanWithLiteral(): Unit = {
    // amount < 50
    val predicate1 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount < 10
    val predicate2 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(10))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount < 201
    val predicate3 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(201))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount < 200
    val predicate4 = createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(200))
    // partial overlap(excluding max value)
    assertEquals(Some(1.0 - 1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 < amount
    val predicate5 = createCall(LESS_THAN, createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))
  }

  @Test
  def testLessThanWithoutLiteral(): Unit = {
    // amount < price
    val predicate = createCall(LESS_THAN, createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(10)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(1), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(10L), Some(8.0), Some(8), Some(1), Some(19)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // partial overlap (account's nullCount is not 0)
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))

    // partial overlap
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate))
  }

  @Test
  def testLessThanOrEqualsToWithLiteral(): Unit = {
    // amount <= 50
    val predicate1 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(50))
    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    assertEquals(estimator1.defaultComparisonSelectivity, estimator1.evaluate(predicate1))

    // tests with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)

    // amount <= 9
    val predicate2 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(9))
    // no overlap
    assertEquals(Some(0.0), estimator2.evaluate(predicate2))

    // amount <= 200
    val predicate3 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(200))
    // complete overlap
    assertEquals(Some(1.0), estimator2.evaluate(predicate3))

    // partial overlap
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)), estimator2.evaluate(predicate1))

    // amount <= 10
    val predicate4 = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createNumericLiteral(10))
    // partial overlap(excluding min value)
    assertEquals(Some(1.0 / 100.0), estimator2.evaluate(predicate4))

    //  50 <= amount
    val predicate5 = createCall(LESS_THAN_OR_EQUAL,
      createNumericLiteral(50), createInputRef(amount_idx))
    // partial overlap
    assertEquals(Some((200.0 - 50.0) / (200.0 - 10.0)), estimator2.evaluate(predicate5))
  }

  @Test
  def testLessThanOrEqualsToWithoutLiteral(): Unit = {
    // amount <= price
    val predicate = createCall(LESS_THAN_OR_EQUAL,
      createInputRef(amount_idx), createInputRef(price_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // tests with statistics
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(2), Some(9)))))
    // no overlap
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))

    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(1), Some(20)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    // complete overlap
    assertEquals(Some(1.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate))

    // partial overlap
    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), Some(0L), Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), Some(0L), Some(8.0), Some(8), Some(20), Some(80)))))
    assertEquals(Some(1.0 / 3.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate))
  }

  @Test
  def testLike(): Unit = {
    // name like 'ross'
    val predicate = createCall(LIKE, createInputRef(name_idx), createStringLiteral("ross"))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultLikeSelectivity, estimator.evaluate(predicate))
  }

  @Test
  def testIsNull(): Unit = {
    // name is null
    val predicate = createCall(IS_NULL, createInputRefWithNullability(name_idx, true))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(estimator.defaultIsNullSelectivity, estimator.evaluate(predicate))

    val colStats = createColumnStats(Some(80L), Some(10L), Some(16.0), Some(32), None, None)
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map("name" -> colStats)))
    assertEquals(Some(10.0 / 100.0),
      new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate))
  }

  @Test
  def testIsNotNull(): Unit = {
    // name is not null
    val predicate = createCall(IS_NOT_NULL, createInputRef(name_idx))
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(Some(1.0), estimator.evaluate(predicate))

    val predicate2 = createCall(IS_NOT_NULL, createInputRefWithNullability(name_idx, true))
    assertEquals(estimator.defaultIsNotNullSelectivity, estimator.evaluate(predicate2))

    val colStats = createColumnStats(Some(80L), Some(10L), Some(16.0), Some(32), None, None)
    val statistic1 = createFlinkStatistic(Some(100L), Some(Map("name" -> colStats)))
    assertEquals(Some(1.0 - 10.0 / 100.0),
      new SelectivityEstimator(mockScan(statistic1), mq).evaluate(predicate2))
  }

  @Test
  def testIn(): Unit = {
    val estimator = new SelectivityEstimator(scan, mq)

    // name in ("abc", "def")
    val predicate1 = createCall(IN,
      createInputRef(name_idx),
      createStringLiteral("abc"),
      createStringLiteral("def"))
    // test with unsupported type
    assertEquals(Some(estimator.defaultEqualsSelectivity.get * 2), estimator.evaluate(predicate1))

    // tests with supported type
    val predicate2 = createCall(IN,
      createInputRef(amount_idx),
      createNumericLiteral(10.0),
      createNumericLiteral(20.0),
      createNumericLiteral(30.0),
      createNumericLiteral(40.0))
    // test without statistics
    assertEquals(Some(estimator.defaultEqualsSelectivity.get * 4), estimator.evaluate(predicate2))

    // test with statistics
    val statistic2 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(50), Some(200)))))
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic2), mq).evaluate(predicate2))

    val statistic3 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(15), Some(200)))))
    assertEquals(Some(3.0 / 80.0),
      new SelectivityEstimator(mockScan(statistic3), mq).evaluate(predicate2))

    // min or max is null
    val statistic4 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(100L), None, Some(8.0), Some(8), Some(50), None))))
    assertEquals(Some(0.0), new SelectivityEstimator(mockScan(statistic4), mq).evaluate(predicate2))

    // ndv is null
    val statistic5 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(None, None, Some(8.0), Some(8), Some(10), Some(200)))))
    assertEquals(Some(estimator.defaultEqualsSelectivity.get * 4),
      new SelectivityEstimator(mockScan(statistic5), mq).evaluate(predicate2))

    // column interval is null
    val statistic6 = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(40L), None, Some(8.0), Some(8), None, None))))
    assertEquals(Some(4.0 / 40.0),
      new SelectivityEstimator(mockScan(statistic6), mq).evaluate(predicate2))
  }

  @Test
  def testAnd(): Unit = {
    // amount <= 50 and price > 6.5
    val predicate = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(6.5))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * selectivity), estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    assertEquals(Some(((50.0 - 10.0) / (200.0 - 10.0)) * ((8.0 - 6.5) / (8.0 - 2.0))),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate))
  }

  @Test
  def testOr(): Unit = {
    // amount <= 50 or price > 6.5
    val predicate = createCall(OR,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(6.5))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * 2 - selectivity * selectivity), estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    val leftSelectivity = (50.0 - 10.0) / (200.0 - 10.0)
    val rightSelectivity = (8.0 - 6.5) / (8.0 - 2.0)
    assertEquals(Some((leftSelectivity + rightSelectivity) - (leftSelectivity * rightSelectivity)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate))

    // amount = 50 or amount = 60
    val predicate1 = createCall(OR,
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(60))
    )
    assertEquals(Some(2.0 / 80.0),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))

    // amount = 50 or amount = 60 or amount > 70
    val predicate2 = createCall(OR,
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(60)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(70))
    )
    val inSelectivity = 2.0 / 80.0
    val greaterThan70Selectivity = (200.0 - 70.0) / (200.0 - 10.0)
    assertEquals(
      Some(inSelectivity + greaterThan70Selectivity - inSelectivity * greaterThan70Selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate2))

    // amount < 50 or amount > 80
    val predicate3 = createCall(OR,
      createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(80))
    )
    val lessThan50Selectivity = (50.0 - 10.0) / (200.0 - 10.0)
    val greaterThan80Selectivity = (200.0 - 80.0) / (200.0 - 10.0)
    assertEquals(
      Some(lessThan50Selectivity + greaterThan80Selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate3))

    //  50 = amount or amount = 60 or 70 < amount or price = 5 or price < 3
    val predicate4 = createCall(OR,
      createCall(EQUALS, createNumericLiteral(50), createInputRef(amount_idx)),
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(60)),
      createCall(LESS_THAN, createNumericLiteral(70), createInputRef(amount_idx)),
      createCall(EQUALS, createInputRef(price_idx), createNumericLiteral(5)),
      createCall(LESS_THAN, createInputRef(price_idx), createNumericLiteral(3))
    )

    val inSelectivity1 = 2.0 / 80.0
    val lessThan70Selectivity = (200.0 - 70.0) / (200.0 - 10.0)
    val priceSelectivity = 1.0 / 50.0 + (3.0 - 2.0) / (8.0 - 2.0)
    assertEquals(
      Some(inSelectivity1 + lessThan70Selectivity + priceSelectivity -
        inSelectivity1 * lessThan70Selectivity * priceSelectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate4))
  }

  @Test
  def testNot(): Unit = {
    // not(amount <= 50)
    val predicate = createCall(NOT,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    assertEquals(Some(1.0 - estimator.defaultComparisonSelectivity.get),
      estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    assertEquals(Some(1.0 - (50.0 - 10.0) / (200.0 - 10.0)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate))
  }

  @Test
  def testAndOrNot(): Unit = {
    // amount <= 50 and (name = "abc" or not(price < 4.5))
    val predicate = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(OR,
        createCall(EQUALS, createInputRef(name_idx), createStringLiteral("abc")),
        createCall(NOT,
          createCall(LESS_THAN, createInputRef(price_idx), createNumericLiteral(4.5))
        )
      )
    )

    // test without statistics
    val estimator1 = new SelectivityEstimator(scan, mq)
    val accountSelectivity1 = estimator1.defaultComparisonSelectivity.get
    val nameSelectivity1 = estimator1.defaultEqualsSelectivity.get
    val notPriceSelectivity1 = 1.0 - estimator1.defaultComparisonSelectivity.get
    val selectivity1 = accountSelectivity1 * (nameSelectivity1 + notPriceSelectivity1 -
      nameSelectivity1 * notPriceSelectivity1)
    assertEquals(Some(selectivity1), estimator1.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    val estimator2 = new SelectivityEstimator(mockScan(statistic), mq)
    val accountSelectivity2 = (50.0 - 10.0) / (200.0 - 10.0)
    val nameSelectivity2 = estimator2.defaultEqualsSelectivity.get
    // not(price < 4.5) will be converted to (price >= 4.5)
    val notPriceSelectivity2 = (8 - 4.5) / (8.0 - 2.0)
    val selectivity2 = accountSelectivity2 * (nameSelectivity2 + notPriceSelectivity2 -
      nameSelectivity2 * notPriceSelectivity2)
    assertEquals(Some(selectivity2), estimator2.evaluate(predicate))
  }

  @Test
  def testPredicateWithUdf(): Unit = {
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    val estimator = new SelectivityEstimator(mockScan(statistic), mq)

    // abs(amount) <= 50
    val predicate1 = createCall(LESS_THAN,
      createCall(SqlStdOperatorTable.ABS, createInputRef(amount_idx)),
      createNumericLiteral(50))
    // with builtin udf
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate1))

    val scalarFunction = new NotBuiltInUdf()
    // NotBuiltInUdf(amount) <= 50
    val predicate2 = createCall(LESS_THAN,
      createCall(
        UserDefinedFunctionUtils.createScalarSqlFunction(
          scalarFunction.functionIdentifier,
          scalarFunction.toString,
          scalarFunction,
          typeFactory),
        createInputRef(amount_idx)),
      createNumericLiteral(50))
    // with not builtin udf
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate2))
  }

  @Test
  def testOnlyPartitionPredicate(): Unit = {
    // partition > "2015"
    val predicate = createCall(GREATER_THAN,
      createInputRef(partition_idx), createStringLiteral("2015"))
    // filter is not push down
    val estimator1 = new SelectivityEstimator(
      mockScan(isPartitionableTableSource = true, isFilterPushedDown = false),
      mq)
    assertEquals(Some(0.05), estimator1.evaluate(predicate))

    // partition in ("2014", "2015", "2016")
    val predicate2 = createCall(IN,
      createInputRef(partition_idx),
      createStringLiteral("2014"),
      createStringLiteral("2015"),
      createStringLiteral("2016"))
    assertEquals(Some(0.03), estimator1.evaluate(predicate2))

    // filter is push down
    val estimator2 = new SelectivityEstimator(
      mockScan(isPartitionableTableSource = true, isFilterPushedDown = true),
      mq)
    assertEquals(Some(1.0), estimator2.evaluate(predicate))
  }

  @Test
  def testPredicatesWithPartitionPredicate(): Unit = {
    // amount <= 50 and partition = "2015"
    val predicate = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(EQUALS, createInputRef(partition_idx), createStringLiteral("2015"))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(
      mockScan(isPartitionableTableSource = true, isFilterPushedDown = true), mq)
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    assertEquals(Some((50.0 - 10.0) / (200.0 - 10.0)),
      new SelectivityEstimator(
        mockScan(
          statistic = statistic,
          isPartitionableTableSource = true,
          isFilterPushedDown = true),
        mq).evaluate(predicate))
  }

  @Test
  def testUnsupportedPartitionPredicate(): Unit = {
    val scalarFunction = new NotBuiltInUdf()
    // amount <= 50 and NotBuiltInUdf(partition) = "2015"
    val predicate1 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(50)),
      createCall(EQUALS,
        createCall(
          UserDefinedFunctionUtils.createScalarSqlFunction(
            scalarFunction.functionIdentifier,
            scalarFunction.toString,
            scalarFunction,
            typeFactory),
          createInputRef(partition_idx)),
        createStringLiteral("2015"))
    )
    // test without statistics
    val estimator = new SelectivityEstimator(
      mockScan(isPartitionableTableSource = true, isFilterPushedDown = false), mq)
    val selectivity = estimator.defaultComparisonSelectivity.get *
      estimator.defaultEqualsSelectivity.get
    assertEquals(Some(selectivity), estimator.evaluate(predicate1))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(10), Some(200)))))
    assertEquals(Some(((50.0 - 10.0) / (200.0 - 10.0)) * estimator.defaultEqualsSelectivity.get),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))

    // partition > name
    val predicate2 = createCall(GREATER_THAN,
      createInputRef(partition_idx),
      createInputRef(name_idx))
    assertEquals(estimator.defaultComparisonSelectivity, estimator.evaluate(predicate2))
  }

  @Test
  def testSelectivityWithTableConfig(): Unit = {
    val tableConfig1 = new TableConfig()
    tableConfig1.getParameters.setDouble(TableConfig.SQL_CBO_SELECTIVITY_COMPARISON_DEFAULT, 0.05)
    tableConfig1.getParameters.setDouble(TableConfig.SQL_CBO_SELECTIVITY_EQUALS_DEFAULT, 0.015)
    tableConfig1.getParameters.setDouble(TableConfig.SQL_CBO_SELECTIVITY_ISNULL_DEFAULT, 0.015)
    tableConfig1.getParameters.setDouble(TableConfig.SQL_CBO_SELECTIVITY_DEFAULT, 0.025)
    val scan1 = mockScan(tableConfig = Some(tableConfig1))
    val estimator1 = new SelectivityEstimator(scan1, mq)
    // amount = 50
    val predicate1 = createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(50))
    assertEquals(Some(0.015), estimator1.evaluate(predicate1))
    // amount > 50
    val predicate2 = createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(50))
    assertEquals(Some(0.05), estimator1.evaluate(predicate2))
    // name is null
    val predicate3 = createCall(IS_NULL, createInputRefWithNullability(name_idx, true))
    assertEquals(Some(0.015), estimator1.evaluate(predicate3))
    // (account = 10) is true
    val predicate4 = createCall(IS_TRUE,
      createCall(EQUALS, createInputRef(amount_idx), createNumericLiteral(10))
    )
    assertEquals(Some(0.015), estimator1.evaluate(predicate4))

    // illegal selectivity value in config
    val tableConfig2 = new TableConfig()
    tableConfig2.getParameters.setDouble(TableConfig.SQL_CBO_SELECTIVITY_COMPARISON_DEFAULT, 10)
    val scan2 = mockScan(tableConfig = Some(tableConfig2))
    val estimator2 = new SelectivityEstimator(scan2, mq)
    assertEquals(Some(0.15), estimator2.evaluate(predicate1))
  }

  @Test
  def testSelectivityWithSameRexInputRefs(): Unit = {
    // amount <= 45 and amount > 40
    val predicate1 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(45)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(40))
    )
    // amount <= 45 and amount > 40 and price > 4.5 and price < 5
    val predicate2 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(45)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(40)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(4.5)),
      createCall(LESS_THAN, createInputRef(price_idx), createNumericLiteral(5))
    )

    // amount <= 45 and amount > 40 and price > 4.5 and cast(price as INTEGER) < 5
    val predicate3 = createCall(AND,
      createCall(LESS_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(45)),
      createCall(GREATER_THAN, createInputRef(amount_idx), createNumericLiteral(40)),
      createCall(GREATER_THAN, createInputRef(price_idx), createNumericLiteral(4.5)),
      createCall(LESS_THAN, createCast(createInputRef(price_idx), 1), createNumericLiteral(5))
    )

    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * selectivity), estimator.evaluate(predicate1))
    assertEquals(Some((selectivity * selectivity) * selectivity * selectivity),
      estimator.evaluate(predicate2))

    // test with statistics
    val statistic = createFlinkStatistic(Some(100L), Some(Map(
      "amount" -> createColumnStats(Some(80L), None, Some(8.0), Some(8), Some(0), Some(100)),
      "price" -> createColumnStats(Some(50L), None, Some(8.0), Some(8), Some(2), Some(8)))))
    assertEquals(Some(((45 - 40) * 1.0) / (100 - 0)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))
    assertEquals(Some((((45 - 40) * 1.0) / (100 - 0)) * ((5 - 4.5) / (8 - 2))),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate2))
    assertEquals(Some((((45 - 40) * 1.0) / (100 - 0)) * ((8 - 4.5) / (8 - 2)) * selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate3))

    // amount < 120 and amount => 80
    val predicate4 = createCall(AND,
      createCall(LESS_THAN, createInputRef(amount_idx), createNumericLiteral(120)),
      createCall(GREATER_THAN_OR_EQUAL, createInputRef(amount_idx), createNumericLiteral(80))
    )
    assertEquals(Some(((100 - 80) * 1.0) / (100 - 0)),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate4))
  }

  @Test
  def testSelectivityWithSameRexInputRefsAndStringType(): Unit = {
    // name > "abc" and name < "test"
    val predicate1 = createCall(AND,
      createCall(GREATER_THAN, createInputRef(name_idx), createStringLiteral("abc")),
      createCall(LESS_THAN, createInputRef(name_idx), createStringLiteral("test"))
    )

    // test without statistics
    val estimator = new SelectivityEstimator(scan, mq)
    val selectivity = estimator.defaultComparisonSelectivity.get
    assertEquals(Some(selectivity * selectivity), estimator.evaluate(predicate1))

    // test with statistics
    val statistic = createFlinkStatistic(Some(1000L), Some(Map("name" ->
      createColumnStats(Some(800L), Some(0L), Some(16.0), Some(32), Some("aaa"), Some("max")))))
    assertEquals(Some(selectivity * selectivity),
      new SelectivityEstimator(mockScan(statistic), mq).evaluate(predicate1))
  }

  private class TestPartitionableTableSource(filterPushedDown: Boolean)
    extends PartitionableTableSource with TableSource {

    override def getAllPartitions: util.List[Partition] = ???

    override def getPrunedPartitions: util.List[Partition] = ???

    override def getPartitionFieldNames: Array[String] = Array("partition")

    override def getPartitionFieldTypes: Array[TypeInformation[_]] = ???

    override def isPartitionPruned: Boolean = ???

    override def applyPrunedPartitionsAndPredicate(
      partitionPruned: Boolean,
      prunedPartitions: util.List[Partition],
      predicates: util.List[Expression]): TableSource = ???

    override def getReturnType: DataType =
      new RowSchema(relDataType).dataType

    override def isFilterPushedDown: Boolean = filterPushedDown

    override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType)
  }

}

object SelectivityEstimatorTest {

  @BeforeClass
  def beforeAll(): Unit = {
    RelMetadataQuery
      .THREAD_PROVIDERS
      .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE))
  }

}

private class NotBuiltInUdf extends ScalarFunction {
  def eval(v: String): String = v

  def eval(v: Int): Int = v
}
