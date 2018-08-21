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

package org.apache.flink.table.api.stream.table

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.optimize.FlinkStreamPrograms
import org.apache.flink.table.runtime.utils.{CommonTestData, StreamTestData}
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.util.{TableTestBase, TestFilterableTableSource, TestFlinkLogicalLastRowRule, TestTableSourceWithUniqueKeys}
import org.apache.flink.types.Row
import org.junit.{Assert, Test}

class TableSourceTest extends TableTestBase {

  @Test
  def testStreamProjectableSourceScanPlanTableApi(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
        .scan(tableName)
        .select('last, 'id.floor(), 'score * 2)

    util.verifyPlan(result)
  }

  @Test
  def testStreamProjectableSourceScanPlanSQL(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val sqlQuery = s"SELECT `last`, floor(id), score * 2 FROM $tableName"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testStreamProjectableSourceScanNoIdentityCalc(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
        .scan(tableName)
        .select('id, 'score, 'first)

    util.verifyPlan(result)
  }

  @Test
  def testStreamFilterableSourceScanPlan(): Unit = {
    val tableSource = new TestFilterableTableSource
    val tableName = "filterableTable"
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where("amount > 2 && price * 2 < 32")

    util.verifyPlan(result)
  }

  @Test
  def testStreamFilterableSourceWithTrim(): Unit = {
    val tableSource = new TestFilterableTableSource
    val tableName = "filterableTable"
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
      .scan(tableName)
      .where('name.trim() === "Test" ||
        'name.trim(removeLeading = false) === "Test " ||
        'name.trim(removeTrailing = false) === " Test" ||
        'name.trim(character = "   ") === "Test")

    util.verifyPlan(result)
  }

  @Test
  def testTableSourceWithLongRowTimeField(): Unit = {
    val tableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime"
    )

    val util = streamTestUtil()
    util.tableEnv.registerTableSource("rowTimeT", tableSource)

    val t = util.tableEnv.scan("rowTimeT").select("rowtime, id, name, val")

    util.verifyPlan(t)
  }

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {
    val tableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime"
    )

    val util = streamTestUtil()
    util.tableEnv.registerTableSource("rowTimeT", tableSource)

    val t = util.tableEnv.scan("rowTimeT").select("rowtime, id, name, val")

    util.verifyPlan(t)
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {
    val tableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime"
    )

    val util = streamTestUtil()
    util.tableEnv.registerTableSource("rowTimeT", tableSource)

    val t = util.tableEnv.scan("rowTimeT")
        .filter("val > 100")
        .window(Tumble over 10.minutes on 'rowtime as 'w)
        .groupBy('name, 'w)
        .select('name, 'w.end, 'val.avg)

    util.verifyPlan(t)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.registerTableSource("procTimeT", new TestProctimeSource("pTime"))

    val t = util.tableEnv.scan("procTimeT").select("pTime, id, name, val")

    util.verifyPlan(t)
  }

  @Test
  def testProcTimeTableSourceOverWindow(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.registerTableSource("procTimeT", new TestProctimeSource("pTime"))

    val t = util.tableEnv.scan("procTimeT")
        .window(Over partitionBy 'id orderBy 'pTime preceding 2.hours as 'w)
        .select('id, 'name, 'val.sum over 'w as 'valSum)
        .filter('valSum > 100)

    util.verifyPlan(t)
  }

  @Test
  def testProjectableProcTimeTableSource(): Unit = {
    // ensures that projection is not pushed into table source with proctime indicators
    val util = streamTestUtil()

    val projectableTableSource = new TestProctimeSource("pTime") with ProjectableTableSource {
      override def projectFields(fields: Array[Int]): TableSource = {
        // ensure this method is not called!
        Assert.fail()
        null.asInstanceOf[TableSource]
      }
    }
    util.tableEnv.registerTableSource("PTimeTable", projectableTableSource)

    val t = util.tableEnv.scan("PTimeTable")
        .select('name, 'val)
        .where('val > 10)

    util.verifyPlan(t)
  }

  @Test
  def testProjectableRowTimeTableSource(): Unit = {
    // ensures that projection is not pushed into table source with rowtime indicators
    val util = streamTestUtil()

    val projectableTableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime") with ProjectableTableSource {

      override def projectFields(fields: Array[Int]): TableSource = {
        // ensure this method is not called!
        Assert.fail()
        null.asInstanceOf[TableSource]
      }
    }
    util.tableEnv.registerTableSource("RTimeTable", projectableTableSource)

    val t = util.tableEnv.scan("RTimeTable")
        .select('name, 'val)
        .where('val > 10)

    util.verifyPlan(t)
  }


  @Test
  def testWithPkMultiRowUpdateTransposWithCalc(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.TOPN,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
      .groupBy('pk)
      .select('pk, 'a.count)

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testWithPkMultiRowUpdate(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.TOPN,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
      .groupBy('pk)
      .select('pk, 'a.count, 'c.count)

    util.verifyPlanAndTrait(resultTable)
  }

  def csvTable: (CsvTableSource, String) = {
    val csvTable = CommonTestData.getCsvTableSource
    val tableName = "csvTable"
    (csvTable, tableName)
  }
}

class TestRowtimeSource(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    rowtimeField: String)
    extends StreamTableSource[Row] with DefinedRowtimeAttribute {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getRowtimeAttribute: String = rowtimeField

  override def getReturnType: DataType = {
    DataTypes.of(new RowTypeInfo(fieldTypes, fieldNames))
  }
}

class TestProctimeSource(timeField: String)
    extends StreamTableSource[Row] with DefinedProctimeAttribute {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getProctimeAttribute: String = timeField

  override def getReturnType: DataType = {
    DataTypes.of(
      new RowTypeInfo(
        Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        Array("id", "val", "name")))
  }
}


