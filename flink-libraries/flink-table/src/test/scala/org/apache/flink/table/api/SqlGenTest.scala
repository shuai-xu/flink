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

package org.apache.flink.table.api

import _root_.java.util

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.runtime.utils.TestingAppendSink
import org.apache.flink.table.sinks.{AppendStreamTableSink, TableSink}
import org.apache.flink.table.sources.{IndexKey, _}
import org.apache.flink.table.sqlgen._
import org.apache.flink.table.types._
import org.apache.flink.table.util.TableTestBase
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.junit.Test

import _root_.scala.collection.JavaConverters._

class SqlGenTest extends TableTestBase {

  @Test
  def testUDTFAS(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(
      Array("a", "b", "c"),
      Array(Types.INT, Types.STRING, Types.INT)
    )

    tEnv.registerTableSource("source1", tableSource1)
    val t1 = tEnv.scan("source1")
    val tf = new TestTableFunction
    t1.leftOuterJoin(tf('b)).writeToSink(new AppendSink)

    tEnv.getSqlText()
  }

  @Test
  def testMultiSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(
      Array("a", "b", "c"),
      Array(Types.INT, Types.STRING, Types.INT)
    )

    tEnv.registerTableSource("source1", tableSource1)
    val t1 = tEnv.scan("source1")

    t1.writeToSink(new AppendSink)
    t1.select('a + 1 as 'a, 'b, 'c).writeToSink(new AppendSink)

    tEnv.getSqlText()
  }

  @Test
  def testNestedJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(
      Array("a", "b", "c"),
      Array(Types.INT, Types.STRING, Types.INT)
    )

    tEnv.registerTableSource("source1", tableSource1)
    val t1 = tEnv.scan("source1")

    val nestedTable1 = new HBaseDimensionTableSource(Array("c1", "c2", "c3"))
    tEnv.registerTableSource("nestedTableSource1", nestedTable1)

    val nestedTable2 = new HBaseDimensionTableSource(Array("c4", "c5", "c6"))
    tEnv.registerTableSource("nestedTableSource2", nestedTable2)

    val nestedTableSource1 = tEnv.scan("nestedTableSource1")
    val nestedTableSource2 = tEnv.scan("nestedTableSource2")
    t1.join(nestedTableSource1, 'c1 === 'b)
        .join(nestedTableSource2, 'c4 === 'b)
        .writeToSink(new AppendSink)
    tEnv.getSqlText()
  }

  @Test
  def testUDF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(
      Array("a", "b", "c"),
      Array(Types.INT, Types.STRING, Types.INT)
    )

    tEnv.registerTableSource("source1", tableSource1)
    val t1 = tEnv.scan("source1")

    val udf = new TestUDF
    t1.select(udf(10L), 'b, udf('c, "123"), SingletonUDF('c))
        .writeToSink(new AppendSink)

    tEnv.getSqlText()
  }

  @Test
  def test(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(
      Array("a", "b", "c"), Array(Types.INT, Types.STRING, Types.INT))

    val tableSource2 = new SimpleTableSource(
      Array("d", "e", "f"), Array(Types.INT, Types.STRING, Types.INT))

    val tableSource3 = new SimpleTableSource(
      Array("x", "y", "z"), Array(Types.INT, Types.STRING, Types.INT))

    val dimSource = new SimpleDimensionTableSource(temporal = true)

    tEnv.registerTableSource("source1", tableSource1)
    tEnv.registerTableSource("source2", tableSource2)
    tEnv.registerTableSource("source3", tableSource3)
    tEnv.registerTableSource("dimTable1", dimSource)

    val t1 = tEnv.scan("source1")
    val t2 = tEnv.scan("source2")
    val t3 = tEnv.scan("source3")

    val dimTable1 = tEnv.scan("dimTable1")

    val tf = new TestTableFunction
    val udf = new TestUDF
    t1.unionAll(t2)
        .leftOuterJoin(tf('b) as 'd)
        .select('a + 1 as 'a, 'b, 'c, 'd)
        .join(t3, 'a === 'x)
        .where("(a > 0 && c != -1) && (b = 'x' || b = 'y')")
        .groupBy('a, 'x, 'y, 'z)
        .select('a as 'a, 'b.count as 'b, 'c.sum as 'c, 'd.count as 'd)
        .join(dimTable1, 'b === 'dd)
        .select(udf('a), 'b, 'c, 'd, 'dd, 'ff)
        .writeToSink(new AppendSink)

    tEnv.getSqlText()
  }


  @Test
  def testOverWindow1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(Array("a", "b", "c"), Array(Types.INT, Types.STRING,
      Types.INT))
    tEnv.registerTableSource("source1", tableSource1)

    val t1 = tEnv.scan("source1")

    t1.select('a, 'b, 'c)
        .window(Over partitionBy 'a orderBy proctime() preceding UNBOUNDED_ROW following
            CURRENT_ROW as 'w)
        .select('a, 'b.count over 'w as 'b)
        .writeToSink(new AppendSink)

    tEnv.getSqlText()
  }

  @Test
  def testOverWindow2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource1 = new SimpleTableSource(Array("a", "b", "c"), Array(Types.INT, Types.STRING,
      Types.INT))
    tEnv.registerTableSource("source1", tableSource1)

    val t1 = tEnv.scan("source1")

    t1.select('a, 'b, 'c)
        .window(Over partitionBy 'a orderBy proctime() preceding 3.seconds following
            2.seconds as 'w)
        .select('a, 'b.count over 'w as 'b)
        .writeToSink(new AppendSink)

    tEnv.getSqlText()
  }

  @Test
  def testProctimeTumblingWindowAgg(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableConfig = new TableConfig
    tableConfig.setSubsectionOptimization(true)
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

    val tableSource = new SimpleTimeAttrTableSource(
      Array("a", "b", "c"),
      Array(Types.INT, Types.STRING, Types.INT),
      "proctime"
    )
    tEnv.registerTableSource("source", tableSource)
    val table = tEnv.scan("source")

    val agg = new TestUDAGG

    val res = table
        .window(Tumble over 2.minutes on 'proctime as 'w)
        .groupBy('w, 'b)
        .select('a.sum, 'b.count, agg('c), 'w.start)
        .writeToSink(new AppendSink)

    tEnv.getSqlText()
  }

}

object SingletonUDF extends ScalarFunction {
  def eval(x: Int) = x
}

class TestTableFunction extends TableFunction[String] {
  def eval(x: String) = collect(x)
  def explain(): String = "1+1=2\n3+3"
}

class TestUDF extends ScalarFunction {
  def eval(x: Long) = x * x

  def eval(x: Int) = x * x

  def eval(x: Int, y: String) = x
}

case class Acc(var x: Int)

class TestUDAGG extends AggregateFunction[Int, Acc] {

  override def createAccumulator(): Acc = Acc(0)

  override def getValue(accumulator: Acc): Int = accumulator.x

  def accumulate(acc: Acc, value: Int): Unit = {
    acc.x = acc.x + value
  }
}

class SimpleTableSource(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]])
    extends StreamTableSource[Row] with ConnectorSource[Row] {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getReturnType: DataType = {
    DataTypes.of(new RowTypeInfo(fieldTypes, fieldNames))
  }

  override def getSourceBuilder: SourceBuilder = new SourceBuilder {
    override def getProperties: util.Map[String, AnyRef] = Map[String, AnyRef](
      "attr1" -> "attr1", "attr2" -> "attr2", "setp" -> "\u0001"
    ).asJava

    override def getType: String = "simpleTableSource"

    override def getSourceCollector: AnyRef = null

    override def build(): StreamTableSource[Row] = null

    override def getPrimaryKeys: Array[String] = Array("pk")
  }
}

class SimpleTimeAttrTableSource(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    proctimeAttr: String)
    extends StreamTableSource[Row] with DefinedProctimeAttribute
        with ConnectorSource[Row] {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getReturnType: DataType = {
    DataTypes.of(new RowTypeInfo(fieldTypes, fieldNames))
  }

  override def getProctimeAttribute: String = proctimeAttr

  override def getSourceBuilder: SourceBuilder = new SourceBuilder {
    override def getProperties: util.Map[String, AnyRef] = Map[String, AnyRef](
      "attr1" -> "attr1", "attr2" -> "attr2"
    ).asJava

    override def getType: String = "SimpleTimeAttrTableSource"

    override def getSourceCollector: AnyRef = null

    override def build(): StreamTableSource[Row] = null

    override def getPrimaryKeys: Array[String] = Array("pk")
  }
}

class SimpleDimensionTableSource(temporal: Boolean)
  extends ConnectorDimSource[BaseRow] {

  override def getIndexes: util.Collection[IndexKey] =
    util.Collections.singleton(IndexKey.of(true, 0))

  override def isTemporal: Boolean = temporal

  override def getLookupFunction(index: IndexKey): FlatMapFunction[BaseRow, BaseRow] =
    new FlatMapFunction[BaseRow, BaseRow] {
      override def flatMap(
          t: BaseRow,
          collector: Collector[BaseRow]): Unit = {}
    }

  override def isAsync: Boolean = false

  override def getAsyncConfig: AsyncConfig = null

  override def getAsyncLookupFunction(
      index: IndexKey): AsyncFunction[BaseRow, BaseRow] = null

  override def getReturnType: DataType =
    DataTypes.createBaseRowType(
        Array(DataTypes.LONG, DataTypes.STRING),
        Array("dd", "ff"))

  override def getDimSourceBuilder: DimSourceBuilder[_] = new DimSourceBuilder[BaseRow] {
    override def getProperties: util.Map[String, AnyRef] = Map[String, AnyRef](
      "attr1" -> "attr1", "attr2" -> "attr2"
    ).asJava

    override def getType: String = "SimpleDimensionTableSource"

    override def build(): DimensionTableSource[BaseRow] = ???

    override def getPrimaryKeys: Array[String] = Array("pk")
  }
}

class AppendSink extends AppendStreamTableSink[Row] with ConnectorSink[Row] {
  var sink = new TestingAppendSink
  var fNames: Array[String] = _
  var fTypes: Array[DataType] = _


  override def emitDataStream(s: DataStream[Row]): Unit = {
    s.addSink(sink).name("TestAppendSink").setParallelism(s.getParallelism)
  }

  override def getOutputType: DataType = DataTypes.createRowType(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[DataType] = fTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[DataType]): TableSink[Row] = {
    val copy = new AppendSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy.sink = sink
    copy
  }

  def getAndClearValues: List[String] = sink.getAppendResults

  override def getSinkBuilder: SinkBuilder[_] = new SinkBuilder[Row] {
    override def getProperties: util.Map[String, AnyRef] = Map[String, AnyRef](
      "attr1" -> "attr1", "async" -> "true"
    ).asJava

    override def getType: String = "AppendSink"

    override def getConverter: AnyRef = null

    override def build(): TableSink[Row] = null

    override def getPrimaryKeys: Array[String] = Array("pk")
  }
}

class HBaseDimensionTableSource(fields: Array[String]) extends StreamTableSource[Row] with
    Serializable {
  override def getReturnType: DataType =
    DataTypes.of(
      new RowTypeInfo(
        fields.map(_ => Types.STRING).toArray[TypeInformation[_]],
        fields
      )
    )

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???
}
