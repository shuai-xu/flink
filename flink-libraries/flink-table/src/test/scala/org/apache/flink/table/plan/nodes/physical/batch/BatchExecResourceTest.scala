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

package org.apache.flink.table.plan.nodes.physical.batch

import java.util.{Arrays => JArrays, Collection => JCollection}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.RowCsvInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction
import org.apache.flink.streaming.api.operators.StreamSource
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecResourceTest.MockTableSource
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.sources.{BatchExecTableSource, LimitableTableSource, TableSource}
import org.apache.flink.table.util.BatchExecResourceUtil
import org.apache.flink.table.util.TableTestBatchExecBase
import org.apache.flink.table.tpc.{STATS_MODE, Schema, TpcHSchemaProvider, TpchTableStatsProvider}
import org.apache.flink.table.types.{DataType, DataTypes, InternalType}
import org.apache.flink.types.Row
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import _root_.scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class BatchExecResourceTest(inferGranularity: String) extends TableTestBatchExecBase {

  private val util = batchExecTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.setSubsectionOptimization(false)
    util.getTableEnv.getConfig.getParameters.setString(
      TableConfig.SQL_EXEC_INFER_RESOURCE_GRANULARITY,
      inferGranularity
    )
    util.addTable[(Int, Long, String)]("SmallTable3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)
    BatchExecResourceTest.setResourceConfig(util.getTableEnv.getConfig)
  }

  @Test
  def testSourcePartitionMaxNum(): Unit = {
    util.getTableEnv.getConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_SOURCE_MAX_PARALLELISM,
      300
    )
    val sqlQuery = "SELECT * FROM SmallTable3"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testSortLimit(): Unit = {
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testRangePartition(): Unit ={
    util.getTableEnv.getConfig.getParameters.setBoolean(
      TableConfig.SQL_EXEC_SORT_ENABLE_RANGE,
      true)
    val sqlQuery = "SELECT * FROM Table5 where d < 100 order by e"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testUnionQuery(): Unit = {
    val table3Schema = new Schema() {
      override def getFieldNames: Array[String] = {
        Array("a", "b", "c")
      }

      override def getFieldTypes: Array[InternalType] = {
        Array(
          DataTypes.INT,
          DataTypes.LONG,
          DataTypes.STRING)
      }

      override def getFieldNullables: Array[Boolean] = {
        getFieldTypes.map(_ => false)
      }
    }
    val colStatsOfTable3 = TableStats(100L, Map[java.lang.String, ColumnStats](
      "a" -> ColumnStats(3L, 1L, 10000D * BatchExecResourceUtil.SIZE_IN_MB, 8, 5, -5),
      "b" -> ColumnStats(5L, 0L, 10000D * BatchExecResourceUtil.SIZE_IN_MB, 32, 6.1D, 0D),
      "c" -> ColumnStats(5L, 0L, 10000D * BatchExecResourceUtil.SIZE_IN_MB, 32, 6.1D, 0D)))
    val table3 = new MockTableSource(table3Schema, colStatsOfTable3)
    util.addTable("Table3", table3)

    val sqlQuery = "SELECT sum(a) as sum_a, g FROM " +
        "(SELECT a, b, c FROM SmallTable3 UNION ALL SELECT a, b, c FROM Table3), Table5 " +
        "WHERE b = e group by g"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testSubsectionOptimization(): Unit = {
    util.getTableEnv.getConfig.setSubsectionOptimization(true)
    val query = "SELECT SUM(a) AS sum_a, c FROM SmallTable3 GROUP BY c "
    val table = util.getTableEnv.sqlQuery(query)
    val result1 = table.select('sum_a.sum as 'total_sum)
    val result2 = table.select('sum_a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyResultPartitionCount()
  }

  @Test
  def testAggregateWithJoin(): Unit = {
    val customerSchema = TpcHSchemaProvider.getSchema("customer")
    val colStatsOfCustomer =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("customer")
    val customer = new MockTableSource(customerSchema, colStatsOfCustomer.get)
    util.addTable("customer", customer)

    val ordersSchema = TpcHSchemaProvider.getSchema("orders")
    val colStastOfOrders =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("orders")
    val orders = new MockTableSource(ordersSchema, colStastOfOrders.get)
    util.addTable("orders", orders)
    val lineitemSchema = TpcHSchemaProvider.getSchema("lineitem")
    val colStatsOfLineitem =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("lineitem")
    val lineitem = new MockTableSource(lineitemSchema, colStatsOfLineitem.get)
    util.addTable("lineitem", lineitem)

    val sqlQuery = "select c.c_name, sum(l.l_quantity)" +
        " from customer c, orders o, lineitem l" +
        " where o.o_orderkey in ( " +
        " select l_orderkey from lineitem group by l_orderkey having" +
        " sum(l_quantity) > 300)" +
        " and c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey" +
        " group by c.c_name"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testLimitPushDown(): Unit = {
    val lineitemSchema = TpcHSchemaProvider.getSchema("lineitem")
    val colStatsOfLineitem =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("lineitem")
    val lineitem = new MockTableSource(lineitemSchema, colStatsOfLineitem.get)
    util.addTable("lineitem", lineitem)

    val sqlQuery = "select * from lineitem limit 1"
    util.verifyResource(sqlQuery)
  }

}

object BatchExecResourceTest {

  @Parameterized.Parameters(name = "{0}")
  def parameters(): JCollection[String] = {
    JArrays.asList("NONE", "SOURCE", "ALL")
  }

  def setResourceConfig(tableConfig: TableConfig): Unit = {
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_DEFAULT_PARALLELISM,
      18)
    tableConfig.getParameters.setLong(
      TableConfig.SQL_EXEC_REL_PROCESS_ROWS_PER_PARTITION,
      2L)
    tableConfig.getParameters.setLong(
      TableConfig.SQL_EXEC_SOURCE_PROCESS_SIZE_PER_PARTITION,
      50000)
    tableConfig.getParameters.setLong(
      TableConfig.SQL_EXEC_SOURCE_MAX_PARALLELISM,
      1000)
    tableConfig.getParameters.setLong(
      TableConfig.SQL_EXEC_INFER_REL_MAX_PARALLELISM,
      800)
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_INFER_MIN_PARALLELISM,
      20
    )
    tableConfig.getParameters.setDouble(
      TableConfig.SQL_EXEC_DEFAULT_CPU,
      0.3
    )
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_SOURCE_MEM,
      52
    )
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_DEFAULT_MEM,
      46
    )
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM,
      33
    )
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_HASH_AGG_TABLE_PREFER_MEM,
      37
    )
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_HASH_JOIN_TABLE_MEM,
      43
    )
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_HASH_JOIN_TABLE_PREFER_MEM,
      47
    )
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_SORT_BUFFER_MEM,
      53
    )
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_SORT_PREFER_BUFFER_MEM,
      57
    )
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_REL_PROCESS_ROWS_PER_PARTITION,
      1000000
    )
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_INFER_RESERVE_RELATIVE_PREFER_MEM_RATIO,
      2
    )
    tableConfig.getParameters.setInteger(
      TableConfig.SQL_EXEC_INFER_MANAGER_MAX_MEM,
      470
    )
    tableConfig.getParameters.setInteger(
      BatchExecResourceUtil.SQL_EXEC_INFER_MIN_MEMORY,
      32
    )
  }

  class MockTableSource(
      schema: Schema,
      stats: TableStats) extends BatchExecTableSource[Row] with LimitableTableSource {
    var isLimitPushdown = false

    override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[Row] = {
      val inputFormat = new RowCsvInputFormat(
        new Path("/tmp/tmp"),
        schema.getFieldTypes.map(DataTypes.toTypeInfo),
        "\n",
        ",")
      // cause the streamEnv is mocked, will new DataStreamSource directly.
      val typeInformation = DataTypes.toTypeInfo(getReturnType).asInstanceOf[TypeInformation[Row]]
      val sourceFunction: InputFormatSourceFunction[Row] =
        new InputFormatSourceFunction(inputFormat, typeInformation)
      val sourceOperator: StreamSource[Row, InputFormatSourceFunction[Row]] =
        new StreamSource(sourceFunction)
      new DataStreamSource[Row](streamEnv, typeInformation, sourceOperator, true, "source")
    }

    override def applyLimit(limit: Long): TableSource = {
      isLimitPushdown = true
      this
    }

    override def isLimitPushedDown: Boolean = {
      isLimitPushdown
    }

    override def getReturnType: DataType = DataTypes.createRowType(
      schema.getFieldTypes.asInstanceOf[Array[DataType]], schema.getFieldNames)

    override def getTableStats: TableStats = stats
  }
}
