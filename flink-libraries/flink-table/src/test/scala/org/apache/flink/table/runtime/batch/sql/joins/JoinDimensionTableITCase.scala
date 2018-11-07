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
package org.apache.flink.table.runtime.batch.sql.joins

import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{Consumer, Supplier}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.sources.{AsyncConfig, BatchTableSource, DimensionTableSource, IndexKey}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.junit.{Before, Ignore, Test}

class JoinDimensionTableITCase extends QueryTest {

  val data = List(
    QueryTest.row(1, 12L, "Julian"),
    QueryTest.row(2, 15L, "Hello"),
    QueryTest.row(3, 15L, "Fabian"),
    QueryTest.row(8, 11L, "Hello world"),
    QueryTest.row(9, 12L, "Hello world!"))

  val typeInfo = new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)

  @Before
  def setup() {
    tEnv.registerCollection("T", data, typeInfo, 'id, 'len, 'content)
    val dim = new TestDimensionTableSource
    tEnv.registerTableSource("csvdim", dim)
    TestFetcher.resetCounter();
  }

  @Test
  def testJoinDimensionTable(): Unit = {
    val dim = new TestDimensionTableSource(false, false)
    tEnv.registerTableSource("csvdim2", dim)

    var sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim2 AS D ON T.id = D.id"

    var expected = Seq(
      QueryTest.row(1, 12, "Julian", "Julian"),
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false);

    sql = "SELECT T.id, T.len, T.content, D.name FROM T LEFT JOIN csvdim2 AS D ON T.id = D.id"
    expected = Seq(
      QueryTest.row(1, 12, "Julian", "Julian"),
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"),
      QueryTest.row(8, 11L, "Hello world", null),
      QueryTest.row(9, 12L, "Hello world!", null))
    checkResult(sql, expected, false);

    assertTrue(TestFetcher.fetchByMapCount > 0 && TestFetcher.fetchByGetCount == 0)
  }

  /**
   * Dimension table with small row count will be joined by scanning
   */
  @Test
  @Ignore
  def testJoinCombinedDimensionTableWithSmallRowCount(): Unit = {
    //TODO: Should be enable when scannable dimension table is enabled
    val dim = new TestDimensionTableSourceCombinedWithBatchTableSource(false, false, 1)
    tEnv.registerTableSource("csvdim2", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim2 AS D ON T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian", "Julian"),
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false)
    assertTrue(TestFetcher.fetchByMapCount == 0 && TestFetcher.fetchByGetCount > 0)
  }

  /**
   * Dimension table with large row count will be joined by random fetching
   */
  @Test
  def testJoinCombinedDimensionTableWithLargeRowCount(): Unit = {
    val dim = new TestDimensionTableSourceCombinedWithBatchTableSource(false, false, Long.MaxValue)
    tEnv.registerTableSource("csvdim2", dim)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim2 AS D ON T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian", "Julian"),
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false);
    assertTrue(TestFetcher.fetchByMapCount > 0 && TestFetcher.fetchByGetCount == 0)
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian", "Julian"),
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false);
  }


  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val expected = Seq(
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false);
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      QueryTest.row(2, 15, "Hello", "Jark", 22),
      QueryTest.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected, false);
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian"),
      QueryTest.row(3, 15, "Fabian"))
    checkResult(sql, expected, false);
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.content = D.name AND T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian"),
      QueryTest.row(3, 15, "Fabian"))
    checkResult(sql, expected, false);
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian", 11),
      QueryTest.row(2, 15, "Jark", 22),
      QueryTest.row(3, 15, "Fabian", 33),
      QueryTest.row(8, 11, null, null),
      QueryTest.row(9, 12, null, null))
    checkResult(sql, expected, false);
  }

  @Test
  def testAsyncJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian", "Julian"),
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false);
  }

  @Test
  def testAsyncJoinTemporalTableWithPushDown(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val expected = Seq(
      QueryTest.row(2, 15, "Hello", "Jark"),
      QueryTest.row(3, 15, "Fabian", "Fabian"))
    checkResult(sql, expected, false);
  }

  @Test
  def testAsyncJoinTemporalTableWithNonEqualFilter(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val expected = Seq(
      QueryTest.row(2, 15, "Hello", "Jark", 22),
      QueryTest.row(3, 15, "Fabian", "Fabian", 33))
    checkResult(sql, expected, false);
  }

  @Test
  def testAsyncLeftJoinTemporalTableWithLocalPredicate(): Unit = {
    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T LEFT JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id " +
        "AND T.len > 1 AND D.age > 20 AND D.name = 'Fabian' " +
        "WHERE T.id > 1"

    val expected = Seq(
      QueryTest.row(2, 15, "Hello", null, null),
      QueryTest.row(3, 15, "Fabian", "Fabian", 33),
      QueryTest.row(8, 11, "Hello world", null, null),
      QueryTest.row(9, 12, "Hello world!", null, null))
    checkResult(sql, expected, false);
  }

  @Test
  def testAsyncJoinTemporalTableOnMultiFields(): Unit = {
    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian"),
      QueryTest.row(3, 15, "Fabian"))
    checkResult(sql, expected, false);
  }

  @Test
  def testAsyncLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvdim " +
        "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val expected = Seq(
      QueryTest.row(1, 12, "Julian", 11),
      QueryTest.row(2, 15, "Jark", 22),
      QueryTest.row(3, 15, "Fabian", 33),
      QueryTest.row(8, 11, null, null),
      QueryTest.row(9, 12, null, null))
    checkResult(sql, expected, false);
  }

  class TestDimensionTableSource2(async: Boolean = false)
      extends TestDimensionTableSource(async) {

    override def getIndexes: util.Collection[IndexKey] = {
      Collections.singleton(IndexKey.of(true, 1, 2))   // primary key(id, name)
    }

    override def getLookupFunction(index: IndexKey): TestDoubleKeyFetcher = {
      fetcher = new TestDoubleKeyFetcher(0, 1) // new key idx mapping to keysRow
      fetcher
    }

    override def getAsyncLookupFunction(index: IndexKey): TestAsyncDoubleKeyFetcher = {
      asyncFetcher = new TestAsyncDoubleKeyFetcher(0, 1) // new idx mapping to keysRow
      asyncFetcher
    }

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType)
  }

  class TestDimensionTableSource(
      async: Boolean = false, temporal: Boolean = true) extends DimensionTableSource[BaseRow] {
    var fetcher: TestDoubleKeyFetcher = null
    var asyncFetcher: TestAsyncDoubleKeyFetcher = null

    val rowType = new BaseRowTypeInfo(
      classOf[BaseRow],
      Array(Types.INT, Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array( "age", "id", "name"))

    override def getReturnType: DataType = DataTypes.internal(rowType)

    override def getLookupFunction(index: IndexKey): TestDoubleKeyFetcher = {
      fetcher = new TestSingleKeyFetcher(0)
      fetcher
    }

    override def isTemporal: Boolean = temporal

    override def isAsync: Boolean = async

    override def getAsyncConfig: AsyncConfig = {
      val conf = new AsyncConfig
      conf.setTimeoutMs(10000)
      conf
    }

    override def getAsyncLookupFunction(index: IndexKey): TestAsyncDoubleKeyFetcher = {
      asyncFetcher = new TestAsyncSingleKeyFetcher(0)
      asyncFetcher
    }

    override def getIndexes: util.Collection[IndexKey] = {
      Collections.singleton(IndexKey.of(true, 1)) // primary key(id)
    }

    def getFetcherResourceCount(): Int = {
      if (async && null != asyncFetcher) {
        asyncFetcher.resourceCounter
      } else if (null != fetcher) {
        fetcher.resourceCounter
      } else {
        0
      }
    }

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType)
  }

  class TestDimensionTableSourceCombinedWithBatchTableSource(
      async: Boolean = false,
      temporal: Boolean = true,
      rowCount: Long = 1) extends DimensionTableSource[BaseRow] with BatchTableSource[BaseRow] {

    var fetcher: TestDoubleKeyFetcher = null
    var asyncFetcher: TestAsyncDoubleKeyFetcher = null

    val rowType: TypeInformation[BaseRow] = new BaseRowTypeInfo(
      classOf[BaseRow],
      Array(Types.INT, Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array( "age", "id", "name"))

    override def getReturnType: DataType = DataTypes.internal(rowType)

    override def getLookupFunction(index: IndexKey): TestDoubleKeyFetcher = {
      fetcher = new TestSingleKeyFetcher(0)
      fetcher
    }

    override def isTemporal: Boolean = temporal

    override def isAsync: Boolean = async

    override def getAsyncConfig: AsyncConfig = {
      val conf = new AsyncConfig
      conf.setTimeoutMs(10000)
      conf
    }

    override def getAsyncLookupFunction(index: IndexKey): TestAsyncDoubleKeyFetcher = {
      asyncFetcher = new TestAsyncSingleKeyFetcher(0)
      asyncFetcher
    }

    override def getIndexes: util.Collection[IndexKey] = {
      Collections.singleton(IndexKey.of(true, 1)) // primary key(id)
    }

    def getFetcherResourceCount(): Int = {
      if (async && null != asyncFetcher) {
        asyncFetcher.resourceCounter
      } else if (null != fetcher) {
        fetcher.resourceCounter
      } else {
        0
      }
    }

    /**
     * Returns the data of the table as a [[org.apache.flink.streaming.api.datastream.DataStream]].
     *
     * NOTE: This method is for internal use only for defining a
     * [[org.apache.flink.table.sources.TableSource]].
     * Do not use it in Table API programs.
     */
    override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
      val mockKey = IndexKey.of(true, 1)
      val fetcher = getLookupFunction(mockKey)
      val sourceFunction = new TestSourceFunction(fetcher.asInstanceOf[TestSingleKeyFetcher])
      val streamSource = streamEnv.addSource(sourceFunction, "test source", rowType)
        .setParallelism(1)
      streamSource.getTransformation.setParallelismLocked(true)
      streamSource
    }

    override def getTableStats: TableStats = new TableStats(
      rowCount, new util.HashMap[String, ColumnStats]())

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = TableSchema.fromDataType(getReturnType)
  }
}


class TestSourceFunction(fetcher: TestSingleKeyFetcher) extends SourceFunction[BaseRow] {
  override def cancel(): Unit = {}

  override def run(sourceContext: SourceContext[BaseRow]): Unit = {
    fetcher.getAllKeys.foreach((key: Int) => sourceContext.collect(fetcher.get(key)))
  }
}

// lookup data table using id index
class TestSingleKeyFetcher(idIndex: Int) extends TestDoubleKeyFetcher(idIndex, 1) {

  override def flatMap(keysRow: BaseRow, collector: Collector[BaseRow]): Unit = {
    val key = keysRow.getInt(idIndex)
    val value = TestTable.singleKeyTable.get(key)
    if (value.isDefined) {
      collect(value.get._1, value.get._2, value.get._3, collector)
    }
    //else null
  }
}

class TestDoubleKeyFetcher(idIndex: Int, nameIndex: Int)
  extends RichFlatMapFunction[BaseRow, BaseRow] {
  var resourceCounter: Int = 0
  var reuse: GenericRow = _

  if (idIndex < 0 || nameIndex < 0) {
    throw new RuntimeException("Must join on primary keys")
  }

  override def open(parameters: Configuration): Unit = {
    resourceCounter += 1
    reuse = new GenericRow(3)
  }

  override def close(): Unit = {
    resourceCounter -= 1
  }

  override def flatMap(keysRow: BaseRow, collector: Collector[BaseRow]): Unit = {
    val key = (keysRow.getInt(idIndex), keysRow.getString(nameIndex))
    val value = TestTable.doubleKeyTable.get(key)
    if (value.isDefined) {
      collect(value.get._1, value.get._2, value.get._3, collector)
    }
    //else null
  }

  def collect(age: Int, id: Int, name: String, out: Collector[BaseRow]): Unit = {
    TestFetcher.fetchByMapCount += 1
    reuse.update(0, age)
    reuse.update(1, id)
    reuse.update(2, name)
    out.collect(reuse)
  }

  def get(key: Int): BaseRow = {
    TestFetcher.fetchByGetCount += 1
    val value = TestTable.singleKeyTable.get(key)
    reuse.update(0, value.get._2)
    reuse.update(1, key)
    reuse.update(2, value.get._1)
    reuse
  }

  def getAllKeys: Iterable[Int] = TestTable.singleKeyTable.keys
}

object TestFetcher {
  def resetCounter() = {
    fetchByGetCount = 0
    fetchByMapCount = 0
  }

  var fetchByGetCount = 0;
  var fetchByMapCount = 0;
}

class TestAsyncSingleKeyFetcher(leftKeyIdx: Int) extends TestAsyncDoubleKeyFetcher(leftKeyIdx, 1) {

  override def asyncInvoke(keysRow: BaseRow, asyncCollector: ResultFuture[BaseRow]): Unit = {
    CompletableFuture
      .supplyAsync(new RowSupplier(keysRow), executor)
      .thenAccept(new Consumer[BaseRow] {
        override def accept(t: BaseRow): Unit = {
          if (t == null) {
            asyncCollector.complete(Collections.emptyList[BaseRow]())
          } else {
            asyncCollector.complete(Collections.singleton(t))
          }
        }
      })
  }

  class RowSupplier(val keysRow: BaseRow) extends Supplier[BaseRow] {
    override def get(): BaseRow = {
      val key = keysRow.getInt(leftKeyIdx)
      val value = TestTable.singleKeyTable.get(key)
      if (value.isDefined) {
        collect(value.get._1, value.get._2, value.get._3)
      } else {
        null
      }
    }

    def collect(age: Int, id: Int, name: String): BaseRow = {
      val row = new GenericRow(3)
      row.update(0, age)
      row.update(1, id)
      row.update(2, name)
      row
    }
  }
}

class TestAsyncDoubleKeyFetcher(leftKeyIdx: Int, nameKeyIdx: Int)
  extends RichAsyncFunction[BaseRow, BaseRow] {

  var resourceCounter: Int = 0
  if (leftKeyIdx < 0 || nameKeyIdx < 0) {
    throw new RuntimeException("Must join on primary keys")
  }

  @transient
  var executor: ExecutorService = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    resourceCounter += 1
    executor = Executors.newSingleThreadExecutor()
  }

  override def close(): Unit = {
    resourceCounter -= 1

    if (null != executor && !executor.isShutdown) {
      executor.shutdownNow()
    }
  }

  override def asyncInvoke(keysRow: BaseRow, asyncCollector: ResultFuture[BaseRow]): Unit = {
    CompletableFuture
      .supplyAsync(new RowSupplier(keysRow), executor)
      .thenAccept(new Consumer[BaseRow] {
        override def accept(t: BaseRow): Unit = {
          if (t == null) {
            asyncCollector.complete(Collections.emptyList[BaseRow]())
          } else {
            asyncCollector.complete(Collections.singleton(t))
          }
        }
      })
  }

  class RowSupplier(val keysRow: BaseRow) extends Supplier[BaseRow] {
    override def get(): BaseRow = {
      val key = (keysRow.getInt(leftKeyIdx), keysRow.getString(nameKeyIdx))
      val value = TestTable.doubleKeyTable.get(key)
      if (value.isDefined) {
        collect(value.get._1, value.get._2, value.get._3)
      } else {
        null
      }
    }

    def collect(age: Int, id: Int, name: String): BaseRow = {
      val row = new GenericRow(3)
      row.update(0, age)
      row.update(1, id)
      row.update(2, name)
      row
    }
  }

}

object TestTable {
  // index by id
  val singleKeyTable: Map[Int, (Int, Int, String)] = Map(
    1 -> (11, 1, "Julian"),
    2 -> (22, 2, "Jark"),
    3 -> (33, 3, "Fabian"))

  // index by (id, name)
  val doubleKeyTable: Map[(Int, String), (Int, Int, String)] = Map(
    (1, "Julian") -> (11, 1, "Julian"),
    (2, "Jark") -> (22, 2, "Jark"),
    (3, "Fabian") -> (33, 3, "Fabian"))
}
