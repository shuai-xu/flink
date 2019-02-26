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
package org.apache.flink.table.runtime.utils

import java.lang.{Long => JLong}
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{Consumer, Supplier}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.functions.{AsyncTableFunction, CustomTypeDefinedFunction, FunctionContext, TableFunction}
import org.apache.flink.table.api.types.{DataType, TypeConverters}
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, GenericRow}
import org.apache.flink.table.sources._
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.TableSchemaUtil
import org.junit.Assert

object TemporalTableUtils {

  // index by id
  val singleKeyTable: Map[Long, (Int, Long, String)] = Map(
    1L -> (11, 1L, "Julian"),
    2L -> (22, 2L, "Jark"),
    3L -> (33, 3L, "Fabian"))

  // index by (id, name)
  val doubleKeyTable: Map[(Long, String), (Int, Long, String)] = Map(
    (1L, "Julian") -> (11, 1L, "Julian"),
    (2L, "Jark") -> (22, 2L, "Jark"),
    (3L, "Fabian") -> (33, 3L, "Fabian"))

  class TestingTemporalTableSourceWithDoubleKey(async: Boolean = false)
    extends TestingTemporalTableSource(async) {

    override def getTableSchema: TableSchema = {
      TableSchemaUtil
      .builderFromDataType(getReturnType)
      .uniqueIndex("id", "name")
      .build()
    }

    override def getLookupFunction(lookupKeys: Array[Int]): TableFunction[BaseRow] = {
      // the lookupkeys must be [1, 2]
      Assert.assertArrayEquals(Array(1, 2), lookupKeys)
      // new key idx mapping to keysRow
      fetcher = new TestingDoubleKeyFetcher(0, 1)
      fetcher
    }

    override def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[BaseRow] = {
      // the lookupkeys must be [1, 2]
      Assert.assertArrayEquals(Array(1, 2), lookupKeys)
      // new idx mapping to keysRow
      asyncFetcher = new TestingAsyncDoubleKeyFetcher(0, 1)
      asyncFetcher
    }
  }

  class TestingTemporalTableSource(
    async: Boolean = false,
    conf: LookupConfig = null,
    delayedReturn: Long = 0L)
  extends TableSource
  with LookupableTableSource[BaseRow]
  with StreamTableSource[BaseRow]
  with BatchTableSource[BaseRow] {

    var fetcher: TestingDoubleKeyFetcher = _
    var asyncFetcher: TestingAsyncDoubleKeyFetcher = _

    override def getReturnType: DataType =
      TypeConverters.createInternalTypeFromTypeInfo(
        new BaseRowTypeInfo(
          Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
          Array( "age", "id", "name")))


    override def getLookupFunction(lookupKeys: Array[Int]): TableFunction[BaseRow] = {
      // the lookupkeys must be [1]
      Assert.assertArrayEquals(Array(1), lookupKeys)
      fetcher = new TestingSingleKeyFetcher(0)
      fetcher
    }

    override def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[BaseRow] = {
      // the lookupkeys must be [1]
      Assert.assertArrayEquals(Array(1), lookupKeys)
      asyncFetcher = new TestingAsyncSingleKeyFetcher(0)
      asyncFetcher
    }

    override def getLookupConfig: LookupConfig = {
      if (conf == null) {
        val config = new LookupConfig
        config.setAsyncEnabled(async)
        config.setAsyncTimeoutMs(10000)
        config
      } else {
        conf
      }
    }

    def getFetcherResourceCount: Int = {
      if (async && null != asyncFetcher) {
        asyncFetcher.resourceCounter
      } else if (null != fetcher) {
        fetcher.resourceCounter
      } else {
        0
      }
    }

    def validateTableFunctionResultType(
        arguments: Array[AnyRef],
        argTypes: Array[Class[_]]): Unit = {
      if (async) {
        asyncFetcher.validateResultType(arguments, argTypes)
      } else {
        fetcher.validateResultType(arguments, argTypes)
      }
    }

    /** Returns the table schema of the table source */
    override def getTableSchema: TableSchema = {
      TableSchemaUtil
      .builderFromDataType(getReturnType)
      .primaryKey("id")
      .build()
    }

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
      throw new UnsupportedOperationException
    }

    override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
      throw new UnsupportedOperationException
    }
  }

  trait TableFunctionWithValidatedResultType extends CustomTypeDefinedFunction {
    private var arguments: Array[AnyRef] = Array()
    private var argTypes: Array[Class[_]] = Array()

    override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
      this.arguments = arguments
      this.argTypes = argTypes
      super.getResultType(arguments, argTypes)
    }

    def validateResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): Unit = {
      assert(this.arguments.length == arguments.length, s"TableFunction getResultType arguments " +
        s"size expect to be ${arguments.length}, while we got size ${this.arguments.length}.")
      assert(this.argTypes.length == argTypes.length, s"TableFunction getResultType argTypes " +
        s"size expect to be ${argTypes.length}, while we got size ${this.argTypes.length}.")
      (this.arguments zip arguments).zipWithIndex foreach { trp =>
        assert(trp._1._1 == trp._1._2, s"TableFunction getResultType arg[${trp._2}] expect to be " +
          s"${trp._1._2} while actually is ${trp._1._1}.")
      }
      (this.argTypes zip argTypes).zipWithIndex foreach { trp =>
        assert(trp._1._1 == trp._1._2, s"TableFunction getResultType arg[${trp._2}] expect to be " +
          s" type ${trp._1._2} while actually is type ${trp._1._1}.")
      }
    }
  }

  // lookup data table using id index
  class TestingSingleKeyFetcher(idIndex: Int) extends TestingDoubleKeyFetcher(idIndex, 1) {
    def eval(id: JLong): Unit = {
      if (id != null) {
        val value = TemporalTableUtils.singleKeyTable.get(id)
        if (value.isDefined) {
          collect(value.get._1, value.get._2, value.get._3)
        }
      }
    }
  }

  class TestingDoubleKeyFetcher(idIndex: Int, nameIndex: Int)
      extends TableFunction[BaseRow]
      with TableFunctionWithValidatedResultType {
    var resourceCounter: Int = 0
    var reuse: GenericRow = _

    if (idIndex < 0 || nameIndex < 0) {
      throw new RuntimeException("Must join on primary keys")
    }

    override def open(context: FunctionContext): Unit = {
      resourceCounter += 1
      reuse = new GenericRow(3)
    }

    override def close(): Unit = {
      resourceCounter -= 1
    }

    def eval(id: JLong, name: BinaryString): Unit = {
      if (id != null && name != null) {
        val value = TemporalTableUtils.doubleKeyTable.get((id, name.toString))
        if (value.isDefined) {
          collect(value.get._1, value.get._2, value.get._3)
        }
      }
    }

    def collect(age: Int, id: Long, name: String): Unit = {
      reuse.update(0, age)
      reuse.update(1, id)
      reuse.update(2, name)
      collect(reuse)
    }
  }

  class TestingAsyncSingleKeyFetcher(leftKeyIdx: Int)
    extends TestingAsyncDoubleKeyFetcher(leftKeyIdx, 1) {

    def eval(asyncCollector: ResultFuture[BaseRow], id: JLong): Unit = {
      CompletableFuture
      .supplyAsync(new SingleKeySupplier(id), executor)
      .thenAccept(new Consumer[BaseRow] {
        override def accept(t: BaseRow): Unit = {
          if (delayedReturn > 0L) {
            Thread.sleep(delayedReturn)
          }
          if (t == null) {
            asyncCollector.complete(Collections.emptyList[BaseRow]())
          } else {
            asyncCollector.complete(Collections.singleton(t))
          }
        }
      })
    }

    class SingleKeySupplier(id: JLong) extends Supplier[BaseRow] {
      override def get(): BaseRow = {
        if (id != null) {
          val value = TemporalTableUtils.singleKeyTable.get(id)
          if (value.isDefined) {
            collect(value.get._1, value.get._2, value.get._3)
          } else {
            null
          }
        } else {
          null
        }
      }

      def collect(age: Int, id: Long, name: String): BaseRow = {
        val row = new GenericRow(3)
        row.update(0, age)
        row.update(1, id)
        row.update(2, name)
        row
      }
    }
  }

  class TestingAsyncDoubleKeyFetcher(leftKeyIdx: Int, nameKeyIdx: Int)
    extends AsyncTableFunction[BaseRow]
    with TableFunctionWithValidatedResultType {

    var resourceCounter: Int = 0
    if (leftKeyIdx < 0 || nameKeyIdx < 0) {
      throw new RuntimeException("Must join on primary keys")
    }

    var delayedReturn: Long = 0L

    def setDelayedReturn(delayedReturn: Long): Unit = {
      this.delayedReturn = delayedReturn
    }

    @transient
    var executor: ExecutorService = _

    override def open(context: FunctionContext): Unit = {
      resourceCounter += 1
      executor = Executors.newSingleThreadExecutor()
    }

    override def close(): Unit = {
      resourceCounter -= 1

      if (null != executor && !executor.isShutdown) {
        executor.shutdownNow()
      }
    }

    def eval(asyncCollector: ResultFuture[BaseRow], id: JLong, name: BinaryString): Unit = {
      CompletableFuture
      .supplyAsync(new DoubleKeySupplier(id, name.toString), executor)
      .thenAccept(new Consumer[BaseRow] {
        override def accept(t: BaseRow): Unit = {
          if (delayedReturn > 0L) {
            Thread.sleep(delayedReturn)
          }
          if (t == null) {
            asyncCollector.complete(Collections.emptyList[BaseRow]())
          } else {
            asyncCollector.complete(Collections.singleton(t))
          }
        }
      })
    }

    class DoubleKeySupplier(id: JLong, name: String) extends Supplier[BaseRow] {
      override def get(): BaseRow = {
        if (id != null && name != null) {
          val value = TemporalTableUtils.doubleKeyTable.get((id, name))
          if (value.isDefined) {
            collect(value.get._1, value.get._2, value.get._3)
          } else {
            null
          }
        } else {
          null
        }
      }

      def collect(age: Int, id: Long, name: String): BaseRow = {
        val row = new GenericRow(3)
        row.update(0, age)
        row.update(1, id)
        row.update(2, name)
        row
      }
    }
  }
}
