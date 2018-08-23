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

import java.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.CoreOptions
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.source.FromElementsFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, NIAGARA_BACKEND, StateBackendMode}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.rules.TemporaryFolder
import org.junit.runners.Parameterized
import org.junit.{After, Assert, Before, Rule}

import scala.collection.JavaConversions._

class StreamingWithStateTestBase(state: StateBackendMode)
  extends AbstractTestBase {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  def before(): Unit = {
    StreamTestSink.clear()
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // set state backend
    state match {
      case HEAP_BACKEND =>
        env.setConfiguration(CoreOptions.STATE_BACKEND_CLASSNAME,
          classOf[HeapInternalStateBackend].getCanonicalName)

      case NIAGARA_BACKEND =>
        env.getConfiguration.setString(
          CoreOptions.STATE_BACKEND_CLASSNAME,
          classOf[NiagaraStateBackend].getCanonicalName)
    }
    this.tEnv = TableEnvironment.getTableEnvironment(env)
  }

  @After
  def after(): Unit = {
    Assert.assertTrue(FailingCollectionSource.failedBefore)
  }

  /**
    * Creates a DataStream from the given non-empty [[Seq]].
    */
  def failingDataSource[T: TypeInformation](data: Seq[T]): DataStream[T] = {
    state match {
      case NIAGARA_BACKEND =>
        // TODO: introducing state recovery may cause NIAGARA core dump, see BLINK-15412517
        // TODO: please revert this after BLINK-15412517 is fixed
        FailingCollectionSource.failedBefore = true
        env.fromCollection(data)
      case _ =>
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
        // reset failedBefore flag to false
        FailingCollectionSource.reset()

        require(data != null, "Data must not be null.")
        val typeInfo = implicitly[TypeInformation[T]]

        val collection = scala.collection.JavaConversions.asJavaCollection(data)
        // must not have null elements and mixed elements
        FromElementsFunction.checkCollection(data, typeInfo.getTypeClass)

        val function = new FailingCollectionSource[T](
          typeInfo.createSerializer(env.getConfig),
          collection,
          data.length / 2) // fail after half elements

        env.addSource(function)(typeInfo).setMaxParallelism(1)
    }
  }
}

object StreamingWithStateTestBase {

  case class StateBackendMode(backend: String) {
    override def toString: String = backend.toString
  }

  val HEAP_BACKEND = StateBackendMode("HEAP")
  val NIAGARA_BACKEND = StateBackendMode("NIAGARA")

  @Parameterized.Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    val isLinuxAliOS = System.getProperty("os.name").startsWith("Linux") &&
      System.getProperty("os.version").contains("alios7")

    if (isLinuxAliOS) {
      Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(NIAGARA_BACKEND))
    } else {
      // if OS is not linux, only check heap state backend
      Seq[Array[AnyRef]](Array(HEAP_BACKEND))
    }
  }
}
