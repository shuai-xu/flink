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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.{MicroBatchOn, MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

abstract class StreamingWithMiniBatchTestBase(minibatch: MiniBatchMode, state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  override def before(): Unit = {
    super.before()
    // set mini batch
    val tableConfig = tEnv.getConfig
    minibatch match {
      case MiniBatchOn =>
        tableConfig.getConf.setBoolean(TableConfigOptions.BLINK_MINIBATCH_JOIN_ENABLED, true)
        tableConfig
        .enableMiniBatch
        .withMiniBatchTriggerTime(1000L)
        .withMiniBatchTriggerSize(3)
      case MicroBatchOn =>
        tableConfig.getConf.setBoolean(TableConfigOptions.BLINK_MINIBATCH_JOIN_ENABLED, true)
        tableConfig
        .enableMicroBatch
        .withMicroBatchTriggerTime(1000L)
        .withMiniBatchTriggerSize(3)
      case MiniBatchOff =>
        tableConfig.getConf.setBoolean(TableConfigOptions.BLINK_MINIBATCH_JOIN_ENABLED, false)
        tableConfig.disableMiniBatch
    }
  }

}

object StreamingWithMiniBatchTestBase {

  case class MiniBatchMode(on: Boolean, microbatch: Boolean) {
    override def toString: String = {
      if (microbatch) {
        if (on) "MicroBatch=ON" else "MicroBatch=OFF"
      } else {
        if (on) "MiniBatch=ON" else "MiniBatch=OFF"
      }
    }
  }

  val MiniBatchOn = MiniBatchMode(true, false)
  val MiniBatchOff = MiniBatchMode(false, false)
  val MicroBatchOn = MiniBatchMode(true, true)

  @Parameterized.Parameters(name = "{0}, StateBackend={1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(MiniBatchOff, HEAP_BACKEND),
      Array(MiniBatchOn, HEAP_BACKEND),
      Array(MiniBatchOff, ROCKSDB_BACKEND),
      Array(MiniBatchOn, ROCKSDB_BACKEND),
      Array(MicroBatchOn, HEAP_BACKEND),
      Array(MicroBatchOn, ROCKSDB_BACKEND))
  }
}
