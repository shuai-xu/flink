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

import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, NIAGARA_BACKEND, StateBackendMode}
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn, MicroBatchOn}
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

abstract class StreamingWithMiniBatchTestBase(minibatch: MiniBatchMode, state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  override def before(): Unit = {
    super.before()
    // set mini batch
    val queryConfig = tEnv.queryConfig
    minibatch match {
      case MiniBatchOn =>
        queryConfig
        .enableMiniBatch
        .enableMiniBatchJoin
        .withMiniBatchTriggerTime(1000L)
        .withMiniBatchTriggerSize(3)
      case MicroBatchOn =>
        queryConfig
        .enableMicroBatch
        .enableMiniBatchJoin
        .withMicroBatchTriggerTime(1000L)
        .withMiniBatchTriggerSize(3)
      case MiniBatchOff =>
        queryConfig.disableMiniBatch.disableMiniBatchJoin
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
    val isLinuxAliOS = System.getProperty("os.name").startsWith("Linux") &&
      System.getProperty("os.version").contains("alios7")

    if (isLinuxAliOS) {
      Seq[Array[AnyRef]](
        Array(MiniBatchOff, HEAP_BACKEND),
        Array(MiniBatchOn, HEAP_BACKEND),
        Array(MiniBatchOff, NIAGARA_BACKEND),
        Array(MiniBatchOn, NIAGARA_BACKEND),
        Array(MicroBatchOn, HEAP_BACKEND),
        Array(MicroBatchOn, NIAGARA_BACKEND))
    } else {
      Seq[Array[AnyRef]](
        Array(MiniBatchOff, HEAP_BACKEND),
        Array(MiniBatchOn, HEAP_BACKEND),
        Array(MicroBatchOn, HEAP_BACKEND))
    }
  }
}
