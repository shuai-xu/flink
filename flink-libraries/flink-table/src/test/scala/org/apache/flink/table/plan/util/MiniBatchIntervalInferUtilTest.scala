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

package org.apache.flink.table.plan.util

import org.apache.flink.table.plan.`trait`.{MiniBatchInterval, MiniBatchMode}

import org.junit.Test

import org.junit.Assert.assertEquals

class MiniBatchIntervalInferUtilTest {
  @Test
  def testRowTimeTraitMergingNone(): Unit = {
    val NoneMiniBatchInterval = MiniBatchInterval.NONE
    val rowtimeMiniBatchInterval = MiniBatchInterval(1000L, MiniBatchMode.RowTime)
    val mergedResult = MiniBatchIntervalInferUtil
      .mergedMiniBatchInterval(NoneMiniBatchInterval, rowtimeMiniBatchInterval)
    assertEquals(mergedResult, rowtimeMiniBatchInterval)
  }

  @Test
  def testProcTimeTraitMergingNone(): Unit = {
    val NoneMiniBatchInterval = MiniBatchInterval.NONE
    val proctimeMiniBatchInterval = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    val mergedResult = MiniBatchIntervalInferUtil
      .mergedMiniBatchInterval(NoneMiniBatchInterval, proctimeMiniBatchInterval)
    assertEquals(mergedResult, proctimeMiniBatchInterval)
  }

  @Test
  def testRowTimeTraitMergingProcTimeTrait1(): Unit = {
    val rowtimeMiniBatchInterval = MiniBatchInterval(4000L, MiniBatchMode.RowTime)
    val proctimeMiniBatchInterval = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    val mergedResult = MiniBatchIntervalInferUtil
      .mergedMiniBatchInterval(rowtimeMiniBatchInterval, proctimeMiniBatchInterval)
    assertEquals(mergedResult, rowtimeMiniBatchInterval)
  }

  @Test
  def testRowTImeTraitMergingProcTimeTrait2(): Unit = {
    val rowtimeMiniBatchInterval = MiniBatchInterval(0L, MiniBatchMode.RowTime)
    val proctimeMiniBatchInterval = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    val mergedResult = MiniBatchIntervalInferUtil
      .mergedMiniBatchInterval(rowtimeMiniBatchInterval, proctimeMiniBatchInterval)
    assertEquals(mergedResult, MiniBatchInterval(1000L, MiniBatchMode.RowTime))
  }

  @Test
  def testRowTimeTraitMergingRowtimeTrait(): Unit = {
    val rowtimeMiniBatchInterval = MiniBatchInterval(3000L, MiniBatchMode.RowTime)
    val proctimeMiniBatchInterval = MiniBatchInterval(5000L, MiniBatchMode.RowTime)
    val mergedResult = MiniBatchIntervalInferUtil
      .mergedMiniBatchInterval(rowtimeMiniBatchInterval, proctimeMiniBatchInterval)
    assertEquals(mergedResult, MiniBatchInterval(1000L, MiniBatchMode.RowTime))
  }
}
