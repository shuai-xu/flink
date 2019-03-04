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

import org.apache.commons.math3.util.ArithmeticUtils

object MiniBatchIntervalInferUtil {
    /**
    * Create a new MiniBatchIntervalTrait with a merged minibatch interval value.
    * The Merge Logic:  MiniBatchMode: (R: rowtime, P: proctime, N: None), I: Interval
    * Possible values:
    * - (R, I = 0): operators that require watermark (window excluded).
    * - (R, I > 0): window / operators that require watermark with minibatch enabled.
    * - (P, I > 0): unbounded agg with minibatch enabled.
    * - (N, I = 0): no operator requires watermark, minibatch disabled
    * ------------------------------------------------
    * |    A        |    B        |   merged result
    * ------------------------------------------------
    * | R, I_1 == 0 | R, I_2      |  R, gcd(I_1, I_2)
    * ------------------------------------------------
    * | R, I_1 == 0 | P, I_2      |  R, I_2
    * ------------------------------------------------
    * | R, I_1 > 0  | R, I_2      |  R, gcd(I_1, I_2)
    * ------------------------------------------------
    * | R, I_1 > 0  | P, I_2      |  R, I_1
    * ------------------------------------------------
    * | P, I_1      | R, I_2 == 0 |  R, I_1
    * ------------------------------------------------
    * | P, I_1      | R, I_2 > 0  |  R, I_2
    * ------------------------------------------------
    * | P, I_1      | P, I_2 > 0  |  P, I_1
    * ------------------------------------------------
    */
  def mergedMiniBatchInterval(mbi: MiniBatchInterval, updatedMbi: MiniBatchInterval)
  : MiniBatchInterval = {
    if (mbi.mode == MiniBatchMode.None) {
      updatedMbi
    } else if (mbi.mode == MiniBatchMode.RowTime) {
      if (updatedMbi.mode == MiniBatchMode.None) {
        mbi
      } else if (updatedMbi.mode == MiniBatchMode.RowTime) {
        MiniBatchInterval(
          ArithmeticUtils.gcd(mbi.interval, updatedMbi.interval), MiniBatchMode.RowTime)
      } else if (mbi.interval == 0) {
        MiniBatchInterval(updatedMbi.interval, MiniBatchMode.RowTime)
      } else {
        mbi
      }
    } else {
      if (updatedMbi.mode == MiniBatchMode.None || updatedMbi.mode == MiniBatchMode.ProcTime) {
        mbi
      } else if (updatedMbi.interval > 0) {
        updatedMbi
      } else {
        MiniBatchInterval(mbi.interval, MiniBatchMode.RowTime)
      }
    }
  }


}
