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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggregate._
import org.apache.flink.table.runtime.stream.table.StringLast
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class CoTableValuedAggregateTest extends TableTestBase {

  @Test
  def testCoGroupTableValuedAggregate(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2, 'l1 === 'r1)
        .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testCoNonGroupTableValuedAggregate(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testCoGroupTableValuedAggregateWithRetract(): Unit = {
    val util = streamTestUtil()
    val last = new StringLast
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
      .groupBy('l1)
      .select('l1, last('l2) as 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
      .groupBy('r1)
      .select('r1, last('r2) as 'r2)
    val fun = new TestInnerJoinFunc
    val resultTable = table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
    util.verifyPlanAndTrait(resultTable)
  }
}
