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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.CoTableValuedAggregateFunction
import org.apache.flink.table.functions.aggregate._
import org.apache.flink.table.util.TableTestBase
import org.apache.flink.util.Collector
import org.junit.Test

class CoTableValuedAggregateValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testConnectOnNonExistingKey(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // no 'foo
    val resultTable = table1.connect(table2, 'foo === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
    util.verifyPlanAndTrait(resultTable)
  }

  @Test(expected = classOf[ValidationException])
  def testFuctionSignatureError(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // there is no 'l3 in the input
    val resultTable = table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2, 'l3)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
    util.verifyPlanAndTrait(resultTable)
  }

  @Test(expected = classOf[ValidationException])
  def testNoAccumulateLeft(): Unit = {
    val util = streamTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    // no accumulateRight in ErrorFunc
    val fun = new ErrorFunc
    val resultTable = table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))

    util.verifyPlan(resultTable)
    util.verifyPlanAndTrait(resultTable)
  }
}

class AccWrapper {
  var l: Long = 0
}

class ErrorFunc
  extends CoTableValuedAggregateFunction
    [Integer, AccWrapper] {

  def accumulateLeft(accumulator: AccWrapper, a: Integer, b: String): Unit = {}

  override def emitValue(accumulator: AccWrapper, out: Collector[Integer]): Unit = {}

  override def createAccumulator(): AccWrapper = {
    new AccWrapper
  }
}
