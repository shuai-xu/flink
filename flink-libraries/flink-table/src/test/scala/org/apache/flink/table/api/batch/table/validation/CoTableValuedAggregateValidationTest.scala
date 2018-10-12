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

package org.apache.flink.table.api.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggregate.TestInnerJoinFunc
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.Test

/**
 * Test for testing co-table-valued aggregate validation.
 */
class CoTableValuedAggregateValidationTest extends TableTestBatchExecBase {

  @Test(expected = classOf[ValidationException])
  def testCoTableValuedAggOnNonExistingKeyJava(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    util.tableEnv.registerFunction("fun", fun)
    // no 'foo
    table1.connect(table2, "foo=r1")
      .coAggApply("fun(l1, l2)(r1, r2, r1 + 1)")
  }

  @Test(expected = classOf[ValidationException])
  def testCoTableValuedAggOnNonExistingKey(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // no 'foo
    table1.connect(table2, 'foo === 'r1)
      .coAggApply(fun('l1, 'l2)('r1, 'r2, 'r1 + 1))
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingCoTableValuedAggDataTypesJava(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // Must fail. Field 'l1 is not String type.
    util.tableEnv.registerFunction("fun", fun)
    table1.connect(table2, "l1=r1")
      .coAggApply("fun(l1, l1)(r1, r2, r1 + 1)")
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingCoTableValuedAggDataTypes(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // Must fail. Field 'l1 is not String type.
    table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l1)('r1, 'r2, 'r1 + 1))
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingCoTableValuedAggParamNumJava(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // Must fail. TestInnerJoinFunc only accept 2 params for accumulateLeft.
    util.tableEnv.registerFunction("fun", fun)
    table1.connect(table2, "l1=r1")
      .coAggApply("fun(l1, l2, l1)(r1, r2, r1 + 1)")
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingCoTableValuedAggParamNum(): Unit = {
    val util = batchTestUtil()
    val table1 = util.addTable[(Int, String)]('l1, 'l2)
    val table2 = util.addTable[(Int, String)]('r1, 'r2)
    val fun = new TestInnerJoinFunc
    // Must fail. TestInnerJoinFunc only accept 2 params for accumulateLeft.
    table1.connect(table2, 'l1 === 'r1)
      .coAggApply(fun('l1, 'l2, 'l1)('r1, 'r2, 'r1 + 1))
  }
}
