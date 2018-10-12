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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.aggregate.SimpleTVAGG
import org.apache.flink.table.util.TableTestBatchExecBase
import org.junit.Test
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

/**
 * Test for testing table-valued aggregate validation.
 */
class TableValuedAggregateValidationTest extends TableTestBatchExecBase {
  @Test(expected = classOf[ValidationException])
  def testNonWorkingTableValuedAggDataTypesJava(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must fail. Field '_1 is not int type.
    t.aggApply("testTVAGGFun(_1)")
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingTableValuedAggDataTypes(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must fail. Field '_1 is not int type.
    t.aggApply(testTVAGGFun('_1))
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingTableValuedAggParamNumJava(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must fail. testTVAGGFun only accept 1 param.
    t.aggApply("testTVAGGFun(_2, _1)")
  }

  @Test(expected = classOf[ValidationException])
  def testNonWorkingTableValuedAggParamNum(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must fail. testTVAGGFun only accept 1 param.
    t.aggApply(testTVAGGFun('_2, '_1))
  }

  @Test(expected = classOf[ValidationException])
  def testNestedTableValuedAggJava(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must fail. Nested tvagg expression is not supported
    t.aggApply("testTVAGGFun(testTVAGGFun(_2))")
  }

  @Test(expected = classOf[ValidationException])
  def testNestedTableValuedAgg(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must fail. Nested tvagg expression is not supported
    t.aggApply(testTVAGGFun(testTVAGGFun('_2)))
  }

  @Test
  def testTableValuedAggGroupKeyParam(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must success.
    t.groupBy('_2).aggApply(testTVAGGFun('_2))
  }

  @Test
  def testTableValuedAggOtherFieldParam(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must success.
    t.groupBy('_1).aggApply(testTVAGGFun('_2))
  }

  @Test
  def controlledTrialOfTestTableValuedAggJava(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must success. Field '_2 is int type.
    t.aggApply("testTVAGGFun(_2)")
  }

  @Test
  def controlledTrialOfTestTableValuedAgg(): Unit = {
    val util = batchTestUtil()
    val testTVAGGFun = new SimpleTVAGG
    val t = util.addTable[(String, Int)]("Table2", '_1, '_2)
    util.tableEnv.registerFunction("testTVAGGFun", testTVAGGFun)

    // Must success. Field '_2 is int type.
    t.aggApply(testTVAGGFun('_2))
  }
}
