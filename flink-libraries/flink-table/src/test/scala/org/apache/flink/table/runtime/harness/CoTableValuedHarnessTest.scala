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

package org.apache.flink.table.runtime.harness

import java.lang.{Integer => JInt, String => JString}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.functions.aggregate.{TestInnerJoinFunc}
import org.apache.flink.table.runtime.utils.BaseRowHarnessAssertor
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BaseRowUtil._
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class CoTableValuedHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode){
  val data = new mutable.MutableList[(Int, String)]

  @Test
  def testInnerJoinUsingCoTableValuedAggFunction(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc

    val joinedRowType = new BaseRowTypeInfo(
      classOf[BaseRow],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val source1 = env.fromCollection(data).toTable(tEnv, 'l1, 'l2)
    val source2 = env.fromCollection(data).toTable(tEnv, 'r1, 'r2)
    val t = source1.connect(source2, 'l1 === 'r1)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1))

    val queryConfig = new StreamQueryConfig()

    val testHarness = createTwoInputHarnessTester(
      t.toRetractStream[Row](queryConfig), "CoGroupTableValuedAggregate")

    val assertor = new BaseRowHarnessAssertor(Array(Types.INT))

    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement1(new StreamRecord(
      setAccumulate(GenericRow.of(1: JInt, "one_l": JString, 1: JInt)), 1))
    testHarness.processElement1(new StreamRecord(
      setAccumulate(GenericRow.of(2: JInt, "two_l": JString, 2: JInt)), 1))
    testHarness.processElement2(new StreamRecord(
      setAccumulate(GenericRow.of(1: JInt, "one_r": JString, 1: JInt, 1: JInt)), 1))
    testHarness.processElement2(new StreamRecord(
      setAccumulate(GenericRow.of(1: JInt, "one_r": JString, 1: JInt, 1: JInt)), 1))
    testHarness.processElement2(new StreamRecord(
      setAccumulate(GenericRow.of(2: JInt, "two_r": JString, 2: JInt, 2: JInt)), 1))
    testHarness.processElement2(new StreamRecord(
      setAccumulate(GenericRow.of(3: JInt, "three_r": JString, 3: JInt, 3: JInt)), 1))

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinedRowType)
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // (1, one_l) join (1, one_r, 1) => (1, 1, one_l, 1, one_r, 1, 2)
    // output: joinKey + leftRow + rightRow + inputCnt
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(
        1: JInt, 1: JInt, "one_l": JString, 1: JInt, "one_r": JString, 1: JInt, 2: JInt))))
    expectedOutput.add(new StreamRecord(
      setRetract(GenericRow.of(
        1: JInt, 1: JInt, "one_l": JString, 1: JInt, "one_r": JString, 1: JInt, 2: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(
        1: JInt, 1: JInt, "one_l": JString, 1: JInt, "one_r": JString, 1: JInt, 3: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(
        1: JInt, 1: JInt, "one_l": JString, 1: JInt, "one_r": JString, 1: JInt, 3: JInt))))
    expectedOutput.add(new StreamRecord(
      setAccumulate(GenericRow.of(
        2: JInt, 2: JInt, "two_l": JString, 2: JInt, "two_r": JString, 2: JInt, 2: JInt))))

    assertor.assertOutputEqualsSorted("result error", expectedOutput, result)
    testHarness.close()
  }
}
