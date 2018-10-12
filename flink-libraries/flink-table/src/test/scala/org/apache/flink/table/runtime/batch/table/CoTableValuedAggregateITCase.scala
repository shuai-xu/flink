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
package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggregate._
import org.apache.flink.table.plan.cost.Func0
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.test.util.TestBaseUtils
import org.junit._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class CoTableValuedAggregateITCase extends QueryTest {

  val left = new mutable.MutableList[(Int, String)]
  left.+=((1, "one_l"))
  left.+=((2, "two_l1"))
  left.+=((2, "two_l2"))

  val right = new mutable.MutableList[(Int, String)]
  right.+=((1, "one_r"))
  right.+=((2, "two_r1"))
  right.+=((2, "two_r2"))
  right.+=((3, "three_r"))
  right.+=((3, "three_r"))
  right.+=((3, "three_r"))

  @Test
  def testInnerJoinUsingCoTableValuedFunction(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc
    val func = new Func0
    val sourceL =  tEnv.fromCollection(Random.shuffle(left), "l1, l2")
    val sourceR =  tEnv.fromCollection(Random.shuffle(right), "r1, r2")
    val result = sourceL.connect(sourceR,'l1 === func('r1))
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .select('l1, 'f0, 'f1, 'f2, 'f3, 'f4, 'f5)
    val expected =
        "1,1,one_l,1,one_r,2,2\n" +
        "2,2,two_l1,2,two_r1,3,4\n" +
        "2,2,two_l1,2,two_r2,3,4\n" +
        "2,2,two_l2,2,two_r1,3,4\n" +
        "2,2,two_l2,2,two_r2,3,4"
    val results = result.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNonKeyCoTableValuedFunction(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc

    val left = new mutable.MutableList[(Int, String)]
    left.+=((1, "one_l"))
    left.+=((2, "two_l1"))
    val right = new mutable.MutableList[(Int, String)]
    right.+=((2, "two_r2"))
    right.+=((3, "three_r"))

    val sourceL =  tEnv.fromCollection(Random.shuffle(left), "l1, l2")
    val sourceR =  tEnv.fromCollection(Random.shuffle(right), "r1, r2")
    val result = sourceL.connect(sourceR)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .select('f0, 'f1, 'f2, 'f3, 'f4, 'f5)
    val expected =
        "1,one_l,2,two_r2,3,4\n" +
        "1,one_l,3,three_r,4,4\n" +
        "2,two_l1,2,two_r2,3,4\n" +
        "2,two_l1,3,three_r,4,4"
    val results = result.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testWithPojoResult(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc
    val func = new Func0
    val sourceL =  tEnv.fromCollection(Random.shuffle(left), "l1, l2")
    val sourceR =  tEnv.fromCollection(Random.shuffle(right), "r1, r2")
    val result = sourceL.connect(sourceR,'l1 + 2 === func('r1) + 2)
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .as('k, 'a, 'b, 'c, 'd, 'e)
    val expected =
        "3,1,one_l,1,one_r,2,2\n" +
        "4,2,two_l1,2,two_r1,3,4\n" +
        "4,2,two_l1,2,two_r2,3,4\n" +
        "4,2,two_l2,2,two_r1,3,4\n" +
        "4,2,two_l2,2,two_r2,3,4"
    val results = result.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPushDownDistributionIntoCoTableValuedAgg(): Unit = {
    val coTVAGGFunc = new TestInnerJoinFunc
    val func = new Func0
    val sourceL =  tEnv.fromCollection(Random.shuffle(left), "l1, l2")
    val sourceR =  tEnv.fromCollection(Random.shuffle(right), "r1, r2")
    val result = sourceL.connect(sourceR,'l1 === func('r1))
      .coAggApply(coTVAGGFunc('l1, 'l2)('r1, 'r2, 'r1 + 1))
      .select('l1, 'f0, 'f1, 'f2, 'f3, 'f4, 'f5)
      .groupBy('l1)
      .select('l1, 'f0.count)
    val expected =
        "1,1\n" +
        "2,4"
    val results = result.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
