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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.runtime.utils.CommonTestData

import com.google.common.collect.Maps
import org.apache.calcite.rel.RelNode
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.io.Source

class RelDigestUtilTest {

  var tableEnv: TableEnvironment = _

  @Before
  def before(): Unit = {
    RelDigestUtil.nonDeterministicIdCounter.set(0)
    val conf = new TableConfig()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    tableEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    tableEnv.registerTableSource("MyTable", CommonTestData.getCsvTableSource)
  }

  @Test
  def testGetDigestWithDynamicFunction(): Unit = {
    val table = tableEnv.sqlQuery(
      """
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
      """.stripMargin)
    val rel = table.getRelNode
    val expected = readFromResource("testGetDigestWithDynamicFunction.out")
    assertEquals(expected, RelDigestUtil.getDigest(rel, addUniqueIdForNonDeterministicOp = true))
  }

  @Test
  def testGetDigestWithDynamicFunctionView(): Unit = {
    val view = tableEnv.sqlQuery("SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1")
    tableEnv.registerTable("MyView", view)
    val table = tableEnv.sqlQuery(
      """
        |(SELECT * FROM MyView)
        |INTERSECT
        |(SELECT * FROM MyView)
        |INTERSECT
        |(SELECT * FROM MyView)
      """.stripMargin)
    val rel = table.getRelNode.accept(new ExpandTableScanShuttle())
    val expected = readFromResource("testGetDigestWithDynamicFunctionView.out")
    assertEquals(expected, RelDigestUtil.getDigest(rel, addUniqueIdForNonDeterministicOp = true))
  }

  @Test
  def testGetDigestWithDynamicFunctionViewAndCache(): Unit = {
    val view = tableEnv.sqlQuery("SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1")
    tableEnv.registerTable("MyView", view)
    val table = tableEnv.sqlQuery(
      """
        |(SELECT * FROM MyView)
        |INTERSECT
        |(SELECT * FROM MyView)
        |INTERSECT
        |(SELECT * FROM MyView)
      """.stripMargin)
    val rel = table.getRelNode.accept(new ExpandTableScanShuttle())
    val expected = readFromResource("testGetDigestWithDynamicFunctionViewAndCache.out")
    val cache = Maps.newIdentityHashMap[RelNode, String]()
    assertEquals(expected, RelDigestUtil.getDigestWithCache(
      rel, addUniqueIdForNonDeterministicOp = true, cache))
  }

  private def readFromResource(name: String): String = {
    val inputStream = getClass.getResource("/digest/" + name).getFile
    Source.fromFile(inputStream).mkString
  }

}
