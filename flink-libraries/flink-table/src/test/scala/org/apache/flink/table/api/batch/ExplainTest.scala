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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}

import org.junit._

class ExplainTest extends TableTestBase {

  private var util: BatchTableTestUtil = _

  @Before
  def setup(): Unit = {
    util = batchTestUtil()
  }

  @Test
  def testFilterWithoutExtended(): Unit = {
    val table = util.tableEnv.fromCollection(Seq((1, "hello")), 'a, 'b)
      .filter("a % 2 = 0")
    util.verifyExplain(table)
  }

  @Test
  def testFilterWithExtended(): Unit = {
    val table = util.tableEnv.fromCollection(Seq((1, "hello")), 'a, 'b)
      .filter("a % 2 = 0")

    util.verifyExplain(table)
  }

  @Test
  def testJoinWithoutExtended(): Unit = {
    val table1 = util.tableEnv.fromCollection(Seq((1, "hello")), 'a, 'b)
    val table2 = util.tableEnv.fromCollection(Seq((1, "hello")), 'c, 'd)
    val table = table1.join(table2).where("b = d").select('a, 'c)

    util.verifyExplain(table)
  }

  @Test
  def testJoinWithExtended(): Unit = {
    val table1 = util.tableEnv.fromCollection(Seq((1, "hello")), 'a, 'b)
    val table2 = util.tableEnv.fromCollection(Seq((1, "hello")), 'c, 'd)
    val table = table1.join(table2).where("b = d").select('a, 'c)

    util.verifyExplain(table)
  }

  @Test
  def testUnionWithoutExtended(): Unit = {
    val table1 = util.tableEnv.fromCollection(Seq((1, "hello")), 'count, 'word)
    val table2 = util.tableEnv.fromCollection(Seq((1, "hello")), 'count, 'word)
    val table = table1.unionAll(table2)

    util.verifyExplain(table)
  }

  @Test
  def testUnionWithExtended(): Unit = {
    val table1 = util.tableEnv.fromCollection(Seq((1, "hello")), 'count, 'word)
    val table2 = util.tableEnv.fromCollection(Seq((1, "hello")), 'count, 'word)
    val table = table1.unionAll(table2)

    util.verifyExplain(table)
  }
}
