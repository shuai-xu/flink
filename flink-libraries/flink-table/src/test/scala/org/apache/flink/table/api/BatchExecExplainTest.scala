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

package org.apache.flink.table.api

import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.junit._

class BatchExecExplainTest extends QueryTest {

  @Before
  def before(): Unit = {
    registerCollection("t", Seq(QueryTest.row(1, "hello")), INT_STRING, 'a, 'b)
    registerCollection("t2", Seq(QueryTest.row(1, "hello")), INT_STRING, 'c, 'd)
  }

  @Test
  def testFilter(): Unit = {
    explainQuery("select * from t where mod(a,2) = 0")
  }

  @Test
  def testJoin(): Unit = {
    explainQuery("select a, c from t, t2 where b = d")
    explainQuery("select * from t where a in (select c from t2)")
    explainQuery("select * from t where exists (select * from t2)")
    explainQuery("select * from t where exists (select * from t2 where t2.c = t.a)")
  }

  @Test
  def testUnion(): Unit = {
    explainQuery("select * from t union all (select * from t2)")
  }

  @Test
  def testOrder(): Unit = {
    explainQuery("select * from t order by a")
    explainQuery("select * from t order by a limit 5")
  }
}
