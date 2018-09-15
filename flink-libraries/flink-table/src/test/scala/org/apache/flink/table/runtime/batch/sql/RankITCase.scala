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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.batch.sql.QueryTest.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.junit._

import scala.collection.Seq

class RankITCase extends QueryTest {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM, 3)
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
  }

  @Test
  def testRankValueFilterWithUpperValue(): Unit = {
    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM Table3) t " +
        "WHERE rk <= 2",
      Seq(row(1, 1, 1), row(2, 2, 1), row(3, 2, 2), row(4, 3, 1), row(5, 3, 2), row(7, 4, 1),
        row(8, 4, 2), row(11, 5, 1), row(12, 5, 2), row(16, 6, 1), row(17, 6, 2)))
  }

  @Test
  def testRankValueFilterWithRange(): Unit = {
    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM Table3) t " +
        "WHERE rk <= 4 and rk > 1",
      Seq(row(3, 2, 2), row(5, 3, 2), row(6, 3, 3), row(8, 4, 2), row(9, 4, 3), row(10, 4, 4),
        row(12, 5, 2), row(13, 5, 3), row(14, 5, 4), row(17, 6, 2), row(18, 6, 3), row(19, 6, 4)))

    checkResult(
      "SELECT a, b FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM Table3) t " +
        "WHERE rk <= 3 and rk > -2",
      Seq(row(1, 1), row(2, 2), row(3, 2), row(4, 3), row(5, 3), row(6, 3), row(7, 4), row(8, 4),
        row(9, 4), row(11, 5), row(12, 5), row(13, 5), row(16, 6), row(17, 6), row(18, 6)))
  }

  @Test
  def testRankValueFilterWithLowerValue(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM Table3) t " +
      "WHERE rk > 2",
      Seq(row(6, 3, 3), row(9, 4, 3), row(10, 4, 4), row(13, 5, 3), row(14, 5, 4), row(15, 5, 5),
        row(18, 6, 3), row(19, 6, 4), row(20, 6, 5), row(21, 6, 6)))
  }

  @Test
  def testRankValueFilterWithEquals(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM Table3) t " +
      "WHERE rk = 2",
      Seq(row(3, 2, 2), row(5, 3, 2), row(8, 4, 2), row(12, 5, 2), row(17, 6, 2)))
  }

  @Test
  def testWithoutPartitionBy(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (ORDER BY a) rk FROM Table3) t " +
      "WHERE rk < 5",
      Seq(row(1, 1, 1), row(2, 2, 2), row(3, 2, 3), row(4, 3, 4)))
  }

  @Test
  def testMultiSameRankFunctionsWithSameGroup(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk1, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk2 FROM Table3) t " +
      "WHERE rk1 < 3",
      Seq(row(1, 1, 1, 1), row(2, 2, 1, 1), row(3, 2, 2, 2), row(4, 3, 1, 1),
        row(5, 3, 2, 2), row(7, 4, 1, 1), row(8, 4, 2, 2), row(11, 5, 1, 1),
        row(12, 5, 2, 2), row(16, 6, 1, 1), row(17, 6, 2, 2)))
  }

}
