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

import java.sql.Date

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.runtime.batch.sql.QueryTest.row
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.DateTimeTestUtil.UTCTimestamp
import org.junit.{Before, Test}

import _root_.scala.collection.JavaConverters._
import scala.collection.Seq

class AggregateReduceGroupingITCase extends QueryTest {

  @Before
  def before(): Unit = {
    tEnv.registerTableSource("T1",
      CommonTestData.createCsvTableSource(
        Seq(row(2, 1, "A", null),
          row(3, 2, "A", "Hi"),
          row(5, 2, "B", "Hello"),
          row(6, 3, "C", "Hello world")),
        Array("a1", "b1", "c1", "d1"),
        Array(DataTypes.INT, DataTypes.INT, DataTypes.STRING, DataTypes.STRING)
      ),
      Set(Set("a1").asJava).asJava
    )

    tEnv.registerTableSource("T2",
      CommonTestData.createCsvTableSource(
        Seq(row(1, 1, "X"),
          row(1, 2, "Y"),
          row(2, 3, null),
          row(2, 4, "Z")),
        Array("a2", "b2", "c2"),
        Array(DataTypes.INT, DataTypes.INT, DataTypes.STRING)
      ),
      Set(Set("b2").asJava, Set("a2", "b2").asJava).asJava
    )

    tEnv.registerTableSource("T3",
      CommonTestData.createCsvTableSource(
        Seq(row(1, 10, "Hi", 1),
          row(2, 20, "Hello", 1),
          row(2, 20, "Hello world", 2),
          row(3, 10, "Hello world, how are you?", 1),
          row(4, 20, "I am fine.", 2),
          row(4, null, "Luke Skywalker", 2)),
        Array("a3", "b3", "c3", "d3"),
        Array(DataTypes.INT, DataTypes.INT, DataTypes.STRING, DataTypes.LONG),
        fieldDelim = "|"
      )
    )

    tEnv.registerTableSource("T4",
      CommonTestData.createCsvTableSource(
        Seq(row(1, 1, "A", UTCTimestamp("2018-06-01 10:05:30"), "Hi"),
          row(2, 1, "B", UTCTimestamp("2018-06-01 10:10:10"), "Hello"),
          row(3, 2, "B", UTCTimestamp("2018-06-01 10:15:25"), "Hello world"),
          row(4, 3, "C", UTCTimestamp("2018-06-01 10:36:49"), "I am fine.")),
        Array("a4", "b4", "c4", "d4", "e4"),
        Array(DataTypes.INT, DataTypes.INT, DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.STRING)
      ),
      Set(Set("a4").asJava).asJava
    )

    tEnv.registerTableSource("T5",
      CommonTestData.createCsvTableSource(
        Seq(row(2, 1, "A", null),
          row(3, 2, "B", "Hi"),
          row(1, null, "C", "Hello"),
          row(4, 3, "D", "Hello world"),
          row(3, 1, "E", "Hello world, how are you?"),
          row(5, null, "F", null),
          row(7, 2, "I", "hahaha"),
          row(6, 1, "J", "I am fine.")),
        Array("a5", "b5", "c5", "d5"),
        Array(DataTypes.INT, DataTypes.INT, DataTypes.STRING, DataTypes.STRING)
      ),
      Set(Set("c5").asJava).asJava
    )

    tEnv.registerTableSource("T6",
      CommonTestData.createCsvTableSource(
        (0 until 50000).map(
          i => row(i, 1L, if (i % 500 == 0) null else s"Hello$i", "Hello world", 10,
            new Date(i + 1531820000000L))),
        Array("a6", "b6", "c6", "d6", "e6", "f6"),
        Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING, DataTypes.STRING, DataTypes.INT,
          DataTypes.DATE)
      ),
      Set(Set("a6").asJava).asJava
    )
  }

  @Test
  def testSingleAggOnTable_SortAgg(): Unit = {
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_PHYSICAL_OPERATORS_DISABLED, "HashAgg")
    testSingleAggOnTable()
    checkResult("SELECT a6, b6, max(c6), count(d6), sum(e6) FROM T6 GROUP BY a6, b6",
      (0 until 50000).map(i => row(i, 1L, if (i % 500 == 0) null else s"Hello$i", 1L, 10))
    )
  }

  @Test
  def testSingleAggOnTable_HashAgg_WithLocalAgg(): Unit = {
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_PHYSICAL_OPERATORS_DISABLED, "SortAgg")
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_CBO_AGG_PHASE_ENFORCER, "TWO_PHASE")
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_HASH_AGG_TABLE_MEM, 2) // 1M
    testSingleAggOnTable()
  }

  @Test
  def testSingleAggOnTable_HashAgg_WithoutLocalAgg(): Unit = {
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_PHYSICAL_OPERATORS_DISABLED, "SortAgg")
    tEnv.getConfig.getConf.setString(TableConfigOptions.SQL_CBO_AGG_PHASE_ENFORCER, "ONE_PHASE")
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_HASH_AGG_TABLE_MEM, 2) // 1M
    testSingleAggOnTable()
  }

  private def testSingleAggOnTable(): Unit = {
    // group by fix length
    checkResult("SELECT a1, b1, count(c1) FROM T1 GROUP BY a1, b1",
      Seq(row(2, 1, 1), row(3, 2, 1), row(5, 2, 1), row(6, 3, 1)))
    // group by string
    checkResult("SELECT a1, c1, count(d1), avg(b1) FROM T1 GROUP BY a1, c1",
      Seq(row(2, "A", 0, 1.0), row(3, "A", 1, 2.0), row(5, "B", 1, 2.0), row(6, "C", 1, 3.0)))
    checkResult("SELECT c5, d5, avg(b5), avg(a5) FROM T5 WHERE d5 IS NOT NULL GROUP BY c5, d5",
      Seq(row("B", "Hi", 2.0, 3.0), row("C", "Hello", null, 1.0),
        row("D", "Hello world", 3.0, 4.0), row("E", "Hello world", 1.0, 3.0),
        row("I", "hahaha", 2.0, 7.0), row("J", "I am fine.", 1.0, 6.0)))
    // group by string with null
    checkResult("SELECT a1, d1, count(d1) FROM T1 GROUP BY a1, d1",
      Seq(row(2, null, 0), row(3, "Hi", 1), row(5, "Hello", 1), row(6, "Hello world", 1)))
    checkResult("SELECT c5, d5, avg(b5), avg(a5) FROM T5 GROUP BY c5, d5",
      Seq(row("A", null, 1.0, 2.0), row("B", "Hi", 2.0, 3.0), row("C", "Hello", null, 1.0),
        row("D", "Hello world", 3.0, 4.0), row("E", "Hello world", 1.0, 3.0),
        row("F", null, null, 5.0), row("I", "hahaha", 2.0, 7.0), row("J", "I am fine.", 1.0, 6.0)))

    checkResult("SELECT a3, b3, count(c3) FROM T3 GROUP BY a3, b3",
      Seq(row(1, 10, 1), row(2, 20, 2), row(3, 10, 1), row(4, 20, 1), row(4, null, 1)))
    checkResult("SELECT a2, b2, count(c2) FROM T2 GROUP BY a2, b2",
      Seq(row(1, 1, 1), row(1, 2, 1), row(2, 3, 0), row(2, 4, 1)))

    // group by constants
    checkResult("SELECT a1, b1, count(c1) FROM T1 GROUP BY a1, b1, 1, true",
      Seq(row(2, 1, 1), row(3, 2, 1), row(5, 2, 1), row(6, 3, 1)))
    checkResult("SELECT count(c1) FROM T1 GROUP BY 1, true", Seq(row(4)))

    // large data, for hash agg mode it will fallback
    checkResult("SELECT a6, c6, avg(b6), count(d6), avg(e6) FROM T6 GROUP BY a6, c6",
      (0 until 50000).map(i => row(i, if (i % 500 == 0) null else s"Hello$i", 1D, 1L, 10D))
    )
    checkResult("SELECT a6, d6, avg(b6), count(c6), avg(e6) FROM T6 GROUP BY a6, d6",
      (0 until 50000).map(i => row(i, "Hello world", 1D, if (i % 500 == 0) 0L else 1L, 10D))
    )
    checkResult("SELECT a6, f6, avg(b6), count(c6), avg(e6) FROM T6 GROUP BY a6, f6",
      (0 until 50000).map(i => row(i, new Date(i + 1531820000000L), 1D,
        if (i % 500 == 0) 0L else 1L, 10D))
    )
  }

  @Test
  def testMultiAggs(): Unit = {
    checkResult("SELECT a1, b1, c1, d1, m, COUNT(*) FROM " +
      "(SELECT a1, b1, c1, COUNT(d1) AS d1, MAX(d1) AS m FROM T1 GROUP BY a1, b1, c1) t " +
      "GROUP BY a1, b1, c1, d1, m",
      Seq(row(2, 1, "A", 0, null, 1), row(3, 2, "A", 1, "Hi", 1),
        row(5, 2, "B", 1, "Hello", 1), row(6, 3, "C", 1, "Hello world", 1)))

    checkResult("SELECT a3, b3, c, s, COUNT(*) FROM " +
      "(SELECT a3, b3, COUNT(d3) AS c, SUM(d3) AS s, MAX(d3) AS m FROM T3 GROUP BY a3, b3) t " +
      "GROUP BY a3, b3, c, s",
      Seq(row(1, 10, 1, 1, 1), row(2, 20, 2, 3, 1), row(3, 10, 1, 1, 1),
        row(4, 20, 1, 2, 1), row(4, null, 1, 2, 1)))
  }

  @Test
  def testAggOnInnerJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1, T2 WHERE a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1)))

    checkResult("SELECT a2, b2, a3, b3, COUNT(c2) FROM " +
      "(SELECT * FROM T2, T3 WHERE b2 = a3) t GROUP BY a2, b2, a3, b3",
      Seq(row(1, 1, 1, 10, 1), row(1, 2, 2, 20, 2), row(2, 3, 3, 10, 0),
        row(2, 4, 4, 20, 1), row(2, 4, 4, null, 1)))

    checkResult("SELECT a1, b1, a2, b2, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1, T2, T3 WHERE a1 = b2 AND a1 = a3) t GROUP BY a1, b1, a2, b2, a3, b3",
      Seq(row(2, 1, 1, 2, 2, 20, 2), row(3, 2, 2, 3, 3, 10, 1)))
  }

  @Test
  def testAggOnLeftJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 LEFT JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1),
        row(5, 2, null, null, 1), row(6, 3, null, null, 1)))

    checkResult("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 LEFT JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3",
      Seq(row(2, 1, 2, 20, 2), row(3, 2, 3, 10, 1),
        row(5, 2, null, null, 1), row(6, 3, null, null, 1)))

    checkResult("SELECT a3, b3, a1, b1, COUNT(c1) FROM " +
      "(SELECT * FROM T3 LEFT JOIN T1 ON a1 = a3) t GROUP BY a3, b3, a1, b1",
      Seq(row(1, 10, null, null, 0), row(2, 20, 2, 1, 2), row(3, 10, 3, 2, 1),
        row(4, 20, null, null, 0), row(4, null, null, null, 0)))
  }

  @Test
  def testAggOnRightJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 RIGHT JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1),
        row(null, null, 1, 1, 0), row(null, null, 2, 4, 0)))

    checkResult("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 RIGHT JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3",
      Seq(row(2, 1, 2, 20, 2), row(3, 2, 3, 10, 1), row(null, null, 1, 10, 0),
        row(null, null, 4, 20, 0), row(null, null, 4, null, 0)))

    checkResult("SELECT a3, b3, a1, b1, COUNT(c1) FROM " +
      "(SELECT * FROM T3 RIGHT JOIN T1 ON a1 = a3) t GROUP BY a3, b3, a1, b1",
      Seq(row(2, 20, 2, 1, 2), row(3, 10, 3, 2, 1),
        row(null, null, 5, 2, 1), row(null, null, 6, 3, 1)))
  }

  @Test
  def testAggOnFullJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 FULL OUTER JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1), row(5, 2, null, null, 1),
        row(6, 3, null, null, 1), row(null, null, 1, 1, 0), row(null, null, 2, 4, 0)))

    checkResult("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 FULL OUTER JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3",
      Seq(row(2, 1, 2, 20, 2), row(3, 2, 3, 10, 1), row(5, 2, null, null, 1),
        row(6, 3, null, null, 1), row(null, null, 1, 10, 0), row(null, null, 4, 20, 0),
        row(null, null, 4, null, 0)))
  }

  @Test
  def testAggOnOver(): Unit = {
    checkResult("SELECT a1, b1, c, COUNT(d1) FROM " +
      "(SELECT a1, b1, d1, COUNT(*) OVER (PARTITION BY c1) AS c FROM T1) t GROUP BY a1, b1, c",
      Seq(row(2, 1, 2, 0), row(3, 2, 2, 1), row(5, 2, 1, 1), row(6, 3, 1, 1)))
  }

  @Test
  def testAggOnWindow(): Unit = {
    checkResult("SELECT a4, b4, COUNT(c4) FROM T4 " +
      "GROUP BY a4, b4, TUMBLE(d4, INTERVAL '15' MINUTE)",
      Seq(row(1, 1, 1), row(2, 1, 1), row(3, 2, 1), row(4, 3, 1)))

    checkResult("SELECT a4, c4, COUNT(b4), AVG(b4) FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)",
      Seq(row(1, "A", 1, 1.0), row(2, "B", 1, 1.0), row(3, "B", 1, 2.0), row(4, "C", 1, 3.0)))

    checkResult("SELECT a4, e4, s, avg(ab), count(cb) FROM " +
      "(SELECT a4, e4, avg(b4) as ab, count(b4) AS cb, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, e4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, e4, s",
      Seq(row(1, "Hi", UTCTimestamp("2018-06-01 10:00:00.0"), 1D, 1),
        row(2, "Hello", UTCTimestamp("2018-06-01 10:00:00.0"), 1D, 1),
        row(3, "Hello world", UTCTimestamp("2018-06-01 10:15:00.0"), 2D, 1),
        row(4, "I am fine.", UTCTimestamp("2018-06-01 10:30:00.0"), 3D, 1)))

    checkResult("SELECT a4, c4, s, COUNT(b4) FROM " +
      "(SELECT a4, c4, avg(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, c4, s",
      Seq(row(1, "A", UTCTimestamp("2018-06-01 10:00:00.0"), 1),
        row(2, "B", UTCTimestamp("2018-06-01 10:00:00.0"), 1),
        row(3, "B", UTCTimestamp("2018-06-01 10:15:00.0"), 1),
        row(4, "C", UTCTimestamp("2018-06-01 10:30:00.0"), 1)))

    checkResult("SELECT a4, c4, e, COUNT(b4) FROM " +
      "(SELECT a4, c4, VAR_POP(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, c4, e",
      Seq(row(1, "A", UTCTimestamp("2018-06-01 10:15:00.0"), 1),
        row(2, "B", UTCTimestamp("2018-06-01 10:15:00.0"), 1),
        row(3, "B", UTCTimestamp("2018-06-01 10:30:00.0"), 1),
        row(4, "C", UTCTimestamp("2018-06-01 10:45:00.0"), 1)))

    checkResult("SELECT a4, b4, c4, COUNT(*) FROM " +
      "(SELECT a4, c4, SUM(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, b4, c4",
      Seq(row(1, 1, "A", 1), row(2, 1, "B", 1), row(3, 2, "B", 1), row(4, 3, "C", 1)))
  }

  @Test
  def testAggWithGroupingSets(): Unit = {
    checkResult("SELECT a1, b1, c1, count(d1) FROM T1 " +
      "GROUP BY GROUPING SETS ((a1, b1), (a1, c1))",
      Seq(row(2, 1, null, 0), row(2, null, "A", 0), row(3, 2, null, 1),
        row(3, null, "A", 1), row(5, 2, null, 1), row(5, null, "B", 1),
        row(6, 3, null, 1), row(6, null, "C", 1)))
  }

  @Test
  def testDistinctAgg(): Unit = {
    checkResult("SELECT a1, b1, COUNT(DISTINCT c1), COUNT(DISTINCT d1) FROM T1 GROUP BY a1, b1",
      Seq(row(2, 1, 1, 0), row(3, 2, 1, 1), row(5, 2, 1, 1), row(6, 3, 1, 1)))
  }

}
