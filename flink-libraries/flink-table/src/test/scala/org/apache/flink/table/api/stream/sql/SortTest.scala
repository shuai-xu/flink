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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class SortTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c,
      'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testSortProcessingTime(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime, c")
  }

  @Test
  def testSortRowTime(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime, c")
  }

  @Test
  def testSortProcessingTimeWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 2")
  }

  @Test
  def testSortRowTimeWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 2")
  }

  @Test
  def testSortProcessingTimeDesc(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime desc, c")
  }

  @Test
  def testSortRowTimeDesc(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime desc, c")
  }

  @Test
  def testSortProcessingTimeDescWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime desc, c LIMIT 2")
  }

  @Test
  def testSortRowTimeDescWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime desc, c LIMIT 2")
  }

  @Test
  def testSortProcessingTimeSecond(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime")
  }

  @Test
  def testSortRowTimeSecond(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime")
  }

  @Test
  def testSortProcessingTimeSecondWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime LIMIT 2")
  }

  @Test
  def testSortRowTimeSecondWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime LIMIT 2")
  }

  @Test
  def testSortProcessingTimeSecondDesc(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime desc")
  }

  @Test
  def testSortRowTimeSecondDesc(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime desc")
  }

  @Test
  def testSortProcessingTimeSecondDescWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime desc LIMIT 2")
  }

  @Test
  def testSortRowTimeDescSecondWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime desc LIMIT 2")
  }

  @Test
  def testSortProcessingTimeSecondDescWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime desc LIMIT 1")
  }

  @Test
  def testSortProcessingTimeSecondAscWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime LIMIT 1")
  }

  @Test
  def testSortProcessingTimeFirstDescWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime desc LIMIT 1")
  }

  @Test
  def testSortProcessingTimeFirstAscWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 1")
  }

  @Test
  def testSortProcessingTimeFirstDescWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime, c desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeFirstAscWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeAscWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime LIMIT 1")
  }

  @Test
  def testSortProcessingTimeDescWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeAscWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeDescWithOffSet0AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime desc LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortProcessingTimeAscWithOffSet0AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY proctime LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortRowTimeSecondDescWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime desc LIMIT 1")
  }

  @Test
  def testSortRowTimeSecondAscWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstDescWithLimit1(): Unit = {
    // FirstLastRow does not support sort on rowtime now, so it is translated to Rank currently
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime desc LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstAscWithLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstDescWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime, c desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeFirstAscWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeAscWithLimit1(): Unit = {
    // FirstLastRow does not support sort on rowtime now, so it is translated to Rank currently
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 1")
  }

  @Test
  def testSortRowTimeDescWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeAscWithOffSet1AndLimit1(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeDescWithOffSet0AndLimit1(): Unit = {
    // FirstLastRow does not support sort on rowtime now, so it is translated to Rank currently
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime desc LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortRowTimeAscWithOffSet0AndLimit1(): Unit = {
    // FirstLastRow does not support sort on rowtime now, so it is translated to Rank currently
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortWithOutTime(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c")
  }

  @Test
  def testSortWithOutTimeWithLimit(): Unit = {
    streamUtil.verifyPlan("SELECT a FROM MyTable ORDER BY c LIMIT 2")
  }

  @Test
  def testLimitWithRowTime(): Unit = {
    streamUtil.verifyPlan("SELECT rowtime, c FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithProcessingTime(): Unit = {
    streamUtil.verifyPlan("SELECT proctime, c FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithRowTimeSecond(): Unit = {
    streamUtil.verifyPlan("SELECT c, rowtime FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithProcessingTimeSecond(): Unit = {
    streamUtil.verifyPlan("SELECT c, proctime FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithRowTimeDesc(): Unit = {
    streamUtil.verifyPlan("SELECT rowtime desc, c FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithProcessingTimeDesc(): Unit = {
    streamUtil.verifyPlan("SELECT proctime desc, c FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithRowTimeDescSecond(): Unit = {
    streamUtil.verifyPlan("SELECT c, rowtime desc FROM MyTable LIMIT 2")
  }

  @Test
  def testLimitWithProcessingTimeDescSecond(): Unit = {
    streamUtil.verifyPlan("SELECT c, proctime desc FROM MyTable LIMIT 2")
  }
}
