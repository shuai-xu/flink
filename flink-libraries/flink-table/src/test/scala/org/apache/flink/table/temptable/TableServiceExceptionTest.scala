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

package org.apache.flink.table.temptable

import java.io.File

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.junit.{Assert, Test}

class TableServiceExceptionTest {

  @Test
  def testTableServiceUnavailable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000))
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)
    tEnv.getConfig.setSubsectionOptimization(true)

    val data = List[(Int, Int)] (
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 5)
    )

    val source = tEnv.fromCollection(data).as('a, 'b)
    val filteredTable = source.filter('a < 5)

    filteredTable.cache()

    filteredTable.collect().size

    val cachedName = tEnv.tableServiceManager.getCachedTableName(filteredTable.logicalPlan).get

    val cacheFileDir =
      new File(
        TableServiceImpl.TABLESERVICE_DEFAULT_STORAGE_PATH_VALUE
          + File.separator
          + cachedName)

    // delete exist cache
    cacheFileDir.listFiles().foreach(_.delete())
    cacheFileDir.delete()
    Assert.assertTrue(!cacheFileDir.exists())

    val result = filteredTable.select('a + 1 as 'a)

    // this action will fail due to missing cache and will fallback to original plan
    val res = result.collect()

    // cache has been re-computed by original plan.
    Assert.assertTrue(cacheFileDir.exists())
    Assert.assertEquals(List(2, 3, 4, 5).mkString("\n"), res.map(_.toString).mkString("\n"))

    tEnv.close()
  }

}
