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

package org.apache.flink.table.runtime.batchexec.sql

import java.io.File
import java.util.UUID

import org.apache.flink.configuration.{ConfigConstants, TaskManagerOptions}
import org.apache.flink.runtime.io.network.partition.external.ExternalShuffleUtils
import org.apache.flink.runtime.io.network.partition.external.yarnShuffleService.YarnShuffleServiceOptions
import org.apache.flink.runtime.util.MockYarnShuffleService
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.runtime.batchexec.sql.PartitionShufflerMode.{ExternalPartitionMergeShuffler, PartitionShufflerMode, PipelinePartitionShuffler, mockApp, mockOutput, mockRootDir, mockUser}
import org.apache.flink.util.NetUtils
import org.apache.hadoop.fs.FileUtil
import org.junit.{After, Before}

class OptionalModeQueryTest(PartitionShufflerMode: PartitionShufflerMode) extends QueryTest {

  override protected def generatorTestEnv: StreamExecutionEnvironment = {
    PartitionShufflerMode match {
      case PipelinePartitionShuffler => super.generatorTestEnv
      case _ =>
        val rootDir = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID + "/"
        val dirs = (1 to 3).map{ index =>
          val output = rootDir + "/" + index + "/"
          val file = new File(output)
          if (!file.exists) file.mkdir()
          output
        }
        val externalPath =  dirs.mkString(",")
        val appendPath = ExternalShuffleUtils.generateRelativeLocalAppDir(
          mockUser, mockApp)
        val jobExternalPath = dirs.map(_ + appendPath).mkString(",")
        val externalPort = NetUtils.getAvailablePort
        conf.getParameters.setBoolean(
          TableConfig.SQL_EXEC_EXTERNAL_SHUFFLE_ENABLED, true
        )
        //Limit operator memory
        conf.getParameters.setInteger(
          TableConfig.SQL_EXEC_EXTERNAL_SHUFFLE_SORT_BUFFER_MEM, 2)
        conf.getParameters.setInteger(
          TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM, 2)
        conf.getParameters.setInteger(
          TableConfig.SQL_EXEC_HASH_JOIN_TABLE_MEM, 2)
        conf.getParameters.setInteger(
          TableConfig.SQL_EXEC_EXTERNAL_BUFFER_MEM, 2)

        //Limit parallelism because more parallelism maybe cause insufficient memory.
        val parallelism = Math.min(4, Runtime.getRuntime.availableProcessors)
        jobConfig.setString(ConfigConstants.YARN_APP_ID, mockApp)
        jobConfig.setString(mockOutput, externalPath)
        jobConfig.setString(mockRootDir, rootDir)
        jobConfig.setInteger(
 YarnShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY, externalPort)
        jobConfig.setString(ConfigConstants.TASK_MANAGER_LOCAL_SHUFFLE_DIRS_KEY, jobExternalPath)
        if (PartitionShufflerMode == ExternalPartitionMergeShuffler) {
          //force external shuffler based partition merge process.
          jobConfig.setInteger(
            TaskManagerOptions.YARN_SHUFFLE_SERVICE_MERGE_FACTOR, 1)
        }
        StreamExecutionEnvironment.createFlip6LocalEnvironment(jobConfig, parallelism)
    }
  }

  var externalShuffleService: MockYarnShuffleService = null.asInstanceOf[MockYarnShuffleService]

  @Before
  def before(): Unit = {
    if (PartitionShufflerMode != PipelinePartitionShuffler) {
      val externalPort: Int = jobConfig.getInteger(
        YarnShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY.key(), 51810)
      externalShuffleService = new MockYarnShuffleService(
        jobConfig.getString(mockOutput, ""), mockUser, mockApp, externalPort, 5, false)
      externalShuffleService.startYarnShuffleServer()
    }
    prepareOp()
  }

  @After
  def after(): Unit = {
    afterOp()
    if (externalShuffleService != null) {
      externalShuffleService.stopYarnShuffleServer()
      externalShuffleService = null.asInstanceOf[MockYarnShuffleService]
    }
    val file = new File(jobConfig.getString(mockRootDir, ""))
    if (file.exists) FileUtil.fullyDelete(file)
  }

  def prepareOp(): Unit = {}
  def afterOp(): Unit = {}
}

object PartitionShufflerMode extends Enumeration {
  val mockUser = "user"
  val mockApp = "appId"
  val mockOutput = "external.path"
  val mockRootDir = "root.dir"
  type PartitionShufflerMode = Value
  val PipelinePartitionShuffler, ExternalPartitionShuffler, ExternalPartitionMergeShuffler =
 Value
}

