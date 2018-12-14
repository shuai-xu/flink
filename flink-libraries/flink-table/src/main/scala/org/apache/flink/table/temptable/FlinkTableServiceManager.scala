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

import org.apache.flink.api.common.JobSubmissionResult
import org.apache.flink.service.ServiceDescriptor
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.factories.TableFactory
import org.apache.flink.table.plan.CacheAwareRelNodePlanBuilder
import org.apache.flink.table.temptable.util.TableServiceUtil
import org.apache.flink.table.util.TableProperties
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  * TableServiceManager is for maintain status of TableService and CachedTables.
  */
class FlinkTableServiceManager(tEnv: TableEnvironment) {

  private val toBeCachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  private[flink] val cachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  private var tableServiceStarted = false

  private var submitResult: JobSubmissionResult = _

  private var tableServiceEnv: StreamExecutionEnvironment = _

  private[flink] var tableServiceFactoryDescriptor:
    Option[FlinkTableServiceFactoryDescriptor] = None

  def setTableService(tableFactory: TableFactory,
                      properties: TableProperties): Unit = {
    tableServiceFactoryDescriptor = Some(
      new FlinkTableServiceFactoryDescriptor(tableFactory, properties)
    )
  }

  def getTableServiceFactory(): Option[TableFactory] = {
    if(tableServiceFactoryDescriptor == None){
      tableServiceFactoryDescriptor = Some(TableServiceUtil.getDefaultTableServiceFactoryDescriptor)
    }
    Some(tableServiceFactoryDescriptor.get.getTableFactory)
  }

  def getTableServiceFactoryProperties(): Option[TableProperties] = {
    if(tableServiceFactoryDescriptor == None){
      tableServiceFactoryDescriptor = Some(TableServiceUtil.getDefaultTableServiceFactoryDescriptor)
    }
    Some(tableServiceFactoryDescriptor.get.getTableProperties)
  }

  private[flink] var tableServiceDescriptor: Option[ServiceDescriptor] = None

  def getTableServiceDescriptor(): Option[ServiceDescriptor] = tableServiceDescriptor

  private[flink] val cachePlanBuilder: CacheAwareRelNodePlanBuilder =
    new CacheAwareRelNodePlanBuilder(tEnv)

  /**
    * Record the uuid of a table to be cached & adding Source/Sink LogicalNode
    * @param table The table to be cached.
    */
  def cacheTable(table: Table): Unit = {
    val tempTableUUID = table.toString + "-" + java.util.UUID.randomUUID()
    toBeCachedTables.put(table.logicalPlan,tempTableUUID)

    val cacheSink = cachePlanBuilder.createCacheTableSink(tempTableUUID, table.logicalPlan)
    tEnv.writeToSink(table, cacheSink)

    val cacheSource = cachePlanBuilder.createCacheTableSource(tempTableUUID, table.logicalPlan)
    tEnv.registerTableSource(tempTableUUID, cacheSource)
  }

  private[flink] def markAllTablesCached(): Unit = {
    cachedTables.putAll(toBeCachedTables)
    toBeCachedTables.clear()
  }

  private[flink] def getToBeCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(toBeCachedTables.get(logicalPlan))
  }

  private[flink] def getCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(cachedTables.get(logicalPlan))
  }

  def startTableServiceJob(): Unit = {
    if(!tableServiceStarted && !toBeCachedTables.isEmpty){
      val executionEnv = StreamExecutionEnvironment.getExecutionEnvironment
      if(tableServiceDescriptor.isEmpty) {
        tableServiceDescriptor = Some(TableServiceUtil.getDefaultFlinkTableServiceDescriptor)
      }
      val descriptor = tableServiceDescriptor.get
      TableServiceUtil.createTableServiceJob(executionEnv,descriptor)
      submitResult = executionEnv.submit("FlinkTableServiceJob")
      tableServiceEnv = executionEnv
      tableServiceStarted = true
    }
  }

  def close(): Unit = {
    if (tableServiceStarted) {
      tableServiceEnv.stopJob(submitResult.getJobID)
      tableServiceStarted = false
    }
  }

}
