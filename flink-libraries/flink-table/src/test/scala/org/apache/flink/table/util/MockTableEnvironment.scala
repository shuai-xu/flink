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

package org.apache.flink.table.util

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.table.api.{QueryConfig, Table, TableConfig, TableEnvironment}
import org.apache.flink.table.plan.cost.{DataSetCost, FlinkCostFactory}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.DataType

import scala.collection.mutable

class MockTableEnvironment extends TableEnvironment(new TableConfig) {

  val tableSources = new mutable.HashMap[String, TableSource]()

  def getTableSource(name: String): TableSource = {
    tableSources.get(name).orNull
  }

  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig,
      sinkName: String): Unit = ???

  override protected def getFlinkCostFactory: FlinkCostFactory = DataSetCost.FACTORY

  override protected def checkValidTableName(name: String): Unit = ???

  override def sql(query: String): Table = ???

  override def registerTableSource(name: String, tableSource: TableSource): Unit = {
    tableSources += name -> tableSource
  }

  override def execute(): JobExecutionResult = ???

  override def execute(jobName: String): JobExecutionResult = ???

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[DataType],
      tableSink: TableSink[_]): Unit = ???

  /**
    * generate a sql text in form of bayes
    *
    * @return
    */
  override def getSqlText(): String = ""

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  override def explain(table: Table): String = ???
}
