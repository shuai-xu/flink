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

package org.apache.flink.table.plan.cost

import java.lang.{Double => JDouble}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata._
import org.apache.flink.table.plan.cost.FlinkMetadata.ColumnNullCount
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDimensionTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.util.Preconditions

/**
  * FlinkRelMdColumnNullCount supplies a default implementation of
  * [[FlinkRelMetadataQuery.getColumnNullCount]] for the standard logical algebra.
  */
class FlinkRelMdColumnNullCount private extends MetadataHandler[ColumnNullCount] {

  override def getDef: MetadataDef[ColumnNullCount] = FlinkMetadata.ColumnNullCount.DEF

  /**
    * Gets the null count of the given column in TableScan.
    *
    * @param ts    TableScan RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column in TableScan
    */
  def getColumnNullCount(ts: TableScan, mq: RelMetadataQuery, index: Int): JDouble = {
    Preconditions.checkArgument(mq.isInstanceOf[FlinkRelMetadataQuery])
    val relOptTable = ts.getTable.asInstanceOf[FlinkRelOptTable]
    val fieldNames = relOptTable.getRowType.getFieldNames
    Preconditions.checkArgument(index >= 0 && index < fieldNames.size())
    val fieldName = fieldNames.get(index)
    val statistic = relOptTable.getFlinkStatistic
    val colStats = statistic.getColumnStats(fieldName)
    if (colStats != null && colStats.nullCount != null) colStats.nullCount.toDouble else null
  }

  /**
    * Gets the null count of the given column in FlinkLogicalDimensionTableSourceScan.
    * TODO implements it.
    * currently the estimation logic is same with BatchExecJoinTable which matches
    * method: getColumnNullCount(rel: RelNode, mq: RelMetadataQuery, index: Int).
    *
    * @param ts    TableScan RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return the null count of the given column in TableScan
    */
  def getColumnNullCount(
      ts: FlinkLogicalDimensionTableSourceScan,
      mq: RelMetadataQuery,
      index: Int): JDouble = null

  /**
    * Catches-all rule when none of the others apply.
    *
    * @param rel   RelNode to analyze
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return Always returns null
    */
  def getColumnNullCount(rel: RelNode, mq: RelMetadataQuery, index: Int): JDouble = null

}

object FlinkRelMdColumnNullCount {

  private val INSTANCE = new FlinkRelMdColumnNullCount

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.ColumnNullCount.METHOD, INSTANCE)

}
