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

package org.apache.flink.table.plan.schema

import org.apache.calcite.schema.Statistic
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource

/** Table which defines an external table via a [[TableSource]] */
class TableSourceTable[T](
    val tableSource: TableSource,
    override val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends FlinkTable (
    tableSource.getReturnType,
    tableSource.getTableSchema,
    statistic) {

  /**
    * Returns statistics of current table.
    * Note: If there is no available tableStats yet, try to fetch the table Stats by calling
    * getTableStats method of tableSource.
    *
    * @return statistics of current table
    */
  override def getStatistic: Statistic = {
    // Currently, we could get more exact TableStats by AnalyzeStatistic#generateTableStats
    // and update it by TableEnvironment#alterTableStats.
    // So the default statistic should be prior to the stats from TableSource.
    if (statistic != null && statistic != FlinkStatistic.UNKNOWN) {
      if (statistic.getTableStats != null) {
        statistic
      } else {
        val stats = tableSource.getTableStats
        FlinkStatistic.of(stats, statistic.getUniqueKeys, statistic.getSkewInfo)
      }
    } else {
      val stats = tableSource.getTableStats
      val primaryKeys = tableSchema.getPrimaryKeys.map(_.name)
      if (primaryKeys.isEmpty) {
        FlinkStatistic.of(stats)
      } else {
        val uniqueKeys = ImmutableSet.of(ImmutableSet.copyOf(primaryKeys))
        FlinkStatistic.of(stats, uniqueKeys, null)
      }
    }
  }

  override def copy(statistic: FlinkStatistic): FlinkTable = {
    new TableSourceTable[T](tableSource, statistic)
  }
}
