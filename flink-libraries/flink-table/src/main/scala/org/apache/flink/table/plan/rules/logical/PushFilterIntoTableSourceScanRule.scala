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

package org.apache.flink.table.plan.rules.logical

import java.util

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, RexNodeExtractor}
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.FilterableTableSource
import org.apache.flink.table.sources.orc.OrcTableSource
import org.apache.flink.table.sources.parquet.ParquetTableSource

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConverters._

class PushFilterIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[Filter],
    operand(classOf[LogicalTableScan], none)),
  "PushFilterIntoTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {

    val filter: Filter = call.rel(0).asInstanceOf[Filter]
    if (filter.getCondition == null) return false
    val scan: LogicalTableScan = call.rel(1).asInstanceOf[LogicalTableScan]
    scan.getTable.unwrap(classOf[TableSourceTable]) match {
      case table: TableSourceTable =>
        table.tableSource match {
          case source: ParquetTableSource[_] if !source.isFilterPushedDown  =>
            //FIXME This is not a very elegant solution.
            val tableConfig = scan.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])
            tableConfig.getParameters.getBoolean(
              TableConfig.SQL_EXEC_SOURCE_PARQUET_ENABLE_PREDICATE_PUSHDOWN,
              TableConfig.SQL_EXEC_SOURCE_PARQUET_ENABLE_PREDICATE_PUSHDOWN_DEFAULT)
          case source: OrcTableSource[_] if !source.isFilterPushedDown =>
            val tableConfig = scan.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])
            tableConfig.getParameters.getBoolean(
              TableConfig.SQL_EXEC_SOURCE_ORC_ENABLE_PREDICATE_PUSHDOWN,
              TableConfig.SQL_EXEC_SOURCE_ORC_ENABLE_PREDICATE_PUSHDOWN_DEFAULT)
          case source: FilterableTableSource if !source.isFilterPushedDown => true
          case _ => false
        }
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0).asInstanceOf[Filter]
    val scan: LogicalTableScan = call.rel(1).asInstanceOf[LogicalTableScan]
    val table: FlinkRelOptTable = scan.getTable.asInstanceOf[FlinkRelOptTable]
    pushFilterIntoScan(call, filter, scan, table, description)
  }

  private def pushFilterIntoScan(
      call: RelOptRuleCall,
      filter: Filter,
      scan: LogicalTableScan,
      relOptTable: FlinkRelOptTable,
      description: String): Unit = {

    val relBuilder = call.builder()
    val functionCatalog = FlinkRelOptUtil.getFunctionCatalog(filter)
    val maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(scan)
    val (predicates, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        filter.getCondition,
        maxCnfNodeCount,
        filter.getInput.getRowType.getFieldNames,
        relBuilder.getRexBuilder,
        functionCatalog)

    if (predicates.isEmpty) {
      // no condition can be translated to expression
      return
    }

    val remainingPredicates = new util.LinkedList[Expression]()
    predicates.foreach(e => remainingPredicates.add(e))

    val newRelOptTable = applyPredicate(remainingPredicates, relOptTable, relBuilder)

    val newScan = new LogicalTableScan(scan.getCluster, scan.getTraitSet, newRelOptTable)

    // check whether framework still need to do a filter
    if (remainingPredicates.isEmpty && unconvertedRexNodes.isEmpty) {
      call.transformTo(newScan)
    } else {
      relBuilder.push(scan)
      val remainingConditions =
        (remainingPredicates.asScala.map(expr => expr.toRexNode(relBuilder))
            ++ unconvertedRexNodes)
      val remainingCondition = remainingConditions.reduce((l, r) => relBuilder.and(l, r))
      val newFilter = filter.copy(filter.getTraitSet, newScan, remainingCondition)
      call.transformTo(newFilter)
    }
  }

  private def applyPredicate(
      predicates: util.List[Expression],
      relOptTable: FlinkRelOptTable,
      relBuilder: RelBuilder): FlinkRelOptTable = {
    val originPredicatesSize = predicates.size()
    val tableSourceTable = relOptTable.unwrap(classOf[TableSourceTable])
    val filterableSource = tableSourceTable.tableSource.asInstanceOf[FilterableTableSource]
    filterableSource.setRelBuilder(relBuilder)
    val newTableSource = filterableSource.applyPredicate(predicates)
    val updatedPredicatesSize = predicates.size()
    val statistics = if (originPredicatesSize == updatedPredicatesSize) {
      // Keep all Statistics if no predicates can be pushed down
      tableSourceTable.statistic
    } else {
      // Only keep uniqueKeys and modified monotonicity properties after predicates pushed down
      FlinkStatistic.of(tableSourceTable.statistic.getUniqueKeys,
                        tableSourceTable.statistic.getRelModifiedMonotonicity)
    }
    val newTableSourceTable = tableSourceTable.replaceTableSource(newTableSource).copy(statistics)
    relOptTable.copy(newTableSourceTable, relOptTable.getRowType)
  }

}


object PushFilterIntoTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushFilterIntoTableSourceScanRule
}
