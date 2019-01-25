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
package org.apache.flink.table.plan.rules.physical.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.common.CommonTemporalTableJoin
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalIntermediateTableScan, FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.util.TemporalJoinUtil.containsTemporalJoinCondition
import org.apache.flink.table.sources.{LookupableTableSource, TableSource}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.{RexNode, RexProgram}

trait CommonTemporalTableJoinRule {

  protected def matches(join: FlinkLogicalJoin, tableScan: TableScan): Boolean = {
    // shouldn't match temporal table function join
    if (containsTemporalJoinCondition(join.getCondition)) {
      return false
    }
    if (findTableSource(tableScan).isEmpty) {
      throw new TableException(
        "Temporal table join only support join on a TableSource " +
          "not on a DataStream or an intermediate query")
    }
    // currently temporal table join only support LookupableTableSource
    isLookupableTableSource(tableScan)
  }

  protected def findTableSource(relNode: RelNode): Option[TableSource] = {
    relNode match {
      case logicalScan: FlinkLogicalTableSourceScan => Some(logicalScan.tableSource)
      case physicalScan: PhysicalTableSourceScan => Some(physicalScan.tableSource)
      case intermediateScan: FlinkLogicalIntermediateTableScan =>
        findTableSource(intermediateScan.intermediateTable.relNode)
      case _ => None
    }
  }

  protected def isLookupableTableSource(relNode: RelNode): Boolean = {
    relNode match {
      case logicalScan: FlinkLogicalTableSourceScan =>
        logicalScan.tableSource.isInstanceOf[LookupableTableSource[_]]
      case physicalScan: PhysicalTableSourceScan =>
        physicalScan.tableSource.isInstanceOf[LookupableTableSource[_]]
      case intermediateScan: FlinkLogicalIntermediateTableScan =>
        isLookupableTableSource(intermediateScan.intermediateTable.relNode)
      case _ => false
    }
  }

  protected def transform(
      join: FlinkLogicalJoin,
      input: FlinkLogicalRel,
      tableSource: TableSource,
      period: RexNode,
      calcProgram: Option[RexProgram]): CommonTemporalTableJoin
}

abstract class BaseSnapshotOnTableScanRule(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalSnapshot],
        operand(classOf[TableScan], any()))),
    description)
  with CommonTemporalTableJoinRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val tableScan = call.rel[TableScan](3)
    matches(join, tableScan)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val input = call.rel[FlinkLogicalRel](1)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)
    val tableScan = call.rel[RelNode](3)
    val tableSource = findTableSource(tableScan).orNull

    val temporalJoin = transform(join, input, tableSource, snapshot.getPeriod, None)
    call.transformTo(temporalJoin)
  }

}

abstract class BaseSnapshotOnCalcTableScanRule(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalSnapshot],
        operand(classOf[FlinkLogicalCalc],
          operand(classOf[TableScan], any())))),
    description)
  with CommonTemporalTableJoinRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val tableScan = call.rel[TableScan](4)
    matches(join, tableScan)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val input = call.rel[FlinkLogicalRel](1)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)
    val calc = call.rel[FlinkLogicalCalc](3)
    val tableScan = call.rel[RelNode](4)
    val tableSource = findTableSource(tableScan).orNull

    val temporalJoin = transform(
      join, input, tableSource, snapshot.getPeriod, Some(calc.getProgram))
    call.transformTo(temporalJoin)
  }

}
