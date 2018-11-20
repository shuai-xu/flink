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
package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTemporalTableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

class FlinkLogicalTemporalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable,
    val period: RexNode)
  extends FlinkLogicalTableSourceScan(cluster, traitSet, relOptTable)
  with FlinkLogicalRel {

  override val tableSource: TableSource =
    relOptTable.unwrap(classOf[TableSourceTable]).tableSource

  override def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): FlinkLogicalTemporalTableSourceScan = {
    new FlinkLogicalTemporalTableSourceScan(cluster, traitSet, relOptTable, period)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", tableSource.getTableSchema.getColumnNames.mkString(", "))
      .item("period", period)
  }
}


class FlinkLogicalTemporalTableSourceScanConverter
  extends ConverterRule(
    classOf[LogicalTemporalTableScan],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTemporalTableSourceScanConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[TableScan](0)
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable])
    tableSourceTable match {
      case _: TableSourceTable => true
      case _ => false
    }
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[LogicalTemporalTableScan]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val table = scan.getTable.asInstanceOf[FlinkRelOptTable]

    new FlinkLogicalTemporalTableSourceScan(
      rel.getCluster,
      traitSet,
      table,
      scan.getPeriod
    )
  }
}

object FlinkLogicalTemporalTableSourceScan {
  val CONVERTER = new FlinkLogicalTemporalTableSourceScanConverter
}
