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
import org.apache.flink.table.plan.util.JoinTableUtil
import org.apache.flink.table.sources.{DimensionTableSource, TableSource}

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexNode, RexProgram}

class FlinkLogicalDimensionTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable,
    val period: Option[RexNode] = None,
    val calcProgram: Option[RexProgram] = None)
  extends FlinkLogicalTableSourceScan(cluster, traitSet, relOptTable)
  with FlinkLogicalRel {

  override val tableSource: TableSource =
    relOptTable.unwrap(classOf[TableSourceTable]).tableSource

  override def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): FlinkLogicalDimensionTableSourceScan = {
    new FlinkLogicalDimensionTableSourceScan(cluster, traitSet, relOptTable, period, calcProgram)
  }

  override def deriveRowType(): RelDataType = {
    // use calcProgram's output row type if not none
    calcProgram match {
      case Some(_) => calcProgram.get.getOutputRowType
      case None => super.deriveRowType()
    }
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
      .item("calcProgram", calcProgram)
  }

  override def isDeterministic: Boolean = JoinTableUtil.isDeterministic(calcProgram, period, null)

}

class FlinkLogicalDimensionTableSourceScanConverter(scanClass: Class[_ <: TableScan])
  extends ConverterRule(
    scanClass,
    FlinkConventions.LOGICAL,
    FlinkConventions.LOGICAL,
    s"${scanClass.getSimpleName}2DimTableConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan = call.rel[TableScan](0)
    // matches FlinkLogicalTemporalTableSourceScan or FlinkLogicalTableSourceScan
    // and NEVER convert a FlinkLogicalDimensionTableSourceScan
    !scan.isInstanceOf[FlinkLogicalDimensionTableSourceScan] && isDimensionTableSourceScan(scan)
  }

  private[flink] def isDimensionTableSourceScan(relNode: RelNode): Boolean = relNode match {
    case temporal: FlinkLogicalTemporalTableSourceScan =>
      temporal.tableSource.isInstanceOf[DimensionTableSource[_]]
    case ts: FlinkLogicalTableSourceScan =>
      ts.tableSource.isInstanceOf[DimensionTableSource[_]]
    case _ => false
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalTableSourceScan]
    val cluster = rel.getCluster
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val table = scan.getTable.asInstanceOf[FlinkRelOptTable]

    val newScan = scan match {
      case temporal: FlinkLogicalTemporalTableSourceScan =>
        new FlinkLogicalDimensionTableSourceScan(
          cluster,
          traitSet,
          table,
          Some(temporal.period)
        )
      case _ =>
        new FlinkLogicalDimensionTableSourceScan(
          cluster,
          traitSet,
          table
        )
    }
    newScan
  }
}

object FlinkLogicalDimensionTableSourceScan {
  val TEMPORAL_CONVERTER =
    new FlinkLogicalDimensionTableSourceScanConverter(classOf[FlinkLogicalTemporalTableSourceScan])
  val STATIC_CONVERTER =
    new FlinkLogicalDimensionTableSourceScanConverter(classOf[FlinkLogicalTableSourceScan])
}
