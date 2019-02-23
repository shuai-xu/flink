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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.common.CommonTemporalTableJoin
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTemporalTableJoin
import org.apache.flink.table.plan.rules.physical.common.{BaseSnapshotOnCalcTableScanRule, BaseSnapshotOnTableScanRule}
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rex.{RexNode, RexProgram}

object BatchExecTemporalTableJoinRule {
  val SNAPSHOT_ON_TABLESCAN: RelOptRule = new SnapshotOnTableScanRule
  val SNAPSHOT_ON_CALC_TABLESCAN: RelOptRule = new SnapshotOnCalcTableScanRule

  class SnapshotOnTableScanRule
    extends BaseSnapshotOnTableScanRule("BatchExecSnapshotOnTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        tableSource: TableSource,
        period: RexNode,
        calcProgram: Option[RexProgram]): CommonTemporalTableJoin = {
      doTransform(join, input, tableSource, period, calcProgram)
    }
  }

  class SnapshotOnCalcTableScanRule
    extends BaseSnapshotOnCalcTableScanRule("BatchExecSnapshotOnCalcTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        tableSource: TableSource,
        period: RexNode,
        calcProgram: Option[RexProgram]): CommonTemporalTableJoin = {
      doTransform(join, input, tableSource, period, calcProgram)
    }

  }

  private def doTransform(
      join: FlinkLogicalJoin,
      input: FlinkLogicalRel,
      tableSource: TableSource,
      period: RexNode,
      calcProgram: Option[RexProgram]): BatchExecTemporalTableJoin = {

    val joinInfo = join.analyzeCondition

    val cluster = join.getCluster
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val tableRowType = typeFactory.buildLogicalRowType(
      tableSource.getTableSchema, isStreaming = Option.apply(false))

    val providedTrait = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    var requiredTrait = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    // if partitioning enabled, use the join key as partition key
    tableSource match {
      case ps: DefinedDistribution if ps.getPartitionFields() != null &&
        !joinInfo.pairs().isEmpty =>
        requiredTrait = requiredTrait.plus(FlinkRelDistribution.hash(joinInfo.leftKeys))
      case _ =>
    }

    val convInput = RelOptRule.convert(input, requiredTrait)
    new BatchExecTemporalTableJoin(
      cluster,
      providedTrait,
      convInput,
      tableSource,
      tableRowType,
      calcProgram,
      period,
      joinInfo,
      join.getJoinType)
  }
}
