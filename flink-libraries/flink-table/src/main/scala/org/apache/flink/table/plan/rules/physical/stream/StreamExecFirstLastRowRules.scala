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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalRank, FlinkLogicalSort}
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecFirstLastRow

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

object StreamExecFirstLastRowRules {

  val SORT_INSTANCE = new StreamExecFirstLastRowFromSortRule
  val RANK_INSTANCE = new StreamExecFirstLastRowFromRankRule

  class StreamExecFirstLastRowFromSortRule
    extends ConverterRule(
      classOf[FlinkLogicalSort],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamExecFirstLastRowFromSortRule")
    with BaseStreamExecRankRule {

    override def matches(call: RelOptRuleCall): Boolean = {
      val sort: FlinkLogicalSort = call.rel(0)
      canSimplifyToFirstLastRow(sort)
    }

    override def convert(rel: RelNode): RelNode = {
      val sort = rel.asInstanceOf[FlinkLogicalSort]

      val requiredDistribution = FlinkRelDistribution.SINGLETON

      val requiredTraitSet = sort.getInput.getTraitSet
                             .replace(FlinkConventions.STREAM_PHYSICAL)
                             .replace(requiredDistribution)

      val convInput: RelNode = RelOptRule.convert(sort.getInput, requiredTraitSet)

      val fieldCollation = sort.collation.getFieldCollations.get(0)
      val fieldType = sort.getRowType.getFieldList.get(fieldCollation.getFieldIndex).getType
      val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)
      val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
      // order by timeIndicator desc ==> lastRow, otherwise is firstRow
      val isLastRow = fieldCollation.direction.isDescending
      new StreamExecFirstLastRow(
        rel.getCluster,
        providedTraitSet,
        convInput,
        Array(),
        isRowtime,
        isLastRow,
        description)
    }
  }

  class StreamExecFirstLastRowFromRankRule
    extends ConverterRule(
      classOf[FlinkLogicalRank],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamExecFirstLastRowRule")
    with BaseStreamExecRankRule {

    override def matches(call: RelOptRuleCall): Boolean = {
      val rank: FlinkLogicalRank = call.rel(0)
      canSimplifyToFirstLastRow(rank)
    }

    override def convert(rel: RelNode): RelNode = {
      val rank = rel.asInstanceOf[FlinkLogicalRank]
      val fieldCollation = rank.sortCollation.getFieldCollations.get(0)
      val fieldType = rank.getInput.getRowType.getFieldList.get(fieldCollation.getFieldIndex)
                      .getType
      val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)

      val requiredDistribution = FlinkRelDistribution.hash(rank.partitionKey.toList)
      val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
                             .replace(FlinkConventions.STREAM_PHYSICAL)
                             .replace(requiredDistribution)
      val convInput: RelNode = RelOptRule.convert(rank.getInput, requiredTraitSet)

      // order by timeIndicator desc ==> lastRow, otherwise is firstRow
      val isLastRow = fieldCollation.direction.isDescending
      new StreamExecFirstLastRow(
        rel.getCluster,
        rank.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL),
        convInput,
        rank.partitionKey.toArray,
        isRowtime,
        isLastRow,
        description)
    }
  }

}
