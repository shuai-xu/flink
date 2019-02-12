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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.sql.SqlKind
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecLastRow
import org.apache.flink.table.plan.util.ConstantRankRange

class StreamExecFirstLastRowRule
  extends ConverterRule(
    classOf[FlinkLogicalRank],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecFirstLastRowRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    val sortCollation = rank.sortCollation
    val rankRange = rank.rankRange

    val isRowNumberFunction = rank.rankFunction.getKind == SqlKind.ROW_NUMBER

    val limit1 = rankRange match {
      case ConstantRankRange(rankStart, rankEnd) => rankStart == 1 && rankEnd == 1
      case _ => false
    }

    val inputRowType = rank.getInput.getRowType
    val sortOnTimeAttributeAndLastRow =
      if (sortCollation.getFieldCollations.isEmpty || sortCollation.getFieldCollations.size() > 1) {
        false
      } else {
        val fieldCollation = sortCollation.getFieldCollations.get(0)
        val fieldType = inputRowType.getFieldList.get(fieldCollation.getFieldIndex).getType

        // TODO: support FirstRow in the future
        val isLastRow = fieldCollation.direction.isDescending
        FlinkTypeFactory.isTimeIndicatorType(fieldType) && isLastRow
      }

    // LastRow must sort on rowtime/proctime desc and fetch the top1 row
    limit1 && sortOnTimeAttributeAndLastRow && isRowNumberFunction
  }

  override def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val fieldCollation = rank.sortCollation.getFieldCollations.get(0)
    val fieldType = rank.getInput.getRowType.getFieldList.get(fieldCollation.getFieldIndex).getType
    // order by timeIndicator desc ==> lastRow, otherwise is firstRow
    val isLastRow = fieldCollation.direction.isDescending
    val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)

    val requiredDistribution = FlinkRelDistribution.hash(rank.partitionKey.toList)
    val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val convInput: RelNode = RelOptRule.convert(rank.getInput, requiredTraitSet)

    if (isLastRow) {
      new StreamExecLastRow(
        rel.getCluster,
        rank.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL),
        convInput,
        rank.partitionKey.toArray,
        isRowtime,
        description)
    } else {
      throw new TableException("Support FirstRow in the future")
    }
  }
}

object StreamExecFirstLastRowRule {
  val INSTANCE = new StreamExecFirstLastRowRule
}
