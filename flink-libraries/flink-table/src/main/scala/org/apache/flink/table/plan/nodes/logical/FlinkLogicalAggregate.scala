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

import java.util.{List => JList}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule._
import org.apache.flink.table.plan.util.PartialFinalType

import scala.collection.JavaConversions._

class FlinkLogicalAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: JList[ImmutableBitSet],
    aggCalls: JList[AggregateCall],
    /* flag indicating whether to skip StreamExecSplitAggregateRule */
    var partialFinal: PartialFinalType = PartialFinalType.NORMAL)
  extends Aggregate(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls)
  with FlinkLogicalRel {

  def setPartialFinal(partialFinal: PartialFinalType): Unit = this.partialFinal = partialFinal

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      indicator: Boolean,
      groupSet: ImmutableBitSet,
      groupSets: JList[ImmutableBitSet],
      aggCalls: JList[AggregateCall]): Aggregate = {
    new FlinkLogicalAggregate(
      cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls, partialFinal)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    if (getGroupSets.size > 1 || getGroupIdExprIndexes(getAggCallList).nonEmpty) {
      planner.getCostFactory.makeInfiniteCost()
    } else {
      val child = this.getInput
      val rowCnt = mq.getRowCount(child)
      val rowSize = mq.getAverageRowSize(child)
      val aggCnt = this.getAggCallList.size
      // group by CPU cost(multiple by 1.1 to encourage less group keys) + agg call CPU cost
      val cpuCost: Double = rowCnt * getGroupCount * 1.1 + rowCnt * aggCnt
      planner.getCostFactory.makeCost(rowCnt, cpuCost, rowCnt * rowSize)
    }
  }
}

private class FlinkLogicalAggregateBatchConverter
  extends ConverterRule(
    classOf[LogicalAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalAggregateBatchConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]

    // we do not support these functions natively
    // they have to be converted using the FlinkAggregateReduceFunctionsRule
    val supported = agg.getAggCallList.map(_.getAggregation.getKind).forall {
      // we support AVG
      case SqlKind.AVG => true
      // but none of the other AVG agg functions
      case k if SqlKind.AVG_AGG_FUNCTIONS.contains(k) => false
      case _ => true
    }

    !agg.containsAccurateDistinctCall() && supported
  }

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalAggregate]
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalAggregate.create(
      newInput,
      agg.indicator,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList)
  }
}

private class FlinkLogicalAggregateStreamConverter
  extends ConverterRule(
    classOf[LogicalAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalAggregateStreamConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]

    // we do not support these functions natively
    // they have to be converted using the FlinkAggregateReduceFunctionsRule
    agg.getAggCallList.map(_.getAggregation.getKind).forall {
      case SqlKind.STDDEV_POP | SqlKind.STDDEV_SAMP | SqlKind.VAR_POP | SqlKind.VAR_SAMP => false
      case _ => true
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalAggregate]
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalAggregate.create(
      newInput,
      agg.indicator,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList)
  }
}

object FlinkLogicalAggregate {
  val BATCH_CONVERTER: ConverterRule = new FlinkLogicalAggregateBatchConverter()
  val STREAM_CONVERTER: ConverterRule = new FlinkLogicalAggregateStreamConverter()

  def create(
      input: RelNode,
      indicator: Boolean,
      groupSet: ImmutableBitSet,
      groupSets: JList[ImmutableBitSet],
      aggCalls: JList[AggregateCall]): FlinkLogicalAggregate = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSetOf(Convention.NONE)
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalAggregate to
    // calculate the distribution trait
    val logicalAggregate = new FlinkLogicalAggregate(
      cluster,
      traitSet,
      input,
      indicator,
      groupSet,
      groupSets,
      aggCalls)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(logicalAggregate)
      .replace(FlinkConventions.LOGICAL).simplify()
    logicalAggregate.copy(newTraitSet, logicalAggregate.getInputs)
      .asInstanceOf[FlinkLogicalAggregate]
  }
}
