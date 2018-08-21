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

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.{RelFieldCollation, RelNode}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.{LogicalSegmentTop, SegmentTop}

class FlinkLogicalSegmentTop(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    groupKeys: ImmutableBitSet,
    fieldCollation: RelFieldCollation,
    withTies: ImmutableBitSet)
  extends SegmentTop(cluster, traits, input, groupKeys, fieldCollation, withTies)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new FlinkLogicalSegmentTop(cluster, traitSet, inputs.get(0), groupKeys,
      fieldCollation, withTies)
  }
}

private class FlinkLogicalSegmentTopConverter extends ConverterRule(
  classOf[LogicalSegmentTop],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalSegmentTopConverter") {
  override def convert(rel: RelNode): RelNode = {
    val segmentTop = rel.asInstanceOf[LogicalSegmentTop]
    val newInput = RelOptRule.convert(segmentTop.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalSegmentTop.create(
      newInput,
      segmentTop.groupKeys,
      segmentTop.fieldCollation,
      segmentTop.withTies)
  }
}

object FlinkLogicalSegmentTop {
  val CONVERTER: ConverterRule = new FlinkLogicalSegmentTopConverter

  def create(
    input: RelNode,
    groupKeys: ImmutableBitSet,
    fieldCollation: RelFieldCollation,
    withTies: ImmutableBitSet): FlinkLogicalSegmentTop = {
    val cluster = input.getCluster
    val traits = cluster.traitSetOf(Convention.NONE)
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalSegmentTop to
    // calculate the distribution trait.
    val segmentTop = new FlinkLogicalSegmentTop(cluster, traits, input, groupKeys,
      fieldCollation, withTies)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(segmentTop)
      .replace(FlinkConventions.LOGICAL).simplify()
    segmentTop.copy(newTraitSet, segmentTop.getInputs).asInstanceOf[FlinkLogicalSegmentTop]
  }
}
