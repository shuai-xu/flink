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

import java.util.{ArrayList => JArrayList}

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.{RelCollations, RelFieldCollation, RelNode}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSegmentTop
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSegmentTop

class BatchExecSegmentTopRule extends ConverterRule(
  classOf[FlinkLogicalSegmentTop],
  FlinkConventions.LOGICAL,
  FlinkConventions.BATCHEXEC,
  "BatchExecSegmentTopRule") {

  override def convert(rel: RelNode): RelNode = {
    val segmentTop = rel.asInstanceOf[FlinkLogicalSegmentTop]

    val groupKeys = segmentTop.groupKeys
    val fieldCollation = segmentTop.fieldCollation

    val globalDistribution = if (groupKeys.isEmpty) {
      FlinkRelDistribution.SINGLETON
    } else {
      FlinkRelDistribution.hash(groupKeys.asList)
    }
    val requiredTraitSet = segmentTop.getTraitSet
      .replace(FlinkConventions.BATCHEXEC)
      .replace(globalDistribution)
      .replace(RelCollations.EMPTY) // Replace the RelCollation with EMPTY.
      .replace(createRelCollation(groupKeys.toArray, fieldCollation))
    val newInput = RelOptRule.convert(segmentTop.getInput, requiredTraitSet)

    val providedTraitSet = segmentTop.getTraitSet.replace(FlinkConventions.BATCHEXEC)
    new BatchExecSegmentTop(
      rel.getCluster,
      providedTraitSet,
      newInput,
      groupKeys,
      fieldCollation,
      segmentTop.withTies,
      description)
  }

  private def createRelCollation(groupSet: Array[Int], fieldCollation: RelFieldCollation) = {
    val fields = new JArrayList[RelFieldCollation]()
    for (field <- groupSet) {
      fields.add(new RelFieldCollation(field))
    }
    fields.add(fieldCollation)
    RelCollations.of(fields)
  }
}

object BatchExecSegmentTopRule {
  val INSTANCE:RelOptRule = new BatchExecSegmentTopRule
}
