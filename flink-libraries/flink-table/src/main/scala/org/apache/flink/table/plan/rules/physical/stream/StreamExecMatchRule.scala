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

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexInputRef
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecMatch
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalMatch
import org.apache.flink.table.plan.schema.BaseRowSchema

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StreamExecMatchRule
  extends ConverterRule(
    classOf[FlinkLogicalMatch],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecMatchRule") {

  override def convert(rel: RelNode): RelNode = {
    val logicalMatch: FlinkLogicalMatch = rel.asInstanceOf[FlinkLogicalMatch]

    val logicalKeys = logicalMatch.getPartitionKeys.asScala.map {
      case inputRef: RexInputRef => inputRef.getIndex
    }

    val requiredDistribution = if (logicalKeys.nonEmpty) {
      FlinkRelDistribution.hash(logicalKeys.map(Integer.valueOf))
    } else {
      FlinkRelDistribution.ANY
    }
    val requiredTraitSet = logicalMatch.getInput.getTraitSet.replace(
      FlinkConventions.STREAMEXEC).replace(requiredDistribution)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAMEXEC)

    val convertInput: RelNode =
      RelOptRule.convert(logicalMatch.getInput, requiredTraitSet)


    new StreamExecMatch(
      rel.getCluster,
      providedTraitSet,
      convertInput,
      logicalMatch.getPattern,
      logicalMatch.isStrictStart,
      logicalMatch.isStrictEnd,
      logicalMatch.getPatternDefinitions,
      logicalMatch.getMeasures,
      logicalMatch.getAfter,
      logicalMatch.getSubsets,
      logicalMatch.getRowsPerMatch,
      logicalMatch.getPartitionKeys,
      logicalMatch.getOrderKeys,
      logicalMatch.getInterval,
      logicalMatch.getEmit,
      new BaseRowSchema(logicalMatch.getRowType),
      new BaseRowSchema(logicalMatch.getInput.getRowType))
  }
}

object StreamExecMatchRule {
  val INSTANCE: RelOptRule = new StreamExecMatchRule
}
