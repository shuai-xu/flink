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

import java.util

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableValuedAggregate
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecGroupTableValuedAggregate

/**
  * Rule for table valued agg function call's flink logical node to stream physical node.
  */
class StreamExecGroupTableValuedAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalTableValuedAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
  "StreamExecGroupTableValuedAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val call = rel.asInstanceOf[FlinkLogicalTableValuedAggregate]
    val requiredDistribution = if (call.groupKey.nonEmpty) {
      val offset = call.call.getOperands.size()
      val groupList = new util.ArrayList[Integer]()
      call.groupKey.zipWithIndex.map(t => offset + t._2).foreach(groupList.add(_))
      FlinkRelDistribution.hash(groupList)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = call.getInput.getTraitSet.replace(
      FlinkConventions.STREAMEXEC).replace(requiredDistribution)
    val newTrait = rel.getTraitSet.replace(FlinkConventions.STREAMEXEC)
    val newInput = RelOptRule.convert(call.getInput, requiredTraitSet)
    new StreamExecGroupTableValuedAggregate(
      rel.getCluster,
      newTrait,
      newInput,
      call.call,
      call.groupKey,
      call.groupKeyName)
  }
}

object StreamExecGroupTableValuedAggregateRule {
  val INSTANCE = new StreamExecGroupTableValuedAggregateRule
}

