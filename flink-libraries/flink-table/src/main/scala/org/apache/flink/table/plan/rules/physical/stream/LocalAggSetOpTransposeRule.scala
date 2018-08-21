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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.core.SetOp
import org.apache.flink.table.plan.`trait`.{AccModeTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.nodes.physical.stream._

import scala.collection.JavaConversions._

/**
  * Planner rule that pushes a [[StreamExecLocalGroupAggregate]] past a [[SetOp]].
  */
class LocalAggSetOpTransposeRule
  extends RelOptRule(
    operand(classOf[StreamExecLocalGroupAggregate],
      // currently only support union
            operand(classOf[StreamExecUnion], any)),
    "LocalAggSetOpTransposeRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val localAgg: StreamExecLocalGroupAggregate = call.rel(0)
    val setOp: StreamExecUnion = call.rel(1)

    val newSetOpInputs = setOp.getInputs.map { input =>
      localAgg.copy(localAgg.getTraitSet, util.Arrays.asList(input))
    }

    val accMode = localAgg.getTraitSet.getTrait(AccModeTraitDef.INSTANCE)
    val updateAsRetraction = localAgg.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
    // setop should extends local agg trait set
    val newSetOpTraitSet = setOp.getTraitSet.replace(accMode).replace(updateAsRetraction)

    val newSetOp = new StreamExecUnion(
      setOp.getCluster,
      newSetOpTraitSet,
      newSetOpInputs.get(0),
      newSetOpInputs.get(1),
      localAgg.getRowType)

    call.transformTo(newSetOp)
  }
}

object LocalAggSetOpTransposeRule {
  val INSTANCE = new LocalAggSetOpTransposeRule
}
