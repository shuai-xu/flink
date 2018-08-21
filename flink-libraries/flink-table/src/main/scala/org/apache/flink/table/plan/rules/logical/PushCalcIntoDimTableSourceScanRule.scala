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
package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.rex.{RexOver, RexProgram, RexProgramBuilder}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalDimensionTableSourceScan}
import org.apache.flink.table.plan.schema.FlinkRelOptTable

class PushCalcIntoDimTableSourceScanRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc],
    operand(classOf[FlinkLogicalDimensionTableSourceScan], none)),
  "PushCalcIntoDimTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc = call.rel[FlinkLogicalCalc](0)
    // Don't push a calc which contains windowed aggregates into a
    // dimension table source for now.
    !RexOver.containsOver(calc.getProgram)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val topCalc = call.rel[FlinkLogicalCalc](0)
    val topProgram = topCalc.getProgram
    val bottomScan = call.rel[FlinkLogicalDimensionTableSourceScan](1)
    val bottomProgram = bottomScan.calcProgram
    val traitSet = topCalc.getTraitSet.replace(FlinkConventions.LOGICAL)

    // merge calc if necessary
    val mergedCalcProgram = bottomProgram match {
      case Some(_) =>
        val newProgram = RexProgramBuilder
          .mergePrograms(topProgram, bottomProgram.get, topCalc.getCluster.getRexBuilder)
        assert(newProgram.getOutputRowType == topProgram.getOutputRowType)
        newProgram
      case None =>
        topProgram
    }

    // remove trivial program
    val newCalcProgram: Option[RexProgram] = if (mergedCalcProgram.isTrivial) None else {
      Some(mergedCalcProgram)
    }

    val newTableScan = new FlinkLogicalDimensionTableSourceScan(
      topCalc.getCluster,
      traitSet,
      bottomScan.getTable.asInstanceOf[FlinkRelOptTable],
      bottomScan.period,
      newCalcProgram
    )
    call.transformTo(newTableScan)
  }
}

object PushCalcIntoDimTableSourceScanRule {
  val INSTANCE = new PushCalcIntoDimTableSourceScanRule
}
