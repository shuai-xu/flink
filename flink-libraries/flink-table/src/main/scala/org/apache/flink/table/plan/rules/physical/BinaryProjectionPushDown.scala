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

package org.apache.flink.table.plan.rules.physical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.flink.table.plan.nodes.common.CommonExchange
import org.apache.flink.table.plan.nodes.physical.BaseExecCalc

/**
  * Let calc output BinaryRow directly, avoiding the serializer converting BaseRow to BinaryRow
  * in Exchange.
  */
class BinaryProjectionPushDown extends RelOptRule(
  operand(classOf[CommonExchange], operand(classOf[BaseExecCalc], any)),
  "BinaryProjectionPushDown") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BaseExecCalc = call.rel(1)
    !calc.outputBinaryRow
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val exchange: CommonExchange = call.rel(0)
    val calc: BaseExecCalc = call.rel(1)
    call.transformTo(exchange.copy(
      exchange.getTraitSet,
      calc.copy(exchange.getTraitSet, outputBinaryRow = true),
      exchange.distribution))
  }
}

object BinaryProjectionPushDown {
  val INSTANCE = new BinaryProjectionPushDown
}
