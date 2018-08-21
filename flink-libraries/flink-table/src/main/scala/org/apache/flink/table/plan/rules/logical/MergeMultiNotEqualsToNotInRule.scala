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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Merge multi AND to a NOT IN.
  * e.g:
  * input predicate: (a <> 10 AND a <> 20 AND a <> 21) OR b <> 5
  * output expressions: a NOT IN (10, 20, 21) OR b <> 5.
  */
class MergeMultiNotEqualsToNotInRule extends RelOptRule(
  operand(classOf[Filter], any),
  "MergeMultiNotEqualsToNotInRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)

    transferNotEqualsToNotIn(call.builder(), filter.getCondition) match {
      case Some(newRex) =>
        call.transformTo(filter.copy(
          filter.getTraitSet,
          filter.getInput,
          newRex))
      case None =>
    }
  }

  def transferNotEqualsToNotIn(builder: RelBuilder, rex: RexNode): Option[RexNode] = {
    var hasCombined = false
    val rexBuilder = builder.getRexBuilder

    val combineMap = new mutable.HashMap[String, mutable.ListBuffer[RexCall]]
    val rexBuffer = new mutable.ArrayBuffer[RexNode]

    RelOptUtil.conjunctions(rex).foreach {
      case call: RexCall =>
        call.getOperator match {
          case NOT_EQUALS =>
            val (left, right) = (call.operands.get(0), call.operands.get(1))
            (left, right) match {
              case (i, _: RexLiteral) =>
                combineMap.getOrElseUpdate(i.toString, mutable.ListBuffer[RexCall]()) += call
              case (l: RexLiteral, i) =>
                combineMap.getOrElseUpdate(i.toString, mutable.ListBuffer[RexCall]()) +=
                    call.clone(call.getType, List(i, l))
              case _ => rexBuffer += call
            }
          case OR =>
            rexBuffer += builder.or(RelOptUtil.disjunctions(call).map { r =>
              transferNotEqualsToNotIn(builder, r) match {
                case Some(newRex) =>
                  hasCombined = true
                  newRex
                case None => r
              }
            })
          case _ => rexBuffer += call
        }
      case r => rexBuffer += r
    }

    combineMap.values.foreach { nodes =>
      val head = nodes.head.getOperands.head
      if (MergeMultiEqualsToInRule.needRewrite(head, nodes.size)) {
        val valuesNode = nodes.map(_.getOperands.last)
        rexBuffer += rexBuilder.makeCall(NOT_IN, List(head) ++ valuesNode)
        hasCombined = true
      } else {
        rexBuffer += builder.and(nodes)
      }
    }

    if (hasCombined) Some(builder.and(rexBuffer)) else None
  }
}

object MergeMultiNotEqualsToNotInRule {
  val INSTANCE = new MergeMultiNotEqualsToNotInRule
}
