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
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.types.DataTypes

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Merge multi OR to a IN.
  * e.g:
  * input predicate: (a = 10 OR a = 20 OR a = 21) AND b = 5
  * output expressions: a IN (10, 20, 21) AND b = 5.
  */
class MergeMultiEqualsToInRule extends RelOptRule(
  operand(classOf[Filter], any),
  "MergeMultiEqualsToInRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)

    transferEqualsToIn(call.builder(), filter.getCondition) match {
      case Some(newRex) =>
        call.transformTo(filter.copy(
          filter.getTraitSet,
          filter.getInput,
          newRex))
      case None =>
    }
  }

  def transferEqualsToIn(builder: RelBuilder, rex: RexNode): Option[RexNode] = {
    var hasCombined = false
    val rexBuilder = builder.getRexBuilder

    val combineMap = new mutable.HashMap[String, mutable.ListBuffer[RexCall]]
    val rexBuffer = new mutable.ArrayBuffer[RexNode]

    RelOptUtil.disjunctions(rex).foreach {
      case call: RexCall =>
        call.getOperator match {
          case EQUALS =>
            val (left, right) = (call.operands.get(0), call.operands.get(1))
            (left, right) match {
              case (i, _: RexLiteral) =>
                combineMap.getOrElseUpdate(i.toString, mutable.ListBuffer[RexCall]()) += call
              case (l: RexLiteral, i) =>
                combineMap.getOrElseUpdate(i.toString, mutable.ListBuffer[RexCall]()) +=
                    call.clone(call.getType, List(i, l))
              case _ => rexBuffer += call
            }
          case AND =>
            rexBuffer += builder.and(RelOptUtil.conjunctions(call).map { r =>
              transferEqualsToIn(builder, r) match {
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
        rexBuffer += rexBuilder.makeCall(IN, List(head) ++ valuesNode)
        hasCombined = true
      } else {
        rexBuffer += builder.or(nodes)
      }
    }

    if (hasCombined) Some(builder.or(rexBuffer)) else None
  }
}

object MergeMultiEqualsToInRule {

  val INSTANCE = new MergeMultiEqualsToInRule

  private val THRESHOLD: Int = 4
  private val FRACTIONAL_THRESHOLD: Int = 20

  def needRewrite(head: RexNode, size: Int): Boolean = {
    FlinkTypeFactory.toInternalType(head.getType) match {
      case DataTypes.FLOAT => size >= FRACTIONAL_THRESHOLD
      case DataTypes.DOUBLE => size >= FRACTIONAL_THRESHOLD
      case _ => size >= THRESHOLD
    }
  }
}
