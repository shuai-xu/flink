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
package org.apache.flink.table.util

import java.io.{PrintWriter, StringWriter}
import java.lang.{Boolean => JBool, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort, String => JString}
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}
import java.util.Calendar

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rex.{RexInputRef, RexVisitorImpl}
import org.apache.calcite.rex.{RexCall, RexUtil, _}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.functions.sql.internal.SqlAuxiliaryGroupAggFunction

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object FlinkRelOptUtil {

  def toString(
      rel: RelNode,
      treeStyle: Boolean = true,
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource: Boolean = false,
      printMemCost: Boolean = false,
      withRelNodeId: Boolean = false): String = {
    if (!treeStyle) {
      RelOptUtil.toString(rel, detailLevel)
    } else {
      val sw = new StringWriter
      val planWriter = new RelTreeWriterImpl(
        new PrintWriter(sw),
        detailLevel,
        printResource,
        printMemCost,
        withRelNodeId)
      rel.explain(planWriter)
      sw.toString
    }
  }

  def getTableConfig(rel: RelNode): TableConfig = {
    Option(rel.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig]))
      .getOrElse(TableConfig.DEFAULT)
  }

  /**
    * Check whether AUXILIARY_GROUP aggCalls is in the front of the given agg's aggCallList,
    * and whether aggCallList contain AUXILIARY_GROUP when the given agg's groupSet is empty
    * or the indicator is true.
    * Returns AUXILIARY_GROUP aggCalls' args and other aggCalls.
    *
    * @param agg aggregate
    * @return returns AUXILIARY_GROUP aggCalls' args and other aggCalls
    */
  def checkAndSplitAggCalls(agg: Aggregate): (Array[Int], Seq[AggregateCall]) = {
    var nonAuxGroupCallsStartIdx = -1

    val aggCalls = agg.getAggCallList
    aggCalls.zipWithIndex.foreach {
      case (call, idx) =>
        if (call.getAggregation == SqlAuxiliaryGroupAggFunction) {
          require(call.getArgList.size == 1)
        }
        if (nonAuxGroupCallsStartIdx >= 0) {
          // the left aggCalls should not be AUXILIARY_GROUP
          require(call.getAggregation != SqlAuxiliaryGroupAggFunction,
                  "AUXILIARY_GROUP should be in the front of aggCall list")
        }
        if (nonAuxGroupCallsStartIdx < 0 &&
          call.getAggregation != SqlAuxiliaryGroupAggFunction) {
          nonAuxGroupCallsStartIdx = idx
        }
    }

    if (nonAuxGroupCallsStartIdx < 0) {
      nonAuxGroupCallsStartIdx = aggCalls.length
    }

    val (auxGroupCalls, otherAggCalls) = aggCalls.splitAt(nonAuxGroupCallsStartIdx)
    if (agg.getGroupCount == 0) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when groupSet is empty")
    }
    if (agg.indicator) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when indicator is true")
    }

    val auxGrouping = auxGroupCalls.map(_.getArgList.head.toInt).toArray
    require(auxGrouping.length + otherAggCalls.length == aggCalls.length)
    (auxGrouping, otherAggCalls)
  }

  def checkAndGetFullGroupSet(agg: Aggregate): Array[Int] = {
    val (auxGroupSet, _) = checkAndSplitAggCalls(agg)
    agg.getGroupSet.toArray ++ auxGroupSet
  }

  /** Get max cnf node limit by context of rel */
  def getMaxCnfNodeCount(rel: RelNode): Int = {
    getTableConfig(rel).getMaxCnfNodeCount
  }

  /**
    * Gets values of RexLiteral
    *
    * @param literal input RexLiteral
    * @return values of the input RexLiteral
    */
  def getLiteralValue(literal: RexLiteral): Comparable[_] = {
    if (literal.isNull) {
      null
    } else {
      val literalType = literal.getType
      literalType.getSqlTypeName match {
        case BOOLEAN => RexLiteral.booleanValue(literal)
        case TINYINT => literal.getValueAs(classOf[JByte])
        case SMALLINT => literal.getValueAs(classOf[JShort])
        case INTEGER => literal.getValueAs(classOf[Integer])
        case BIGINT => literal.getValueAs(classOf[JLong])
        case FLOAT => literal.getValueAs(classOf[JFloat])
        case DOUBLE => literal.getValueAs(classOf[JDouble])
        case DECIMAL => literal.getValue3.asInstanceOf[BigDecimal]
        case VARCHAR | CHAR => literal.getValueAs(classOf[JString])

        // temporal types
        case DATE =>
          new Date(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case TIME =>
          new Time(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case TIMESTAMP =>
          new Timestamp(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case _ =>
          throw new IllegalArgumentException(s"Literal type $literalType is not supported!")
      }
    }
  }

  /**
    * Tries to decompose the RexNode into two parts based on splitterVisitor.
    * The first part is interested part of the input expression,
    * the second part is rest part of the input expression.
    *
    * For simple condition which is not AND, OR, NOT, it is completely interested or not based on
    * whether it matches the input splitterVisitor or not.
    *
    * For complex condition is Ands, decompose each operands of ANDS recursively, then
    * merge the interested parts of each operand as the interested part of the input condition,
    * merge the rest parts of each operands as the rest parts of the input condition.
    *
    * For complex condition ORs, try to pull up common factors among ORs first, if the common
    * factors is not A ORs, then simplify the question to decompose the common factors expression;
    * else the input condition is completely interested or not based on whether all its operands
    * matches the splitterVisitor or not.
    *
    * For complex condition NOT, it is completely interested or not based on whether its operand
    * matches the splitterVisitor or not.
    *
    * @param expr            the expression to decompose
    * @param rexBuilder      rexBuilder
    * @param splitterVisitor the RexVisitor to check if a RexNode is interested or not
    * @return the decompose result of the RexNode based on splitterVisitor,
    *         which is (interested parts, rest parts)
    */
  def decompose(
      expr: RexNode,
      rexBuilder: RexBuilder,
      splitterVisitor: RexVisitor[JBool]): (Option[RexNode], Option[RexNode]) = {
    val condition = pushNotToLeaf(expr, rexBuilder)
    val (interested: Option[RexNode], rest: Option[RexNode]) = condition.getKind match {
      case AND =>
        val (interestedLists, restLists) = decompose(
          condition.asInstanceOf[RexCall].operands, rexBuilder, splitterVisitor)
        if (interestedLists.isEmpty) {
          (None, Option(condition))
        } else {
          val interestedPart = RexUtil.composeConjunction(rexBuilder, interestedLists.asJava, false)
          if (restLists.isEmpty) {
            (Option(interestedPart), None)
          } else {
            val restPart = RexUtil.composeConjunction(rexBuilder, restLists.asJava, false)
            (Option(interestedPart), Option(restPart))
          }
        }
      case OR =>
        val factor = RexUtil.pullFactors(rexBuilder, condition)
        factor.getKind match {
          case OR =>
            val (interestedLists, restLists) = decompose(
              condition.asInstanceOf[RexCall].operands, rexBuilder, splitterVisitor)
            if (interestedLists.isEmpty || restLists.nonEmpty) {
              (None, Option(condition))
            } else {
              (Option(RexUtil.composeDisjunction(rexBuilder, interestedLists.asJava, false)), None)
            }
          case _ =>
            decompose(factor, rexBuilder, splitterVisitor)
        }
      case NOT =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        decompose(operand, rexBuilder, splitterVisitor) match {
          case (Some(_), None) => (Option(condition), None)
          case (_, _) => (None, Option(condition))
        }
      case _ =>
        if (condition.accept(splitterVisitor)) {
          (Option(condition), None)
        } else {
          (None, Option(condition))
        }
    }
    (convertRexNodeIfAlwaysTrue(interested), convertRexNodeIfAlwaysTrue(rest))
  }

  private def decompose(
      exprs: Iterable[RexNode],
      rexBuilder: RexBuilder,
      splitterVisitor: RexVisitor[JBool]): (Iterable[RexNode], Iterable[RexNode]) = {
    val interestedLists = mutable.ListBuffer[RexNode]()
    val restLists = mutable.ListBuffer[RexNode]()
    exprs.foreach(expr => {
      decompose(expr, rexBuilder, splitterVisitor) match {
        case (Some(interested), Some(rest)) =>
          interestedLists += interested
          restLists += rest
        case (None, Some(rest)) =>
          restLists += rest
        case (Some(interested), None) =>
          interestedLists += interested
      }
    })
    (interestedLists, restLists)
  }

  private def convertRexNodeIfAlwaysTrue(expr: Option[RexNode]): Option[RexNode] = {
    expr match {
      case Some(rex) if rex.isAlwaysTrue => None
      case _ => expr
    }
  }

  private def pushNotToLeaf(expr: RexNode,
      rexBuilder: RexBuilder,
      needReverse: Boolean = false): RexNode =
    (expr.getKind, needReverse) match {
      case (AND, true) | (OR, false) =>
        val convertedExprs = expr.asInstanceOf[RexCall].operands
            .map(pushNotToLeaf(_, rexBuilder, needReverse))
        RexUtil.composeDisjunction(rexBuilder, convertedExprs, false)
      case (AND, false) | (OR, true) =>
        val convertedExprs = expr.asInstanceOf[RexCall].operands
            .map(pushNotToLeaf(_, rexBuilder, needReverse))
        RexUtil.composeConjunction(rexBuilder, convertedExprs, false)
      case (NOT, _) =>
        val child = expr.asInstanceOf[RexCall].operands.head
        pushNotToLeaf(child, rexBuilder, !needReverse)
      case (_, true) if expr.isInstanceOf[RexCall] =>
        val negateExpr = RexUtil.negate(rexBuilder, expr.asInstanceOf[RexCall])
        if (negateExpr != null) negateExpr else RexUtil.not(expr)
      case (_, true) => RexUtil.not(expr)
      case (_, false) => expr
    }

  /**
    * An RexVisitor to judge whether the RexNode is related to the specified index InputRef
    */
  class InputRefVisitor(index: Int) extends RexVisitorImpl[JBool](true) {

    override def visitInputRef(inputRef: RexInputRef): JBool = inputRef.getIndex == index

    override def visitLiteral(literal: RexLiteral): JBool = true

    override def visitCall(call: RexCall): JBool = {
      call.operands.forall(operand => {
        val isRelated = operand.accept(this)
        isRelated != null && isRelated
      })
    }
  }
}
