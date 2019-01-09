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
package org.apache.flink.table.plan.util

import org.apache.flink.table.sources.{IndexKey, TableSource}
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes}
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}

import java.util
import java.util.Comparator

import scala.collection.JavaConversions._

/**
  * Utilities for temporal table join
  */
object TemporalJoinUtil {

  def isDeterministic(
    calcProgram: Option[RexProgram],
    period: RexNode,
    joinCondition: RexNode = null): Boolean = {
    calcProgram match {
      case Some(program) =>
        if (program.getCondition != null) {
          val condition = program.expandLocalRef(program.getCondition)
          if (!FlinkRexUtil.isDeterministicOperator(condition)) {
            return false
          }
        }
        val projection = program.getProjectList.map(program.expandLocalRef)
        if (!projection.forall(p => FlinkRexUtil.isDeterministicOperator(p))) {
          return false
        }
      case _ => // do nothing
    }
    if (!FlinkRexUtil.isDeterministicOperator(period)) {
      return false
    }
    if (joinCondition != null) {
      FlinkRexUtil.isDeterministicOperator(joinCondition)
    } else {
      true
    }
  }

  def getTableIndexKeys(table: TableSource): util.List[IndexKey] = {
    val schema = table.getTableSchema
    val indexes = new util.ArrayList[IndexKey]()
    if (schema.getUniqueIndexes.nonEmpty) {
      schema.getUniqueIndexes
      .map(keys => keys.map(schema.getColumnIndex))
      .foreach(keys => indexes.add(IndexKey.of(true, keys: _*)))
    }
    if (schema.getNormalIndexes.nonEmpty) {
      schema.getNormalIndexes
      .map(index => index.map(schema.getColumnIndex))
      .foreach(index => indexes.add(IndexKey.of(true, index: _*)))
    }
    // sort: unique first
    util.Collections.sort[IndexKey](indexes, new Comparator[IndexKey]() {
      override def compare(o1: IndexKey, o2: IndexKey): Int =
        if (null != o1) o1.compareTo(o2) else 1
    })
    indexes
  }

  // ----------------------------------------------------------------------------------------
  //                          Temporal TableFunction Join Utilities
  // ----------------------------------------------------------------------------------------

  /**
    * [[TEMPORAL_JOIN_CONDITION]] is a specific condition which correctly defines
    * references to rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute.
    * The condition is used to mark this is a temporal tablefunction join.
    * Later rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute will be
    * extracted from the condition.
    */
  val TEMPORAL_JOIN_CONDITION = new SqlFunction(
    "__TEMPORAL_JOIN_CONDITION",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.or(
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.DATETIME,
        OperandTypes.ANY),
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.ANY)),
    SqlFunctionCategory.SYSTEM)


  def isRowtimeCall(call: RexCall): Boolean = {
    checkArgument(call.getOperator == TEMPORAL_JOIN_CONDITION)
    call.getOperands.size() == 3
  }

  def isProctimeCall(call: RexCall): Boolean = {
    checkArgument(call.getOperator == TEMPORAL_JOIN_CONDITION)
    call.getOperands.size() == 2
  }

  def makeRowTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightTimeAttribute: RexNode,
    rightPrimaryKeyExpression: RexNode): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightTimeAttribute,
      rightPrimaryKeyExpression)
  }

  def makeProcTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightPrimaryKeyExpression: RexNode): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightPrimaryKeyExpression)
  }


  def containsTemporalJoinCondition(condition: RexNode): Boolean = {
    val visitor = new TemporalJoinConditionVisitor
    condition.accept(visitor)
    visitor.hasTemporalJoinCondition
  }

  private class TemporalJoinConditionVisitor extends RexVisitorImpl[Void](true) {

    var hasTemporalJoinCondition: Boolean = false

    override def visitCall(call: RexCall): Void = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        super.visitCall(call)
      } else {
        hasTemporalJoinCondition = true
        null
      }
    }
  }

}
