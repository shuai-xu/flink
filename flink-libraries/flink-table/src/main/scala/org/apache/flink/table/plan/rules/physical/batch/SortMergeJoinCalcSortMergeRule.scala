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

import org.apache.flink.table.plan.nodes.physical.batch._

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand, RelOptUtil}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.rex.RexUtil

import scala.collection.JavaConversions._

/**
  * If left child or right child of SortMergeJoin is Calc -> Sort, try to transpose Calc with Sort,
  * then merge Sort into Join operator to improve performance.
  */
class SortMergeJoinCalcSortMergeRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val (calc, sort) = if (leftHasSort(call)) getLeftChild(call) else getRightChild(call)
    isCalcSortTransposable(calc, sort)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: BatchExecSortMergeJoinBase = call.rel(0)
    val leftNeedRewrite = leftHasSort(call)
    val (calc, sort) = if (leftNeedRewrite) getLeftChild(call) else getRightChild(call)
    // remove collation trait of new Calc
    val traitSetOfNewCalc = calc.getTraitSet.replace(RelCollations.EMPTY)
    val newCalc = calc.copy(traitSetOfNewCalc, Seq(sort.getInput))
    val (newIsLeftSorted, newLeft, newIsRightSorted, newRight) = if (leftNeedRewrite) {
      (false, newCalc, join.rightSorted, join.getRight)
    } else {
      (join.leftSorted, join.getLeft, false, newCalc)
    }
    val equivJoin = join match {
      case semiJoin: BatchExecSortMergeSemiJoin =>
        new BatchExecSortMergeSemiJoin(
          semiJoin.getCluster,
          semiJoin.getTraitSet,
          newLeft,
          newRight,
          semiJoin.getCondition,
          semiJoin.leftKeys,
          semiJoin.rightKeys,
          semiJoin.isAnti,
          newIsLeftSorted,
          newIsRightSorted,
          semiJoin.description)
      case j: BatchExecSortMergeJoin =>
        new BatchExecSortMergeJoin(
          j.getCluster,
          j.getTraitSet,
          newLeft,
          newRight,
          j.getCondition,
          j.getJoinType,
          newIsLeftSorted,
          newIsRightSorted,
          j.description)
    }
    call.transformTo(equivJoin)
  }

  private def leftHasSort(call: RelOptRuleCall): Boolean = call.rels.length == 3

  private def getLeftChild(call: RelOptRuleCall): (BatchExecCalc, BatchExecSort) = {
    (call.rels(1).asInstanceOf[BatchExecCalc], call.rels(2).asInstanceOf[BatchExecSort])
  }

  private def getRightChild(call: RelOptRuleCall): (BatchExecCalc, BatchExecSort) = {
    (call.rels(2).asInstanceOf[BatchExecCalc], call.rels(3).asInstanceOf[BatchExecSort])
  }

  private def isCalcSortTransposable(calc: BatchExecCalc, sort: BatchExecSort): Boolean = {
    val program = calc.getProgram
    val projects = program.getProjectList.map(program.expandLocalRef)
    // mapping is a permutation describing where output fields of Calc come from. In the returned
    // map, value of map.getSourceOpt(i) is n if field i is project on input field n,
    // -1 if it is an expression.
    val mapping = RelOptUtil.permutation(projects, calc.getInput.getRowType).inverse()
    val fieldCollations = sort.getCollation.getFieldCollations
    val allSortKeysAreKept = fieldCollations.forall(c => mapping.getTargetOpt(c.getFieldIndex) >= 0)
    // Don't transpose if calc contains non-deterministic expr
    allSortKeysAreKept && RexUtil.isDeterministic(calc.getProgram.getExprList)
  }

}

object SortMergeJoinCalcSortMergeRule {

  val LEFT_SORT = new SortMergeJoinCalcSortMergeRule(
    operand(classOf[BatchExecSortMergeJoinBase],
      some(operand(classOf[BatchExecCalc],
        operand(classOf[BatchExecSort], any())))),
    "SortMergeJoinCalcSortMergeRule(left)")

  val RIGHT_SORT = new SortMergeJoinCalcSortMergeRule(
    operand(classOf[BatchExecSortMergeJoinBase],
      operand(classOf[RelNode], any),
      operand(classOf[BatchExecCalc], operand(classOf[BatchExecSort], any()))),
    "SortMergeJoinCalcSortMergeRule(right)")

}
