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

package com.alibaba.blink.table.plan.rules.datastream

import java.util

import com.alibaba.blink.table.plan.rules.utils.HTableRelUtil._
import com.alibaba.blink.table.sources.HBaseDimensionTableSource
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex._
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin}
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecMultiJoinHTables
import org.apache.flink.table.plan.schema.RowSchema

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Planner rule to flatten a tree of
  * {@link org.apache.calcite.rel.logical.LogicalJoin}s that join with {@link HBaseTableSource}s
  * into a single {@link StreamMultiJoinHTables} with original left input and N {@link
  * HBaseTableSource}s.
  *
  * <p>An input is not flattened if the input is a null generating input in an outer join, i.e.,
  * the right hand side of a left outer join.
  *
  * <p>Join conditions are also pulled up from the inputs into the topmost
  * {@link StreamMultiJoinHTables}, unless the input corresponds to a null generating input in an
  * outer join.
  *
  * <p>Outer join information is also stored in the {@link StreamMultiJoinHTables}.
  * In the case of left outer joins, the join type and outer join conditions are
  * stored in arrays in the {@link StreamMultiJoinHTables}. This outer join information is
  * associated with the null generating input in the outer join. So, in the case
  * of a a left outer join between A and B, the information is associated with B, not A.
  *
  * <p>Here are examples of the {@link StreamMultiJoinHTables}s constructed after this rule
  * has been applied on following join trees.
  * A: DataStream, B & C: HBaseTableSource
  * <ul>
  * <li>A JOIN B JOIN C &rarr; MJ(A, (B, C))
  *
  * <li>(A JOIN B) LEFT JOIN C &rarr; MJ(A, (B, C)), left outer join on input#1
  *
  * <li>(A LEFT JOIN B) JOIN C &rarr; MJ(A, (B, C)), left outer join on input#0
  *
  * <li>(A LEFT JOIN B) LEFT JOIN C &rarr; MJ(A, (B, C)), left outer join on input#0 and input#1
  * </ul>
  *
  * @see org.apache.calcite.rel.rules.FilterMultiJoinMergeRule
  * @see org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule
  */
class StreamExecJoinHTableToMultiJoinRule extends RelOptRule(
  operand(
    classOf[FlinkLogicalJoin],
    operand(classOf[RelNode], any),
    operand(classOf[RelNode], any)),
  "StreamExecJoinHTableToMultiJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val joinInfo = join.analyzeCondition

    /**
      * find at least @limitCnt number of cascaded join/leftJoin HBaseTableSource(s)
      *
      * @param rel
      * @param limitCnt
      * @return
      */
    def findAimNodeAtLeastCnt(rel: RelNode, limitCnt: Int): Boolean = {
      rel match {
        case mj: StreamExecMultiJoinHTables => {
          limitCnt <= 1
          // do not match pattern like 'MJ(A, (B,C)) JOIN D' because we convert the whole
          // sub-tree once
        }

        case rel: RelSubset =>
          findAimNodeAtLeastCnt(rel.getRelList.get(0), limitCnt)

        // only match Join(Project(Join)) pattern here, the inside Project between two
        // Joins is added by optimization rule, differs from user operation.
        case rel: FlinkLogicalCalc => {
          val child = rel.getInput(0)
          if (hasSelectionOnly(rel)) {
            // continue search
            findAimNodeAtLeastCnt(child, limitCnt)
          } else {
            false
          }
        }

        // has a HBaseTableSource on right child of FlinkLogicalJoin
        case rel: FlinkLogicalJoin => {
          val newLimit = if (hasHBaseTableSource(rel.getRight)) {
            limitCnt - 1
          } else {
            limitCnt
          }
          if (newLimit <= 0) {
            return true
          }
          // continue searching
          findAimNodeAtLeastCnt(rel.getLeft, newLimit)
        }

        case _ =>
          false
      }
    }

    // this join require exactly one equi-join condition and
    // currently not support any non-equi-join conditions
    (join.getJoinType == JoinRelType.INNER || join.getJoinType == JoinRelType.LEFT) &&
      !joinInfo.pairs().isEmpty && joinInfo.pairs().size() == 1 && joinInfo.isEqui &&
      findAimNodeAtLeastCnt(join, 2)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {

    class CascadedJoinVistor {
      val stack: mutable.ArrayStack[Node] = new mutable.ArrayStack[Node]()

      def visitor(rel: RelNode): mutable.ArrayStack[Node] = {
        rel match {
          case r: RelSubset => {
            visitor(r.getRelList.get(0))
          }

          case l: FlinkLogicalJoin => {
            if (hasHBaseTableSource(l.getRight)) {
              //hTableSource should never be null here
              val joinInfo = l.analyzeCondition()
              val leftKeyIndex = joinInfo.leftKeys.get(0)
              stack.push(
                new Node(
                  new RowSchema(l.getLeft.getRowType).fieldNames(leftKeyIndex),
                  joinInfo.getEquiCondition(l.getLeft, l.getRight, l.getCluster.getRexBuilder),
                  l.getJoinType,
                  extractHTableSource(l.getRight),
                  false,
                  null,
                  null))
            }
            if (l.getLeft.isInstanceOf[RelSubset] || l.getLeft.isInstanceOf[FlinkLogicalJoin]) {
              visitor(l.getLeft)
            }
          }

          // only match Join(Project(Join)) pattern here, the inside Project between two
          // Joins is added by optimization rule, differs from user operation.
          case c: FlinkLogicalCalc => {
            val child = c.getInput(0)
            if (hasSelectionOnly(c)) {
              visitor(child)
            } else {
              val convInput = RelOptRule.convert(c, FlinkConventions.STREAMEXEC)
              stack.push(new Node(null, null, null, null, false, null, convInput))
            }
          }

          case mj: StreamExecMultiJoinHTables => {
            stack.push(
              new Node(
                null,
                null,
                null,
                null,
                true,
                mj,
                null))
          }

          case r: RelNode => {
            val convInput = RelOptRule.convert(r, FlinkConventions.STREAMEXEC)
            stack.push(new Node(null, null, null, null, false, null, convInput))
          }
        }
        stack
      }
    }

    val origJoin: Join = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val traitSet: RelTraitSet = origJoin.getTraitSet.replace(FlinkConventions.STREAMEXEC)
    val preparedStack: mutable.ArrayStack[Node] = new CascadedJoinVistor().visitor(origJoin)
    assert(preparedStack.size > 0)

    val rootInputRel = preparedStack.pop().relNode
    // for the new StreamMultiJoinHTables
    var tmpJoin: StreamExecMultiJoinHTables = new StreamExecMultiJoinHTables(
      origJoin.getCluster,
      traitSet,
      origJoin.getRowType,
      null,
      //assign top element to mj's left
      rootInputRel,
      null,
      new util.LinkedList(),
      new util.LinkedList(),
      new util.LinkedList(),
      new util.LinkedList(),
      new util.LinkedList(),
      null,
      null,
      description
    )
    while (!preparedStack.isEmpty) {
      val node = preparedStack.pop()
      if (!node.isMultiJoin) {
        tmpJoin.hTableSources.add(node.hBaseTableSource)
        tmpJoin.joinConditions.add(node.joinCondition)
        tmpJoin.leftKeyList.add(node.leftJoinKey)
        tmpJoin.joinTypes.add(node.joinRelType)
      } else if (node.isMultiJoin) {
        val newMultiJoin = node.multiJoin
        newMultiJoin.hTableSources.addAll(tmpJoin.hTableSources)
        newMultiJoin.leftNode = tmpJoin.leftNode
        newMultiJoin.rowRelDataType = tmpJoin.rowRelDataType
        newMultiJoin.leftKeyList.addAll(tmpJoin.leftKeyList)
        newMultiJoin.joinConditions.addAll(tmpJoin.joinConditions)
        newMultiJoin.joinTypes.addAll(tmpJoin.joinTypes)
        tmpJoin = newMultiJoin
      } //no else
    }

    val outputFieldsSet = new util.HashSet[String]()
    origJoin.getRowType.getFieldList.asScala.map(f => outputFieldsSet.add(f.getName))

    for (i <- 0 until tmpJoin.hTableSources.size()) {
      val source = tmpJoin.hTableSources.get(i)
      // check key is selected or not
      if (!outputFieldsSet.contains(source.getKeyName)) {
        source.setSelectKey(false)
      }
    }

    // calc required left fields index due to project push down optimization
    val requiredLeftFieldIdx = new RowSchema(rootInputRel.getRowType).relDataType.getFieldList
      .asScala.zipWithIndex
      .filter(f => outputFieldsSet.contains(f._1.getName)).map(_._2).toArray
    tmpJoin.requiredLeftFieldIdx = requiredLeftFieldIdx

    call.transformTo(tmpJoin)
  }

}

class Node(
    val leftJoinKey: String,
    val joinCondition: RexNode,
    val joinRelType: JoinRelType,
    val hBaseTableSource: HBaseDimensionTableSource[_],
    val isMultiJoin: Boolean,
    val multiJoin: StreamExecMultiJoinHTables,
    val relNode: RelNode) {
}

object StreamExecJoinHTableToMultiJoinRule {
  val INSTANCE = new StreamExecJoinHTableToMultiJoinRule
}
