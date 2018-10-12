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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.flink.table.functions.utils.CoTableValuedAggSqlFunction
import org.apache.flink.table.types.DataType

import scala.collection.JavaConversions._

/**
  * Common co-table-valued aggregate relNode.
  */
abstract class CoTableValuedAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    val leftNode: RelNode,
    val rightNode: RelNode,
    val lRexCall: RexCall,
    val rRexCall: RexCall,
    resultType: DataType,
    val groupKey1: Seq[RexNode],
    val groupKey2: Seq[RexNode])
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonTableValuedAgg {

  override def deriveRowType: RelDataType = {
    val groupKeyNames = getGroupKeyNames(leftNode.getRowType, groupKey1, 0)
    getRowType(cluster, resultType, groupKey1, groupKeyNames)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy1",
        groupingToString(left.getRowType, groupKey1, 0), groupKey1.nonEmpty)
      .itemIf("groupBy2", groupingToString(
        right.getRowType, groupKey2, left.getRowType.getFieldCount), groupKey2.nonEmpty)
      .item("coAggApply",
        buildAggregationToString(
          left.getRowType,
          right.getRowType,
          lRexCall: RexCall,
          rRexCall: RexCall))
  }

  def getGroupKeyNames(
    inputType: RelDataType, groupkeys: Seq[RexNode], offset: Int): Seq[String] = {
    val inFields = inputType.getFieldNames
    groupkeys.map(e => inFields(e.asInstanceOf[RexInputRef].getIndex - offset))
  }

  private[flink] def groupingToString(
    inputType: RelDataType, groupkeys: Seq[RexNode], offset: Int): String = {
    getGroupKeyNames(inputType, groupkeys, offset).mkString(",")
  }

  private[flink] def buildAggregationToString(
    leftType: RelDataType,
    rightType: RelDataType,
    lRexCall: RexCall,
    rRexCall: RexCall): String = {
    val in1Fields = leftType.getFieldNames
    val in2Fields = rightType.getFieldNames
    val in1ArgsIndex = lRexCall.getOperands.toArray.zipWithIndex.map(_._2)
    val in2ArgsIndex = rRexCall.getOperands.toArray.zipWithIndex.map(_._2)
    s"${lRexCall.op.asInstanceOf[CoTableValuedAggSqlFunction].toString}" +
      s"(${in1ArgsIndex.map(in1Fields(_)).mkString(", ")})" +
      s"(${in2ArgsIndex.map(in2Fields(_)).mkString(", ")})"
  }
}
