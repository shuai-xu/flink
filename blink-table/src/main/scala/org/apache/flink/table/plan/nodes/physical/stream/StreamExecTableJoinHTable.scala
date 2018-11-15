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

package org.apache.flink.table.plan.nodes.physical.stream

import java.util.concurrent.TimeUnit
import java.util.{List => JList}

import com.alibaba.blink.table.sources.HBaseDimensionTableSource
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConverters._

class StreamTableJoinHTable(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    val leftNode: RelNode,
    hTableSource: HBaseDimensionTableSource[_],
    outputSchema: BaseRowSchema,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: JList[IntPair],
    val joinType: JoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, leftNode)
  with StreamExecRel {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamTableJoinHTable(
      cluster,
      traitSet,
      inputs.get(0),
      hTableSource,
      outputSchema,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    .item("where", joinConditionToString)
    .item("join", joinSelectionToString)
    .item("joinType", joinTypeToString)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d //fetch one row from a HBase table each request, low cost.
    planner.getCostFactory.makeCost(elementRate, elementRate, elementRate)
  }

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @return StreamTransformation of type [[BaseRow]]
    */
  override def translateToPlan(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    // we should use physical row type info here, exclude time indicator added by system
    val returnType = outputSchema.typeInfo(classOf[BaseRow])

    if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
      throw TableException("Only support inner or left join with a HBaseTable.")
    }

    if (keyPairs.size != 1) {
      // invalid join condition,  support single only, e.g, 'id === tid'
      throw TableException(
        "Join a HBaseTable should have exactly one equality condition.\n" +
          s"\tLeft: ${leftNode.toString},\n" +
          s"\tRight: ${hTableSource.toString},\n" +
          s"\tCondition: ($joinConditionToString)"
      )
    } else {
      val pair = keyPairs.get(0)
      // HBaseTable's rowKey index always be zero
      val leftKeyIdx = pair.source

      val inputTransformation = getInput.asInstanceOf[StreamExecRel]
        .translateToPlan(tableEnv)
      val leftKeyType = inputTransformation.getOutputType
          .asInstanceOf[BaseRowTypeInfo[_]].getTypeAt(leftKeyIdx)

      val asyncFunction = hTableSource.getAsyncFetchFunction[BaseRow, BaseRow](
        returnType.asInstanceOf[TypeInformation[BaseRow]],
        joinType,
        leftKeyIdx,
        leftKeyType,
        hTableSource.isStrongConsistency,
        tableEnv.execEnv.getConfig.isObjectReuseEnabled)

      val ordered = hTableSource.isOrderedMode
      val timeout = TimeUnit.MILLISECONDS.toMillis(hTableSource.getAsyncTimeoutMs)

      val (mode, name) = if (ordered) {
        val operatorName = s"ordered-async-join: ${hTableSource.explainSource()}"
        (OutputMode.ORDERED, operatorName)
      } else {
        // unordered
        val operatorName = s"unordered-async-join: ${hTableSource.explainSource()}"
        (OutputMode.UNORDERED, operatorName)
      }
      val operator = new AsyncWaitOperator(
        asyncFunction, timeout, hTableSource.getAsyncBufferCapacity, mode)

      new OneInputTransformation(
        inputTransformation,
        name,
        operator,
        returnType.asInstanceOf[TypeInformation[BaseRow]],
        tableEnv.execEnv.getParallelism)
    }
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = joinRowType.getFieldNames
    getExpressionStringInternal(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case JoinRelType.INNER => "InnerJoin"
    case JoinRelType.LEFT => "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }

  private def getExpressionStringInternal(
      expr: RexNode,
      inFields: JList[String],
      localExprsTable: Option[JList[RexNode]]): String = {

    expr match {
      case i: RexInputRef =>
        inFields.get(i.getIndex)

      case l: RexLiteral =>
        l.toString

      case _: RexLocalRef if localExprsTable.isEmpty =>
        throw new IllegalArgumentException("Encountered RexLocalRef without " +
                                             "local expression table")

      case l: RexLocalRef =>
        val lExpr = localExprsTable.get.get(l.getIndex)
        getExpressionStringInternal(lExpr, inFields, localExprsTable)

      case c: RexCall =>
        val op = c.getOperator.toString
        val ops =
          c.getOperands.asScala.map(getExpressionStringInternal(_, inFields, localExprsTable))
        c.getOperator match {
          case _ : SqlAsOperator => ops.head
          case _ => s"$op(${ops.mkString(", ")})"
        }

      case fa: RexFieldAccess =>
        val referenceExpr =
          getExpressionStringInternal(fa.getReferenceExpr, inFields, localExprsTable)
        val field = fa.getField.getName
        s"$referenceExpr.$field"

      case cv: RexCorrelVariable =>
        cv.toString

      case _ =>
        throw new IllegalArgumentException(s"Unknown expression type '${expr.getClass}': $expr")
    }
  }
}
