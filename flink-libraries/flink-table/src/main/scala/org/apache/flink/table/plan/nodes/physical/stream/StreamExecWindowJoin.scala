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

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.operators.StreamOperator
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.common.CommonJoin
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.{StreamExecUtil, UpdatingPlanChecker}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, JoinedRow}
import org.apache.flink.table.runtime.join.{ProcTimeWindowInnerJoin, WindowJoinUtil}
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.typeutils.BaseRowTypeInfo

/**
  * RelNode for a time windowed stream join.
  */
class StreamExecWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    val leftNode: RelNode,
    val rightNode: RelNode,
    val joinCondition: RexNode,
    val joinType: JoinRelType,
    leftRowSchema: BaseRowSchema,
    rightRowSchema: BaseRowSchema,
    outputRowSchema: BaseRowSchema,
    val isRowTime: Boolean,
    leftLowerBound: Long,
    leftUpperBound: Long,
    remainCondition: Option[RexNode],
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with StreamExecRel {

  override def deriveRowType(): RelDataType = outputRowSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecWindowJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      leftRowSchema,
      rightRowSchema,
      outputRowSchema,
      isRowTime,
      leftLowerBound,
      leftUpperBound,
      remainCondition,
      ruleDescription)
  }

  override def toString: String = {
    joinToString(
      outputRowSchema.relDataType,
      joinCondition,
      FlinkJoinRelType.toFlinkJoinRelType(joinType),
      getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    joinExplainTerms(
      super.explainTerms(pw),
      outputRowSchema.relDataType,
      joinCondition,
      FlinkJoinRelType.toFlinkJoinRelType(joinType),
      getExpressionString)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig

    val isLeftAppendOnly = UpdatingPlanChecker.isAppendOnly(left)
    val isRightAppendOnly = UpdatingPlanChecker.isAppendOnly(right)
    if (!isLeftAppendOnly || !isRightAppendOnly) {
      throw new TableException(
        TableErrors.INST.sqlWindowJoinUpdateNotSupported())
    }

    val leftDataStream = left.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    val rightDataStream = right.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)

    // get the equality keys and other condition
    val joinInfo = JoinInfo.of(leftNode, rightNode, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray

    val leftType = leftRowSchema.internalType(classOf[BinaryRow])
    val rightType = rightRowSchema.internalType(classOf[BinaryRow])

    // generate join function
    val joinFunction = WindowJoinUtil.generateJoinFunction(
      config,
      joinType,
      leftType,
      rightType,
      rowType,
      remainCondition,
      ruleDescription)

    joinType match {
      case JoinRelType.INNER =>
        if (isRowTime) {
          // RowTime JoinCoProcessFunction
          throw new TableException(
            TableErrors.INST.sqlWindowJoinRowTimeInnerJoinBetweenStreamNotSupported())
        } else {
          // Proctime JoinCoProcessFunction
          createProcTimeInnerJoinFunction(
            tableEnv,
            leftDataStream,
            rightDataStream,
            DataTypes.toTypeInfo(leftType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
            DataTypes.toTypeInfo(rightType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
            joinFunction.name,
            joinFunction.code,
            leftKeys,
            rightKeys
          )
        }
      case JoinRelType.FULL =>
        throw new TableException(
          TableErrors.INST.sqlWindowJoinBetweenStreamNotSupported("Full Join"))
      case JoinRelType.LEFT =>
        throw new TableException(
          TableErrors.INST.sqlWindowJoinBetweenStreamNotSupported("Left Join"))
      case JoinRelType.RIGHT =>
        throw new TableException(
          TableErrors.INST.sqlWindowJoinBetweenStreamNotSupported("Right Join"))
    }
  }

  def createProcTimeInnerJoinFunction(
      tableEnv: StreamTableEnvironment,
      leftDataStream: StreamTransformation[BaseRow],
      rightDataStream: StreamTransformation[BaseRow],
      leftTypeInfo: BaseRowTypeInfo[BaseRow],
      rightTypeInfo: BaseRowTypeInfo[BaseRow],
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): StreamTransformation[BaseRow] = {

    val returnTypeInfo = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[JoinedRow])
      .asInstanceOf[BaseRowTypeInfo[JoinedRow]]

    val procInnerJoinFunc = new ProcTimeWindowInnerJoin(
      leftLowerBound,
      leftUpperBound,
      leftTypeInfo,
      rightTypeInfo,
      joinFunctionName,
      joinFunctionCode)

    val leftSelect = StreamExecUtil.getKeySelector(leftKeys, leftTypeInfo)
    val rightSelect = StreamExecUtil.getKeySelector(rightKeys, rightTypeInfo)

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftDataStream,
      rightDataStream,
      "Co-Process",
      new KeyedCoProcessOperator(procInnerJoinFunc).asInstanceOf[StreamOperator[BaseRow]],
      returnTypeInfo.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      tableEnv.execEnv.getParallelism)

    if (leftKeys.isEmpty) {
      ret.forceNonParallel()
    }

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }
}
