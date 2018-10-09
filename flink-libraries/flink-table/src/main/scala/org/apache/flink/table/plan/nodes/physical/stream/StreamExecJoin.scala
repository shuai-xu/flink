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

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.Pair
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.bundle.{CoBundleTrigger, CombinedCoBundleTrigger, CountCoBundleTrigger, TimeCoBundleTrigger}
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableConfig}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.ProjectionCodeGenerator.generateProjection
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, JoinedRow}
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.common.CommonJoin
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.StreamExecUtil
import org.apache.flink.table.runtime.operator.join.stream.bundle._
import org.apache.flink.table.runtime.operator.join.stream.state.JoinStateHandler
import org.apache.flink.table.runtime.operator.join.stream.state.`match`.JoinMatchStateHandler
import org.apache.flink.table.runtime.operator.join.stream._
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StreamExecJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    val leftNode: RelNode,
    val rightNode: RelNode,
    rowRelDataType: RelDataType,
    val joinCondition: RexNode,
    joinRowType: RelDataType,
    val joinInfo: JoinInfo,
    val filterNulls: Array[Boolean],
    keyPairs: List[IntPair],
    val joinType: FlinkJoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with StreamExecRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      filterNulls,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  def inferPrimaryKeyAndJoinStateType(
      input: RelNode, joinKeys: Array[Int]): (Option[Array[Int]], JoinStateHandler.Type) = {
    val uniqueKeys = cluster.getMetadataQuery.getUniqueKeys(input)
    var (pk, stateType) = if (uniqueKeys != null && uniqueKeys.nonEmpty) {
      var primaryKey = uniqueKeys.head.toArray
      val joinKeyIsPk = uniqueKeys.exists { pk =>
        primaryKey = pk.toArray
        pk.forall(joinKeys.contains(_))
      }
      if (joinKeyIsPk) {
        (Some(primaryKey), JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY)
      } else {
        //select tiny pk
        uniqueKeys.foreach(pk =>
          if (primaryKey.length > pk.length()) primaryKey = pk.toArray)
        (Some(primaryKey), JoinStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY)
      }
    } else {
      (None, JoinStateHandler.Type.WITHOUT_PRIMARY_KEY)
    }
    // if join type is semi/anti and without non equal pred, set right state type to count type.
    if (input.equals(rightNode) &&
        (joinType.equals(FlinkJoinRelType.ANTI) || joinType.equals(FlinkJoinRelType.SEMI)) &&
      joinInfo.isEqui) {
      stateType = JoinStateHandler.Type.COUNT_KEY_SIZE
    }
    (pk, stateType)
  }

  override def producesUpdates: Boolean = {
    joinType != FlinkJoinRelType.INNER && joinType != FlinkJoinRelType.SEMI
  }

  override def producesRetractions: Boolean = {
    joinType match {
      case FlinkJoinRelType.FULL | FlinkJoinRelType.RIGHT | FlinkJoinRelType.LEFT => true
      case FlinkJoinRelType.ANTI => true
      case _ => false
    }
  }

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = {
    def getCurrentRel(node: RelNode): RelNode = {
      node match {
        case _: HepRelVertex => node.asInstanceOf[HepRelVertex].getCurrentRel
        case _ => node
      }
    }

    val realInput = getCurrentRel(input)
    val inputUniqueKeys = cluster.getMetadataQuery.getUniqueKeys(realInput)
    if (inputUniqueKeys != null) {
      val joinKeys = if (input == getCurrentRel(left)) {
        keyPairs.map(_.source).toArray
      } else {
        keyPairs.map(_.target).toArray
      }
      val pkContainJoinKey = inputUniqueKeys.exists {
        uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
      }
      if (pkContainJoinKey) false else true
    } else {
      true
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString)
      .item("join", joinSelectionToString)
      .item("joinType", joinTypeToString)
      .itemIf("joinHint", joinHint, joinHint != null)
  }

  @VisibleForTesting
  def explainJoin: JList[Pair[String, AnyRef]] = {
    val (lStateType, lMatchStateType, rStateType, rMatchStateType) = getJoinAllStateType
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("where", joinConditionToString))
    values.add(Pair.of("join", joinSelectionToString))
    values.add(Pair.of("joinType", joinTypeToString))
    values.add(Pair.of("leftStateType", s"$lStateType"))
    values.add(Pair.of("leftMatchStateType", s"$lMatchStateType"))
    values.add(Pair.of("rightStateType", s"$rStateType"))
    values.add(Pair.of("rightMatchStateType", s"$rMatchStateType"))
    values
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }

  /**
   * Translates the FlinkRelNode into a Flink operator.
   *
   * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
   * @return DataStream of type expectedType or RowTypeInfo
   */
  override def translateToPlan(tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig

    val returnType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[JoinedRow])

    // get the equality keys
    val (leftKeys, rightKeys) = checkAndGetKeys(keyPairs, getLeft, getRight, allowEmpty = true)

    val leftTransform = left.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    val rightTransform = right.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)

    val leftType = leftTransform.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
    val rightType = rightTransform.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]

    val joinOpName = joinToString(joinRowType, joinCondition, joinType, getExpressionString)

    val leftSelect = StreamExecUtil.getKeySelector(leftKeys.toArray, leftType)
    val rightSelect = StreamExecUtil.getKeySelector(rightKeys.toArray, rightType)

    val maxRetentionTime = queryConfig.getMaxIdleStateRetentionTime
    val minRetentionTime = queryConfig.getMinIdleStateRetentionTime
    val lPkProj = generatePrimaryKeyProjection(config, left, leftType, leftKeys.toArray)
    val rPkProj = generatePrimaryKeyProjection(config, right, rightType, rightKeys.toArray)

    val (lStateType, lMatchStateType, rStateType, rMatchStateType) = getJoinAllStateType
    val condFunc = generateConditionFunction(config, leftType, rightType)
    val leftIsAccRetract = StreamExecRetractionRules.isAccRetract(left)
    val rightIsAccRetract = StreamExecRetractionRules.isAccRetract(right)

    val operator = if (queryConfig.isMiniBatchJoinEnabled) {
      joinType match {
        case FlinkJoinRelType.INNER =>
          new BatchInnerJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            getMiniBatchTrigger(queryConfig),
            queryConfig.getParameters.getBoolean(
              StreamQueryConfig.BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.LEFT =>
          new LeftOuterBatchJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            getMiniBatchTrigger(queryConfig),
            queryConfig.getParameters.getBoolean(
              StreamQueryConfig.BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.RIGHT =>
          new RightOuterBatchJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            getMiniBatchTrigger(queryConfig),
            queryConfig.getParameters.getBoolean(
              StreamQueryConfig.BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.FULL =>
          new FullOuterBatchJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            getMiniBatchTrigger(queryConfig),
            queryConfig.getParameters.getBoolean(
              StreamQueryConfig.BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI =>
          new AntiSemiBatchJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            inferMatchStateTypeBase(lStateType),
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            joinType.equals(FlinkJoinRelType.SEMI),
            joinInfo.isEqui,
            filterNulls,
            getMiniBatchTrigger(queryConfig),
            queryConfig.getParameters.getBoolean(
              StreamQueryConfig.BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
      }
    } else {
      joinType match {
        case FlinkJoinRelType.INNER =>
          new InnerJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            filterNulls)
        case FlinkJoinRelType.LEFT =>
          new LeftOuterJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            filterNulls)
        case FlinkJoinRelType.RIGHT =>
          new RightOuterJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            filterNulls)
        case FlinkJoinRelType.FULL =>
          new FullOuterJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            filterNulls)
        case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI =>
          new SemiAntiJoinStreamOperator(
            leftType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            rightType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            joinType.equals(FlinkJoinRelType.ANTI),
            inferMatchStateType(lStateType),
            !rightIsAccRetract,
            filterNulls)
      }
    }

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftTransform,
      rightTransform,
      joinOpName,
      operator,
      returnType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      tableEnv.execEnv.getParallelism)

    if (leftKeys.isEmpty) {
      ret.forceNonParallel()
    }

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.asInstanceOf[ResultTypeQueryable[_]].getProducedType)
    ret
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case FlinkJoinRelType.INNER => "InnerJoin"
    case FlinkJoinRelType.LEFT => "LeftOuterJoin"
    case FlinkJoinRelType.RIGHT => "RightOuterJoin"
    case FlinkJoinRelType.FULL => "FullOuterJoin"
    case FlinkJoinRelType.SEMI => "SemiJoin"
    case FlinkJoinRelType.ANTI => "AntiJoin"
  }

  private[flink] def generatePrimaryKeyProjection(
      config: TableConfig,
      input: RelNode,
      inputType: BaseRowTypeInfo[_], keys: Array[Int]): GeneratedProjection = {

    val (pk, _) = inferPrimaryKeyAndJoinStateType(input, keys)

    if (pk.nonEmpty) {
      val pkType = {
        val types = inputType.getFieldTypes
        new BaseRowTypeInfo(classOf[BinaryRow], pk.get.map(types(_)): _*)
      }
      generateProjection(
        CodeGeneratorContext(config),
        "PkProjection",
        DataTypes.internal(inputType).asInstanceOf[BaseRowType],
        DataTypes.internal(pkType).asInstanceOf[BaseRowType],
        pk.get,
        reusedOutRecord = false)
    } else {
      null
    }
  }

  private[flink] def generateConditionFunction(
      config: TableConfig,
      leftType: BaseRowTypeInfo[_],
      rightType: BaseRowTypeInfo[_]): GeneratedJoinConditionFunction = {
    val ctx = CodeGeneratorContext(config)
    // should consider null fields
    val exprGenerator = new ExprCodeGenerator(ctx, false, true)
        .bindInput(DataTypes.internal(leftType))
        .bindSecondInput(DataTypes.internal(rightType))

    val body = if (joinInfo.isEqui) {
      // only equality condition
      "return true;"
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(cluster.getRexBuilder)
      val condition = exprGenerator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinConditionFunction(
      ctx,
      "ConditionFunction",
      body, config)
  }

  private[flink] def getJoinAllStateType: (JoinStateHandler.Type, JoinMatchStateHandler.Type,
      JoinStateHandler.Type, JoinMatchStateHandler.Type) = {
    // get the equality keys
    val (leftKeys, rightKeys) = checkAndGetKeys(keyPairs, getLeft, getRight, allowEmpty = true)
    val (_, lStateType) = inferPrimaryKeyAndJoinStateType(getLeft, leftKeys.toArray)
    val (_, rStateType) = inferPrimaryKeyAndJoinStateType(getRight, rightKeys.toArray)

    val (lStateMatchType, rStateMatchType) = joinType match {
      case FlinkJoinRelType.INNER =>
        (JoinMatchStateHandler.Type.EMPTY_MATCH, JoinMatchStateHandler.Type.EMPTY_MATCH)

      case FlinkJoinRelType.LEFT =>
        (inferMatchStateType(lStateType), JoinMatchStateHandler.Type.EMPTY_MATCH)

      case FlinkJoinRelType.RIGHT =>
        (JoinMatchStateHandler.Type.EMPTY_MATCH, inferMatchStateType(rStateType))

      case FlinkJoinRelType.FULL =>
        (inferMatchStateType(lStateType), inferMatchStateType(rStateType))

      case FlinkJoinRelType.SEMI | FlinkJoinRelType.ANTI =>
        (inferMatchStateType(lStateType), JoinMatchStateHandler.Type.EMPTY_MATCH)
    }

    (lStateType, lStateMatchType, rStateType, rStateMatchType)
  }

  private[flink] def inferMatchStateType(
      inputSideStateType: JoinStateHandler.Type): JoinMatchStateHandler.Type = {

    var matchType = inferMatchStateTypeBase(inputSideStateType)
    //if joinType is Semi and the right side don't send retraction message. then semi operator
    // won't keep the match counts.
    if (joinType.equals(FlinkJoinRelType.SEMI) && !StreamExecRetractionRules.isAccRetract(right)) {
      matchType = JoinMatchStateHandler.Type.EMPTY_MATCH
    }
    matchType
  }

  private[flink] def inferMatchStateTypeBase(inputSideStateType: JoinStateHandler.Type):
  JoinMatchStateHandler.Type = {

    if (joinInfo.isEqui) {
      //only equality condition
      JoinMatchStateHandler.Type.ONLY_EQUALITY_CONDITION_EMPTY_MATCH
    } else {
      inputSideStateType match {
        case JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY =>
          JoinMatchStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY_MATCH

        case JoinStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY =>
          JoinMatchStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY_MATCH

        case _ =>
          //match more than one time
          JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH
      }
    }
  }

  private def getMiniBatchTrigger(queryConfig: StreamQueryConfig) = {
    val timeTrigger: Option[CoBundleTrigger[BaseRow, BaseRow]] =
      if (queryConfig.isMicroBatchEnabled) {
        None
      } else {
        Some(new TimeCoBundleTrigger[BaseRow, BaseRow](queryConfig.getMiniBatchTriggerTime))
      }
    val sizeTrigger: Option[CoBundleTrigger[BaseRow, BaseRow]] =
      if (queryConfig.getMiniBatchTriggerSize == Long.MinValue) {
        None
      } else {
        Some(new CountCoBundleTrigger[BaseRow, BaseRow](queryConfig.getMiniBatchTriggerSize))
      }
    new CombinedCoBundleTrigger[BaseRow, BaseRow](
      Array(timeTrigger, sizeTrigger)
        .filter(_.isDefined)
        .map(_.get)
    )
  }
}

