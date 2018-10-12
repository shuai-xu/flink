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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.{AggsHandlerCodeGenerator, BatchExecAggregateCodeGen}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.dataview.DataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.functions.utils.CoTableValuedAggSqlFunction
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.cost.BatchExecCost.FUNC_CPU_COST
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.CoTableValuedAggregate
import org.apache.flink.table.plan.util.{AggregateInfo, AggregateInfoList, CoAggregateInfo}
import org.apache.flink.table.runtime.operator.join.batch.SortMergeCoTableValuedAggOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BatchExecResourceUtil.InferMode
import org.apache.flink.table.util.BatchExecResourceUtil

import scala.collection.JavaConversions._
import scala.collection.mutable

class BatchExecCoGroupTableValuedAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    lRexCall: RexCall,
    rRexCall: RexCall,
    groupKey1: Seq[RexNode],
    groupKey2: Seq[RexNode])
  extends CoTableValuedAggregate(
    cluster,
    traitSet,
    left,
    right,
    lRexCall,
    rRexCall,
    lRexCall.getOperator.asInstanceOf[CoTableValuedAggSqlFunction].externalResultType,
    groupKey1,
    groupKey2)
    with BatchExecAggregateCodeGen
    with CommonSortMergeJoin
    with RowBatchExecRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    super.supplement(new BatchExecCoGroupTableValuedAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      lRexCall,
      rRexCall,
      groupKey1,
      groupKey2))
  }

  lazy val offset = leftNode.getRowType.getFieldCount
  lazy val leftAllKey = groupKey1.map(_.asInstanceOf[RexInputRef].getIndex).toArray
  lazy val rightAllKey = groupKey2.map(_.asInstanceOf[RexInputRef].getIndex - offset).toArray

  lazy val keyType = new BaseRowType(
    classOf[BinaryRow],
    leftAllKey.map { index =>
      FlinkTypeFactory.toInternalType(leftNode.getRowType.getFieldList.get(index).getType)
    }, leftAllKey.map(leftNode.getRowType.getFieldNames.get(_)))

  override def isBarrierNode: Boolean = true

  def getCost(planner: RelOptPlanner, mq: RelMetadataQuery, inputRows: Double)
  : (Double, Double, Double) = {

    val cpuCost = FUNC_CPU_COST * inputRows
    val rowCountCost: Double = mq.getRowCount(this) * 2
    val memCost: Double = mq.getAverageRowSize(this)
    (rowCountCost, cpuCost, memCost)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }

    val leftCost = getCost(planner, mq, leftRowCnt)
    val rightCost = getCost(planner, mq, rightRowCnt)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(
      leftCost._1 + rightCost._1,
      leftCost._2 + rightCost._2,
      0,
      0,
      leftCost._3 + rightCost._3)
  }

  /**
    * Push down hash distribution for downstream node.
    */
  def pushDownHashDistributionIntoCoTableValuedAgg(requiredDistribution: FlinkRelDistribution)
  : (Boolean, FlinkRelDistribution, FlinkRelDistribution) = {

    // Only HashDistribution can be push down
    if (requiredDistribution.getType != HASH_DISTRIBUTED) {
      return (false, null, null)
    }

    val requiredShuffleKeys = requiredDistribution.getKeys
    val requiredLeftShuffleKeys = mutable.ArrayBuffer[Int]()
    val requiredRightShuffleKeys = mutable.ArrayBuffer[Int]()
    requiredShuffleKeys.foreach { key =>
      // only left keys are selected by CoTableValuedAgg
      if (key < lRexCall.operands.size()) {
        // get left index and right index in inputs
        requiredLeftShuffleKeys += groupKey1(key).asInstanceOf[RexInputRef].getIndex
        requiredRightShuffleKeys +=
          groupKey2(key).asInstanceOf[RexInputRef].getIndex - left.getRowType.getFieldCount
      } else {
        return (false, null, null)
      }
    }

    (true,
      FlinkRelDistribution.hash(
        ImmutableIntList.of(requiredLeftShuffleKeys: _*), requireStrict = true),
      FlinkRelDistribution.hash(
        ImmutableIntList.of(requiredRightShuffleKeys: _*), requireStrict = true)
    )
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val (canPushDown, leftDistribution, rightDistribution) =
      pushDownHashDistributionIntoCoTableValuedAgg(requiredDistribution)
    if (!canPushDown) {
      return null
    }
    val toRestrictHashDistributionByKeys = (distribution: FlinkRelDistribution) =>
      getCluster.getPlanner.emptyTraitSet.replace(FlinkConventions.BATCHEXEC).replace(distribution)
    val leftRequiredTrait = toRestrictHashDistributionByKeys(leftDistribution)
    val rightRequiredTrait = toRestrictHashDistributionByKeys(rightDistribution)
    val newLeft = RelOptRule.convert(getLeft, leftRequiredTrait)
    val newRight = RelOptRule.convert(getRight, rightRequiredTrait)
    // Can not push down collation into CoTableValuedAgg.
    copy(getTraitSet.replace(requiredDistribution), Seq(newLeft, newRight))
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

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

  def getOutputRowTypeInfo: BaseRowTypeInfo[BaseRow] = {
    val outputTypeInfo = if (groupKey1.isEmpty) {
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[GenericRow])
    } else {
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[JoinedRow])
    }
    outputTypeInfo.asInstanceOf[BaseRowTypeInfo[BaseRow]]
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  override def translateToPlanInternal(
    tableEnv: BatchTableEnvironment,
    queryConfig: BatchQueryConfig): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig

    val leftInput = getLeft.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)
    val rightInput = getRight.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)

    val leftType = DataTypes.internal(leftInput.getOutputType).asInstanceOf[BaseRowType]
    val rightType = DataTypes.internal(rightInput.getOutputType).asInstanceOf[BaseRowType]

    val perRequestSize =
      BatchExecResourceUtil.getPerRequestManagedMemory(config)* BatchExecResourceUtil.SIZE_IN_MB
    val infer = BatchExecResourceUtil.getInferMode(config).equals(InferMode.ALL)

    val leftRatio = if (infer) {
      inferLeftRowCountRatio(getLeft, getRight, getCluster.getMetadataQuery)
    } else {
      0.5d
    }

    val totalReservedSortMemory = resource.getReservedManagedMem * BatchExecResourceUtil.SIZE_IN_MB
    val totalMaxSortMemory = resource.getMaxManagedMem * BatchExecResourceUtil.SIZE_IN_MB

    val sortReservedMemorySize1 = calcSortMemory(leftRatio, totalReservedSortMemory)
    val preferManagedMemorySize1 = calcSortMemory(leftRatio, totalMaxSortMemory)

    val input1Types =
      lRexCall.getOperands.toArray
        .map(n => FlinkTypeFactory.toInternalType(n.asInstanceOf[RexNode].getType))
    val input2Types =
      rRexCall.getOperands.toArray
        .map(n => FlinkTypeFactory.toInternalType(n.asInstanceOf[RexNode].getType))

    val sqlFunction: CoTableValuedAggSqlFunction =
      lRexCall.getOperator.asInstanceOf[CoTableValuedAggSqlFunction]
    val accType = sqlFunction.externalAccType
    val resultType = sqlFunction.externalResultType
    val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
      0,
      sqlFunction.getFunction,
      accType,
      true)

    val aggInfoList = AggregateInfoList(Array[AggregateInfo](
      new CoAggregateInfo(
        null,
        sqlFunction.getFunction,
        0,
        input1Types.zipWithIndex.map(_._2),
        input2Types.zipWithIndex.map(_._2),
        Array(newExternalAccType),
        specs,
        resultType)))

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig, supportReference = true),
      tableEnv.getRelBuilder,
      false,
      needMerge = false,
      tableEnv.getConfig.getNullCheck)
      .bindInput(input1Types)
      .bindSecondInput(input2Types)

    val generateAggHandler: GeneratedCoTableValuedAggHandleFunction =
      generator.generateCoTableValuedAggHandler("CoGroupTableValuedAggHandler", aggInfoList)

    // sort code gen
    val operator = new SortMergeCoTableValuedAggOperator(
      sortReservedMemorySize1,
      preferManagedMemorySize1,
      totalReservedSortMemory - sortReservedMemorySize1,
      totalMaxSortMemory - preferManagedMemorySize1,
      perRequestSize,
      ProjectionCodeGenerator.generateProjection(
        CodeGeneratorContext(config), "SMCTAGGProjection", leftType, keyType, leftAllKey),
      ProjectionCodeGenerator.generateProjection(
        CodeGeneratorContext(config), "SMCTAGGProjection", rightType, keyType, rightAllKey),
      newGeneratedSorter(leftAllKey, leftType),
      newGeneratedSorter(rightAllKey, rightType),
      newGeneratedSorter(leftAllKey.indices.toArray, keyType),
      generateAggHandler)

    val transformation = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftInput,
      rightInput,
      toString,
      operator,
      getOutputRowTypeInfo,
      resultPartitionCount)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  override def toString: String = {
    s"CoGroupTableValuedAggregate(${
      if (groupKey1.nonEmpty) {
        s"groupBy1: (${groupingToString(left.getRowType, groupKey1, 0)}), " +
          s"groupBy2: (${groupingToString(
            right.getRowType, groupKey2, left.getRowType.getFieldCount)})"
      } else {
        ""
      }
    }coAggApply:(${
      buildAggregationToString(
        left.getRowType,
        right.getRowType,
        lRexCall: RexCall,
        rRexCall: RexCall)
    }))"
  }
}
