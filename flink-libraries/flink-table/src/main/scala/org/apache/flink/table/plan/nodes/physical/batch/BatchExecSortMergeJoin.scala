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
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedSorter, ProjectionCodeGenerator, SortCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.plan.cost.BatchExecCost._
import org.apache.flink.table.plan.cost.{FlinkCostFactory, FlinkRelMetadataQuery}
import org.apache.flink.table.plan.nodes.ExpressionFormat
import org.apache.flink.table.runtime.aggregate.{RelFieldCollations, SortUtil}
import org.apache.flink.table.runtime.operator.join.batch.SortMergeJoinOperator
import org.apache.flink.table.runtime.sort.BinaryExternalSorter
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.util.ExecResourceUtil
import org.apache.flink.table.util.ExecResourceUtil.InferMode

import scala.collection.JavaConversions._

trait BatchExecSortMergeJoinBase extends BatchExecJoinBase {

  lazy val joinOperatorName: String = if (getCondition != null) {
    val inFields = inputDataType.getFieldNames.toList
    s"SortMergeJoin(where: ${
      getExpressionString(getCondition, inFields, None, ExpressionFormat.Infix)})"
  } else {
    "SortMergeJoin"
  }

  lazy val (leftAllKey, rightAllKey) = checkAndGetKeys(keyPairs, getLeft, getRight)

  override def toString: String = joinOperatorName

  override def isBarrierNode: Boolean = true

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw).itemIf("reuse_id", getReuseId, isReused)

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val (canDistributionPushDown, leftDistribution, rightDistribution) =
      pushDownHashDistributionIntoNonBroadcastJoin(requiredDistribution)
    if (!canDistributionPushDown) {
      return null
    }
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val requiredFieldCollations = requiredCollation.getFieldCollations
    val shuffleKeysSize = leftDistribution.getKeys.size

    val newLeft = RelOptRule.convert(getLeft, leftDistribution)
    val newRight = RelOptRule.convert(getRight, rightDistribution)

    // SortMergeJoin can provide collation trait, check whether provided collation can satisfy
    // required collations
    val canCollationPushDown = if (requiredCollation.getFieldCollations.isEmpty) {
      false
    } else if (requiredFieldCollations.size > shuffleKeysSize) {
      // Sort by [a, b] can satisfy [a], but cannot satisfy [a, b, c]
      false
    } else {
      val leftKeys = leftDistribution.getKeys
      val leftFieldCnt = getLeft.getRowType.getFieldCount
      val rightKeys = rightDistribution.getKeys.map(_ + leftFieldCnt)
      requiredFieldCollations.zipWithIndex.forall { case (fc, index) =>
        val cfi = fc.getFieldIndex
        if (cfi < leftFieldCnt && flinkJoinType != FlinkJoinRelType.RIGHT) {
          val fieldCollationOnLeftSortKey = RelFieldCollations.of(leftKeys.get(index))
          fc == fieldCollationOnLeftSortKey
        } else if (cfi >= leftFieldCnt &&
          (flinkJoinType == FlinkJoinRelType.RIGHT ||
              flinkJoinType == FlinkJoinRelType.INNER)) {
           val fieldCollationOnRightSortKey = RelFieldCollations.of(rightKeys.get(index))
           fc == fieldCollationOnRightSortKey
        } else {
          false
        }
      }
    }
    var newProvidedTraitSet = getTraitSet.replace(requiredDistribution)
    if (canCollationPushDown) {
      newProvidedTraitSet = newProvidedTraitSet.replace(requiredCollation)
    }
    copy(newProvidedTraitSet, Seq(newLeft, newRight))
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }
    val leftSortCpuCost = COMPARE_CPU_COST * leftRowCnt * Math.log(leftRowCnt)
    val rightSortCpuCost = COMPARE_CPU_COST * rightRowCnt * Math.log(rightRowCnt)
    // cost of evaluating each join condition
    val joinConditionCpuCost = COMPARE_CPU_COST * (leftRowCnt + rightRowCnt)
    val cpuCost = leftSortCpuCost + rightSortCpuCost + joinConditionCpuCost
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    // assume memory is big enough, so sort process and mergeJoin process will not spill to disk.
    val sortMemCost = BatchExecRel.calcNeedMemoryForSort(mq, getLeft) +
        BatchExecRel.calcNeedMemoryForSort(mq, getRight)
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, sortMemCost)
  }

  private def inferLeftRowCountRatio: Double = {
    val mq = FlinkRelMetadataQuery.reuseOrCreate(getCluster.getMetadataQuery)
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      0.5d
    } else {
      leftRowCnt / (rightRowCnt + leftRowCnt)
    }
  }

  private def calcSortMemory(ratio: Double, totalSortMemory: Long): Long = {
    val minGuaranteedMemory = BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM
    val maxGuaranteedMemory = totalSortMemory - BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM
    val inferLeftSortMemory = (totalSortMemory * ratio).toLong
    Math.max(Math.min(inferLeftSortMemory, maxGuaranteedMemory), minGuaranteedMemory)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig

    val leftInput = getLeft.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv)
    val rightInput = getRight.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv)

    val leftType = DataTypes.internal(leftInput.getOutputType).asInstanceOf[BaseRowType]
    val rightType = DataTypes.internal(rightInput.getOutputType).asInstanceOf[BaseRowType]

    val keyType = new BaseRowType(
      classOf[BinaryRow], leftAllKey.map(leftType.getFieldTypes()(_)): _*)

    val condFunc = generateConditionFunction(config, leftType, rightType)

    val externalBufferMemory = ExecResourceUtil.
        getExternalBufferManagedMemory(config)
    val externalBufferMemorySize = externalBufferMemory * ExecResourceUtil.SIZE_IN_MB

    val perRequestSize =
      ExecResourceUtil.getPerRequestManagedMemory(config)* ExecResourceUtil.SIZE_IN_MB
    val infer = ExecResourceUtil.getInferMode(config).equals(InferMode.ALL)

    val leftRatio = if (infer) {
      inferLeftRowCountRatio
    } else {
      0.5d
    }

    val totalReservedSortMemory = (resource.getReservedManagedMem - externalBufferMemory *
        getExternalBufferNum) * ExecResourceUtil.SIZE_IN_MB

    val totalMaxSortMemory = (resource.getMaxManagedMem - externalBufferMemory *
        getExternalBufferNum) * ExecResourceUtil.SIZE_IN_MB

    val sortReservedMemorySize1 = calcSortMemory(leftRatio, totalReservedSortMemory)

    val preferManagedMemorySize1 = calcSortMemory(leftRatio, totalMaxSortMemory)

    // sort code gen
    val operator = new SortMergeJoinOperator(
      sortReservedMemorySize1, preferManagedMemorySize1,
      totalReservedSortMemory - sortReservedMemorySize1,
      totalMaxSortMemory - preferManagedMemorySize1,
      perRequestSize, externalBufferMemorySize,
      flinkJoinType, getRelNodeSize(getLeft) < getRelNodeSize(getRight), condFunc,
      ProjectionCodeGenerator.generateProjection(
        CodeGeneratorContext(config), "SMJProjection", leftType, keyType, leftAllKey.toArray),
      ProjectionCodeGenerator.generateProjection(
        CodeGeneratorContext(config), "SMJProjection", rightType, keyType, rightAllKey.toArray),
      newGeneratedSorter(leftAllKey.toArray, leftType),
      newGeneratedSorter(rightAllKey.toArray, rightType),
      newGeneratedSorter(leftAllKey.indices.toArray, keyType),
      filterNulls,
      tableEnv.getConfig.getParameters.getInteger(TableConfig.SQL_EXEC_SORT_MAX_NUM_FILE_HANDLES))

    val transformation = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftInput,
      rightInput,
      joinOperatorName,
      operator,
      getOutputType,
      resultPartitionCount)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private[flink] def getExternalBufferNum: Int = {
    if (flinkJoinType == FlinkJoinRelType.FULL) 2 else 1
  }

  private def newGeneratedSorter(originalKeys: Array[Int], t: BaseRowType): GeneratedSorter = {
    val originalOrders = originalKeys.map((_) => true)
    val (keys, orders, nullsIsLast) = SortUtil.deduplicationSortKeys(
      originalKeys,
      originalOrders,
      SortUtil.getNullDefaultOrders(originalOrders))

    val types = keys.map(t.getFieldTypes()(_))
    val compAndSers = types.zip(orders).map { case (internalType, order) =>
      (TypeUtils.createComparator(internalType, order), TypeUtils.createSerializer(internalType))
    }
    val comps = compAndSers.map(_._1)
    val sers = compAndSers.map(_._2)

    val gen = new SortCodeGenerator(keys, types, comps, orders, nullsIsLast)
    GeneratedSorter(
      gen.generateNormalizedKeyComputer("SMJComputer"),
      gen.generateRecordComparator("SMJComparator"),
      sers, comps)
  }
}

class BatchExecSortMergeJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    joinCondition: RexNode,
    joinType: JoinRelType,
    val description: String)
  extends Join(cluster, traitSet, left, right, joinCondition, Set.empty[CorrelationId], joinType)
  with BatchExecSortMergeJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join =
    super.supplement(new BatchExecSortMergeJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      description))
}

class BatchExecSortMergeSemiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    joinCondition: RexNode,
    leftKeys: ImmutableIntList,
    rightKeys: ImmutableIntList,
    isAntiJoin: Boolean,
    val description: String)
  extends SemiJoin(cluster, traitSet, left, right, joinCondition, leftKeys, rightKeys, isAntiJoin)
  with BatchExecSortMergeJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      condition: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): SemiJoin = {
    val joinInfo = JoinInfo.of(left, right, condition)
    super.supplement(new BatchExecSortMergeSemiJoin(
      cluster,
      traitSet,
      left,
      right,
      condition,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      isAnti,
      description))
  }
}
