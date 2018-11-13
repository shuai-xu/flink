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

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.calcite.rel._
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{BaseRowType, DataTypes}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.runtime.aggregate.RelFieldCollations
import org.apache.flink.table.runtime.operator.OneInputSubstituteStreamOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._
import scala.collection.mutable

class BatchExecLocalSortAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int])
  extends BatchExecSortAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    isMerge = false,
    isFinal = false) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecLocalSortAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputRelDataType,
      grouping,
      auxGrouping)
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(inputRelDataType, grouping), grouping.nonEmpty)
      .itemIf("auxGrouping", groupingToString(inputRelDataType, auxGrouping), auxGrouping.nonEmpty)
      .item("select", aggregationToString(
        inputRelDataType,
        grouping,
        auxGrouping,
        rowRelDataType,
        aggCallToAggFunction.map(_._1),
        aggCallToAggFunction.map(_._2),
        isMerge = false,
        isGlobal = false))
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    // Does not to try to satisfy requirement by localAgg's input if enforce to use two-stage agg.
    if (isEnforceTwoStageAgg) {
      return null
    }
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    requiredDistribution.getType match {
      case Type.HASH_DISTRIBUTED | Type.RANGE_DISTRIBUTED =>
        val groupSetLen = grouping.length
        val mappingKeys = mutable.ArrayBuffer[Int]()
        requiredDistribution.getKeys.foreach { key =>
          if (key < groupSetLen) {
            mappingKeys += grouping(key)
          } else {
            // Cannot push down distribution if keys are not group keys of agg
            return null
          }
        }
        val pushDownDistributionKeys = ImmutableIntList.of(mappingKeys: _*)
        val pushDownDistribution = requiredDistribution.getType match {
          case Type.HASH_DISTRIBUTED =>
            FlinkRelDistribution.hash(pushDownDistributionKeys, requiredDistribution.requireStrict)
          case Type.RANGE_DISTRIBUTED => FlinkRelDistribution.range(pushDownDistributionKeys)
        }
        val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
        val providedFieldCollations = (0 until groupSetLen).map(RelFieldCollations.of)
        val providedCollation = RelCollations.of(providedFieldCollations)
        val newTraitSet = if (providedCollation.satisfies(requiredCollation)) {
          getTraitSet.replace(requiredDistribution).replace(requiredCollation)
        } else {
          getTraitSet.replace(requiredDistribution)
        }
        val pushDownRelTraits = input.getTraitSet.replace(pushDownDistribution)
        val newInput = RelOptRule.convert(getInput, pushDownRelTraits)
        copy(newTraitSet, Seq(newInput))
      case _ => null
    }
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv)
    val outputRowType = getOutputRowType
    val ctx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val inputType = DataTypes.internal(input.getOutputType).asInstanceOf[BaseRowType]
    val generatedOperator = if (grouping.isEmpty) {
      codegenWithoutKeys(isMerge = false, isFinal = false,
        ctx, tableEnv, inputType, outputRowType, "NoGrouping")
    } else {
      codegenWithKeys(ctx, tableEnv, inputType, outputRowType)
    }
    val operator = new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)
    val transformation = new OneInputTransformation[BaseRow, BaseRow](
      input,
      getAggOperatorName("LocalSortAggregate"),
      operator,
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = getAggOperatorName("LocalSortAggregate")

  override def toString: String = getOperatorName
}
