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
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel._
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.{ImmutableIntList, Util}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{BaseRowType, DataTypes}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.runtime.aggregate.RelFieldCollations
import org.apache.flink.table.runtime.operator.OneInputSubstituteStreamOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.FlinkRelOptUtil

import scala.collection.JavaConversions._

class BatchExecSortAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean)
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
    isMerge = isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    super.supplement(new BatchExecSortAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputRelDataType,
      grouping,
      auxGrouping,
      isMerge))
  }

  override def isBarrierNode: Boolean = true

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val pushDownDistribution = requiredDistribution.getType match {
      case SINGLETON => if (grouping.length == 0) requiredDistribution else null
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        if (requiredDistribution.requireStrict) {
          if (shuffleKeys == groupKeysList) {
            FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList)
          } else {
            null
          }
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // Hash [a] can satisfy Hash[a, b]
          FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList, requireStrict = false)
        } else {
          val tableConfig = FlinkRelOptUtil.getTableConfig(this)
          if (tableConfig.aggregateShuffleByPartialKeyEnabled &&
              groupKeysList.containsAll(shuffleKeys)) {
            // If partialKey is enabled, push down partialKey requirement into input.
            FlinkRelDistribution.hash(shuffleKeys.map(k => Integer.valueOf(grouping(k))),
              requireStrict = false)
          } else {
            null
          }
        }
      case _ => null
    }
    if (pushDownDistribution == null) {
      return null
    }
    val providedCollation = if (grouping.length == 0) {
      RelCollations.EMPTY
    } else {
      val providedFieldCollations = grouping.map(RelFieldCollations.of).toList
      RelCollations.of(providedFieldCollations)
    }
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
      getTraitSet.replace(requiredDistribution).replace(requiredCollation)
    } else {
      getTraitSet.replace(requiredDistribution)
    }
    val newInput = RelOptRule.convert(getInput, pushDownDistribution)
    copy(newProvidedTraitSet, Seq(newInput))
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("isMerge", isMerge)
      .itemIf("groupBy", groupingToString(inputRelDataType, grouping), grouping.nonEmpty)
      .itemIf("auxGrouping", groupingToString(inputRelDataType, auxGrouping), auxGrouping.nonEmpty)
      .item("select", aggregationToString(
        inputRelDataType,
        grouping,
        auxGrouping,
        rowRelDataType,
        aggCallToAggFunction.map(_._1),
        aggCallToAggFunction.map(_._2),
        isMerge,
        isGlobal = true))
      .itemIf("reuse_id", getReuseId, isReused)
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
      codegenWithoutKeys(
        isMerge, isFinal = true, ctx, tableEnv, inputType, outputRowType, "NoGrouping")
    } else {
      codegenWithKeys(ctx, tableEnv, inputType, outputRowType)
    }
    val operator = new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)

    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = {
    val aggregateNamePrefix = if (isMerge) "Global" else "Complete"
    aggregateNamePrefix + "SortAggregate"
  }

  override def toString: String = getOperatorName
}
