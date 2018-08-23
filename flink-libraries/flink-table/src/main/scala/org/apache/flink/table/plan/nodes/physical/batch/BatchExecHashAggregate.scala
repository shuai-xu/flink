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
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.{ImmutableIntList, Util}
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment, TableConfig}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.operator.SubstituteStreamOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{BatchExecResourceUtil, FlinkRelOptUtil}

import scala.collection.JavaConversions._

class BatchExecHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean)
  extends BatchExecHashAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputType,
    grouping,
    auxGrouping,
    isMerge = isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    super.supplement(new BatchExecHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputType,
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
            FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList, requireStrict = true)
          } else {
            null
          }
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // Hash [a] can satisfy Hash[a, b]
          FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList)
        } else {
          val tableConfig = FlinkRelOptUtil.getTableConfig(this)
          if (tableConfig.aggregateShuffleByPartialKeyEnabled &&
              groupKeysList.containsAll(shuffleKeys)) {
            // If partialKey is enabled, push down partialKey requirement into input.
           FlinkRelDistribution.hash(
             shuffleKeys.map(k => Integer.valueOf(grouping(k))))
          } else {
            null
          }
        }
      case _ => null
    }
    if (pushDownDistribution == null) {
      return null
    }
    val newInput = RelOptRule.convert(getInput, pushDownDistribution)
    val newProvidedTraitSet = getTraitSet.replace(requiredDistribution)
    copy(newProvidedTraitSet, Seq(newInput))
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
        .item("isMerge", isMerge)
        .itemIf("groupBy", groupingToString(inputType, grouping), grouping.nonEmpty)
        .itemIf("auxGrouping", groupingToString(inputType, auxGrouping), auxGrouping.nonEmpty)
        .item("select", aggregationToString(
          inputType,
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
    * @param queryConfig The configuration for the query to generate.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[BaseRow] = {
    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)
    val ctx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val outputRowType = getOutputRowType
    val inputType = DataTypes.internal(input.getOutputType).asInstanceOf[BaseRowType]
    LOG.info(
      this + " the reserved: " + reservedResSpec + ", and the preferred: " + preferResSpec + ".")
    val generatedOperator = if (grouping.isEmpty) {
      codegenWithoutKeys(isMerge, isFinal, ctx, tableEnv, inputType, outputRowType, "NoGrouping")
    } else {
      val reservedManagedMem =
        BatchExecResourceUtil.getManagedMemory(reservedManagedMem) * BatchExecResourceUtil.SIZE_IN_MB
      val preferredManagedMem =
        BatchExecResourceUtil.getManagedMemory(preferredManagedMem) * BatchExecResourceUtil.SIZE_IN_MB
      codegenWithKeys(
        ctx,
        tableEnv,
        inputType,
        outputRowType,
        reservedManagedMem,
        preferredManagedMem)
    }
    val operator = new SubstituteStreamOperator[BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    operator.setRelID(transformation.getId)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    tableEnv.getRUKeeper().setRelID(this, transformation.getId)

    transformation.setResources(reservedResSpec, preferResSpec)

    transformation
  }

  private def getOperatorName = {
    val aggregateNamePrefix = if (isMerge) "Global" else "Complete"
    getAggOperatorName(aggregateNamePrefix + "HashAggregate")
  }

  override def toString: String = getOperatorName
}
