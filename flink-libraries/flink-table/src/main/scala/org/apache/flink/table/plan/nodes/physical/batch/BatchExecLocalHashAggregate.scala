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
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.operator.SubstituteStreamOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BatchExecResourceUtil

import scala.collection.JavaConversions._
import scala.collection.mutable

class BatchExecLocalHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int])
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
    isMerge = false,
    isFinal = false) {

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
        .itemIf("groupBy", groupingToString(inputType, grouping), grouping.nonEmpty)
        .itemIf("auxGrouping", groupingToString(inputType, auxGrouping), auxGrouping.nonEmpty)
        .item("select", aggregationToString(
          inputType,
          grouping,
          auxGrouping,
          rowRelDataType,
          aggCallToAggFunction.map(_._1),
          aggCallToAggFunction.map(_._2),
          isMerge = false,
          isGlobal = false))
        .itemIf("reuse_id", getReuseId, isReused)
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    super.supplement(new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputType,
      grouping,
      auxGrouping))
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
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
        val pushDownRelTraits = input.getTraitSet.replace(pushDownDistribution)
        val newInput = RelOptRule.convert(getInput, pushDownRelTraits)
        copy(getTraitSet.replace(requiredDistribution), Seq(newInput))
      case _ => null
    }
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
    val generatedOperator = if (grouping.isEmpty) {
      codegenWithoutKeys(isMerge = false, isFinal = false, ctx, tableEnv,
        inputType, outputRowType, "NoGrouping")
    } else {
      val managedMem = reservedResSpec.getManagedMemoryInMB *
          BatchExecResourceUtil.SIZE_IN_MB
      val preferredManagedMem =
        preferResSpec.getManagedMemoryInMB * BatchExecResourceUtil.SIZE_IN_MB
      codegenWithKeys(ctx, tableEnv, inputType, outputRowType, managedMem,
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
    LOG.info(
      this + " the reserved: " + reservedResSpec + ", and the preferred: " + preferResSpec + ".")
    operator.setRelID(transformation.getId)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    tableEnv.getRUKeeper().setRelID(this, transformation.getId)

    transformation.setResources(reservedResSpec, preferResSpec)
    transformation
  }

  private def getOperatorName = getAggOperatorName("LocalHashAggregate")

  override def toString: String = getOperatorName
}
