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

import java.util.{Arrays => JArrays, List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelFieldCollation, RelNode, RelWriter}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.cost.BatchExecCost.FUNC_CPU_COST
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.calcite.SegmentTop
import org.apache.flink.table.runtime.operator.SegmentTopOperator
import org.apache.flink.table.typeutils.TypeUtils

import scala.collection.JavaConversions._

class BatchExecSegmentTop(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode,
    groupKeys: ImmutableBitSet,
    fieldCollation: RelFieldCollation,
    withTies: ImmutableBitSet,
    ruleDescription: String)
  extends SegmentTop(cluster, traits, inputRel, groupKeys, fieldCollation, withTies)
  with RowBatchExecRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    super.supplement(new BatchExecSegmentTop(
      cluster,
      traitSet,
      inputs.get(0),
      groupKeys,
      fieldCollation,
      withTies,
      ruleDescription))
  }

  override def isBarrierNode: Boolean = false

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator, only need to compare between agg column.
    val inputRows = mq.getRowCount(getInput())
    val cpu = FUNC_CPU_COST * inputRows
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val writer = super.explainTerms(pw)
      .itemIf("groupBy", fieldsToString(groupKeys), groupKeys.cardinality > 0)
      .item("orderBy", orderingToString(getRowType, JArrays.asList(fieldCollation)))
      .item("withTies", fieldsToString(withTies))
    writer.itemIf("reuse_id", getReuseId, isReused)
    writer
  }

  private[flink] def fieldsToString(fieldIndices: ImmutableBitSet): String = {
    val inFields = getRowType.getFieldNames
    fieldIndices.asList().map(inFields(_)).mkString(", ")
  }

  private[flink] def orderingToString(
    inputType: RelDataType,
    orderFields: JList[RelFieldCollation]): String = {

    val inFields = inputType.getFieldList

    orderFields.map {
      x => s"${inFields(x.getFieldIndex).getName} ${x.direction.shortString}"
    }.mkString(", ")
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  override protected def translateToPlanInternal(
    tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[BaseRow] = {
    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)
    val outputType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[JoinedRow])
    val sortingKeys = groupKeys.toArray
    // The collation for the grouping fields is inessential here, we only use the
    // comparator to distinguish different groups.
    // (order[is_asc], null_is_last)
    val sortCollation = sortingKeys.map(_ => (true, true))

    val inputRowType = FlinkTypeFactory.toInternalBaseRowType(inputRel.getRowType, classOf[BaseRow])
    val (groupComparators, groupSerializers) = TypeUtils.flattenComparatorAndSerializer(
      inputRowType.getArity,
      sortingKeys,
      sortCollation.map(_._1),
      inputRowType.getFieldTypes)
    val sortCodeGen = new SortCodeGenerator(
      sortingKeys,
      sortingKeys.map(inputRowType.getTypeAt),
      groupComparators,
      sortCollation.map(_._1),
      sortCollation.map(_._2))
    val groupingSorter = GeneratedSorter(
      null,
      sortCodeGen.generateRecordComparator("GroupingComparator"),
      groupSerializers,
      groupComparators)

    val tiesCollation = withTies.toArray.map(_ => (true, true))
    val tiesKeys = withTies.toArray

    val (tiesComparators, tiesSerializers) = TypeUtils.flattenComparatorAndSerializer(
      inputRowType.getArity,
      tiesKeys,
      tiesCollation.map(_._1),
      inputRowType.getFieldTypes)
    val tiesSortCodeGen = new SortCodeGenerator(
      tiesKeys,
      tiesKeys.map(inputRowType.getTypeAt),
      tiesComparators,
      tiesCollation.map(_._1),
      tiesCollation.map(_._2))
    val tiesSorter = GeneratedSorter(
      null,
      tiesSortCodeGen.generateRecordComparator("TiesComparator"),
      tiesSerializers,
      tiesComparators)

    //operator needn't cache data
    val operator = new SegmentTopOperator(groupingSorter, tiesSorter)
    val transformation = new OneInputTransformation(
      input,
      "SegmentTop",
      operator,
      outputType.asInstanceOf[TypeInformation[BaseRow]],
      resultPartitionCount)
    operator.setRelID(transformation.getId)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(reservedResSpec, preferResSpec)
    transformation
  }
}
