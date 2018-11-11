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

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.batch.BatchExecRelVisitor
import org.apache.flink.table.plan.cost.BatchExecCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.operator.sort.SortOperator
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.ExecResourceUtil

import scala.collection.JavaConversions._

class BatchExecSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    collations: RelCollation,
    ruleDescription: String)
  extends Sort(cluster, traitSet, inp, collations)
  with RowBatchExecRel {

  private val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(collations.getFieldCollations)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort =
    new BatchExecSort(cluster, traitSet, newInput, newCollation, ruleDescription)

  override def isBarrierNode: Boolean = true

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("orderBy", SortUtil.sortFieldsToString(collations, getRowType))
      .itemIf("reuse_id", getReuseId, isReused)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = mq.getRowCount(input)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(getInput())
    if (rowCount == null) {
      return null
    }
    val numOfSort = collations.getFieldCollations.size()
    val cpuCost = COMPARE_CPU_COST * numOfSort * rowCount * Math.log(rowCount)
    val memCost = BatchExecRel.calcNeedMemoryForSort(mq, input)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv)
    val binaryType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BinaryRow])

    // sort code gen
    val (comparators, serializers, codeGen) = getSortInfo(tableEnv.getConfig)

    val reservedMangedMemorySize = resource.getReservedManagedMem * ExecResourceUtil.SIZE_IN_MB

    val preferMangedMemorySize = resource.getMaxManagedMem * ExecResourceUtil.SIZE_IN_MB
    val perRequestSize =
      ExecResourceUtil.getPerRequestManagedMemory(
        tableEnv.getConfig)* ExecResourceUtil.SIZE_IN_MB

    val operator = new SortOperator(
      reservedMangedMemorySize,
      preferMangedMemorySize,
      perRequestSize.toLong,
      GeneratedSorter(
        codeGen.generateNormalizedKeyComputer("SortBatchExecComputer"),
        codeGen.generateRecordComparator("SortBatchExecComparator"),
        serializers, comparators))

    val transformation = new OneInputTransformation(
      input,
      s"Sort(${SortUtil.sortFieldsToString(collations, getRowType)})",
      operator.asInstanceOf[OneInputStreamOperator[BaseRow, BaseRow]],
      binaryType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      resultPartitionCount)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)

    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private def getSortInfo(tableConfig: TableConfig)
    : (Array[TypeComparator[_]], Array[TypeSerializer[_]], SortCodeGenerator) = {
    val inputRowType = FlinkTypeFactory.toInternalBaseRowType(input.getRowType, classOf[BaseRow])
    // sort code gen
    val keyTypes = keys.map(inputRowType.getFieldTypes()(_))
    val compAndSers = keyTypes.zip(orders).map { case (internalType, order) =>
      (TypeUtils.createComparator(internalType, order), TypeUtils.createSerializer(internalType))
    }
    val comps = compAndSers.map(_._1)
    val sers = compAndSers.map(_._2)
    val codeGen = new SortCodeGenerator(keys, keyTypes, comps, orders, nullsIsLast)
    (comps, sers, codeGen)
  }
}
