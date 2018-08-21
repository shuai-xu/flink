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
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.{BatchExecTableSource, LimitableTableSource}

/**
  * Flink RelNode to read data from an external source defined by a [[BatchExecTableSource]].
  */
class BatchExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with BatchExecScan {

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    flinkTypeFactory.buildLogicalRowType(tableSource.getTableSchema)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    if (rowCnt == null) {
      return null
    }
    val cpu = 0
    val size = rowCnt * mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, cpu, size)
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter =
    super.explainTerms(pw).itemIf("reuse_id", getReuseId, isReused)

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): PhysicalTableSourceScan = {
    super.supplement(new BatchExecTableSourceScan(cluster, traitSet, relOptTable))
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
    val input = getSourceTransformation(tableEnv.streamEnv).asInstanceOf[StreamTransformation[Any]]
    assignSourceResourceAndParallelism(tableEnv, input)
    convertToInternalRow(
      tableEnv, input, getRowType, new TableSourceTable(tableSource), config)
  }

  override def needInternalConversion: Boolean = {
    needsConversion(new TableSourceTable(tableSource).dataType)
  }

  private def getSourceTransformation(streamEnv: StreamExecutionEnvironment):
    StreamTransformation[_] = {
    tableSource.asInstanceOf[BatchExecTableSource[_]]
      .getBoundedStream(streamEnv).getTransformation
  }

  override private[flink] def getTableSourceResultPartitionNum(
    tableEnv: BatchTableEnvironment): (Boolean, Int) = {
    tableSource match {
      case source: LimitableTableSource if source.isLimitPushedDown => (true, 1)
      case _ =>
        val transformation = getSourceTransformation(tableEnv.streamEnv)
        (transformation.isParallelismLocked, transformation.getParallelism)
    }
  }

  override private[flink] def getTableSourceResource(
    tableEnv: BatchTableEnvironment): ResourceSpec = {
    getSourceTransformation(tableEnv.streamEnv).getMinResources
  }
}
