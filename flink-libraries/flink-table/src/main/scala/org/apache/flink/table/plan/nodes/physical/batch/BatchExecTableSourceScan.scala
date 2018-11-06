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
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment, TableException}
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.{BatchExecTableSource, LimitableTableSource, TableSourceUtil}
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.typeutils.TypeUtils
import java.lang.{Boolean => JBoolean}
import java.lang.{Integer => JInteger}

import org.apache.flink.table.plan.batch.BatchExecRelVisitor

/**
  * Flink RelNode to read data from an external source defined by a [[BatchExecTableSource]].
  */
class BatchExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with BatchExecScan {

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
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      None)

    val config = tableEnv.getConfig
    val input = getSourceTransformation(tableEnv.streamEnv).asInstanceOf[StreamTransformation[Any]]
    assignSourceResourceAndParallelism(tableEnv, input)

    // check that declared and actual type of table source DataSet are identical
    if (input.getOutputType != TypeUtils.createTypeInfoFromDataType(tableSource.getReturnType)) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
          s"returned a DataSet of type ${input.getOutputType} that does not match with the " +
          s"type ${tableSource.getReturnType} declared by the TableSource.getReturnType() " +
          s"method. Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      None,
      cluster,
      tableEnv.getRelBuilder,
      DataTypes.TIMESTAMP
    )

    convertToInternalRow(tableEnv, input, fieldIndexes,
      getRowType, tableSource.getReturnType, config, rowtimeExpression)
  }

  override def needInternalConversion = {
    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      None)
    hasTimeAttributeField(fieldIndexes) || needsConversion(tableSource.getReturnType)
  }

  private def getSourceTransformation(streamEnv: StreamExecutionEnvironment):
    StreamTransformation[_] = {
    tableSource.asInstanceOf[BatchExecTableSource[_]]
      .getBoundedStream(streamEnv).getTransformation
  }

  override private[flink] def getTableSourceResultPartitionNum(
      tableEnv: TableEnvironment): JTuple[JBoolean, JInteger] = {
    tableSource match {
      case source: LimitableTableSource if source.isLimitPushedDown => new JTuple(true, 1)
      case _ =>
        val transformation = getSourceTransformation(
          tableEnv.asInstanceOf[BatchTableEnvironment].streamEnv)
        new JTuple(transformation.isParallelismLocked, transformation.getParallelism)
    }
  }

  override private[flink] def getTableSourceResource(
    tableEnv: TableEnvironment): ResourceSpec = {
    getSourceTransformation(tableEnv.asInstanceOf[BatchTableEnvironment].streamEnv).getMinResources
  }
}
