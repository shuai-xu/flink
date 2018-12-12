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

import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.util.BatchExecRelVisitor

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.lang.{Boolean => JBoolean, Integer => JInteger}

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with StreamTransformation.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class BatchExecBoundedStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowRelDataType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with BatchExecScan {

  val boundedStreamTable: DataStreamTable[Any] = getTable.unwrap(classOf[DataStreamTable[Any]])

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def deriveRowType(): RelDataType = rowRelDataType

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecBoundedStreamScan(cluster, traitSet, getTable, getRowType)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val batchTransform = boundedStreamTable.dataStream.getTransformation
    assignSourceResourceAndParallelism(tableEnv, batchTransform)
    convertToInternalRow(
      tableEnv,
      batchTransform,
      boundedStreamTable.fieldIndexes,
      getRowType,
      boundedStreamTable.dataType,
      config,
      None)
  }

  override private[flink] def getTableSourceResultPartitionNum(
      tableEnv: TableEnvironment): JTuple[JBoolean, JInteger] = {
    new JTuple(boundedStreamTable.dataStream.getTransformation.isParallelismLocked,
      boundedStreamTable.dataStream.getParallelism)
  }

  override private[flink] def getTableSourceResource(
    tableEnv: TableEnvironment): ResourceSpec = {
    boundedStreamTable.dataStream.getTransformation.getMinResources
  }

  override def needInternalConversion: Boolean = {
    hasTimeAttributeField(boundedStreamTable.fieldIndexes) ||
        needsConversion(boundedStreamTable.dataType)
  }
}
