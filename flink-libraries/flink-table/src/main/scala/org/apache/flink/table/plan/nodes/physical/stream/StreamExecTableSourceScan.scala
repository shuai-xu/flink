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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.{StreamTableSource, TableSourceUtil}
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.typeutils.TypeUtils

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with StreamScan {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecTableSourceScan(
      cluster,
      traitSet,
      relOptTable
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): PhysicalTableSourceScan = {
    new StreamExecTableSourceScan(
      cluster,
      traitSet,
      relOptTable
    )
  }

  override def translateToPlan(
    tableEnv: StreamTableEnvironment,
    queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig
    val inputDataStream = tableSource
      .asInstanceOf[StreamTableSource[_]]
      .getDataStream(tableEnv.execEnv)
      .asInstanceOf[DataStream[Any]]
    val fieldIdxs = TableSourceUtil.computeIndexMapping(tableSource, true, None)

    // check that declared and actual type of table source DataStream are identical
    if (! DataTypes.internal(tableSource.getReturnType).equals(
        DataTypes.internal(inputDataStream.getType))) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataStream of type ${inputDataStream.getType} that does not match with the " +
        s"type ${tableSource.getReturnType} declared by the TableSource.getReturnType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      None,
      cluster,
      tableEnv.getRelBuilder,
      DataTypes.TIMESTAMP
    )

    convertToInternalRow(
      inputDataStream.getTransformation,
      fieldIdxs,
      getRowType,
      tableSource.getReturnType,
      config,
      rowtimeExpression)
  }
}
