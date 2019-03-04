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

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{MiniBatchIntervalTraitDef, MiniBatchMode}
import org.apache.flink.table.plan.nodes.calcite.WatermarkAssigner
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.runtime.WatermarkAssignerOperator
import org.apache.flink.table.runtime.bundle.{MiniBatchAssignerOperator, MiniBatchedWatermarkAssignerOperator}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util
import java.util.Calendar

class StreamExecWatermarkAssigner (
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputNode: RelNode,
    rowtimeField: String,
    watermarkOffset: Long)
  extends WatermarkAssigner(cluster, traits, inputNode, rowtimeField, watermarkOffset)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def copy(
    traitSet: RelTraitSet,
    inputs: util.List[RelNode]): RelNode = {
    new StreamExecWatermarkAssigner(cluster, traitSet, inputs.get(0), rowtimeField, watermarkOffset)
  }

  override def isDeterministic: Boolean = true

  override def explainTerms(pw: RelWriter): RelWriter = {
    val tableConfig = cluster.getPlanner.getContext.unwrap(classOf[TableConfig])
    val mbInterval = getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval

    if (mbInterval.mode == MiniBatchMode.None) {
      // 1. operator requiring watermark, but minibatch is not enabled
      // 2. redundant watermark definition in DDL
      // 3. existing window, and window minibatch is disabled.
      super.explainTerms(pw).item("miniBatchInterval", "None")
    } else if (mbInterval.mode == MiniBatchMode.ProcTime) {
      val minibatchLatency = tableConfig.getConf.getLong(
        TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
      Preconditions.checkArgument(minibatchLatency > 0,
        "MiniBatch latency must be greater that 0.", null)
      super.explainTerms(pw).item("miniBatchInterval",
        "Proctime, " + minibatchLatency + "ms")
    } else {
      super.explainTerms(pw).item("miniBatchInterval",
        "Rowtime, " + mbInterval.interval + "ms")
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Three implementations of watermark assigner:
    * 1. MiniBatchAssignerOperator: only unbounded group agg with Minibatch enabled.
    * 2. WatermarkAssignerOperator: watermark is required (window...) with Minibatch disabled.
    * 3. MiniBatchedWatermarkAssignerOperator: watermark is required with Minibatch enabled.
    */
  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val inferredInterval = getTraitSet.getTrait(
      MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval

    val (operator, opName) = if (inferredInterval.mode == MiniBatchMode.None) {
      // 1. operator requiring watermark, but minibatch is not enabled
      // 2. redundant watermark definition in DDL
      // 3. existing window, and window minibatch is disabled.
      val rowtimeIndex = getRowType.getFieldNames.indexOf(rowtimeField)
      val op = new WatermarkAssignerOperator(rowtimeIndex, watermarkOffset)
      val opName = s"WatermarkAssigner(rowtime: $rowtimeField, offset: $watermarkOffset)"
      (op, opName)
    } else if (inferredInterval.mode == MiniBatchMode.ProcTime) {
      val op = new MiniBatchAssignerOperator(inferredInterval.interval)
      val opName = s"MiniBatchAssigner(intervalMs: ${inferredInterval.interval})"
      (op, opName)
    } else {
      val rowtimeIndex = getRowType.getFieldNames.indexOf(rowtimeField)
      // get the timezone offset.
      val tzOffset = tableEnv.getConfig.getTimeZone.getOffset(Calendar.ZONE_OFFSET)
      val op = new MiniBatchedWatermarkAssignerOperator(
        rowtimeIndex, watermarkOffset, tzOffset, inferredInterval.interval)
      val opName = s"MiniBatchedWatermarkAssigner(rowtime: $rowtimeField," +
        s" offset: $watermarkOffset, intervalMs: ${inferredInterval.interval})"
      (op, opName)
    }

    val transformation = new OneInputTransformation[BaseRow, BaseRow](
      inputTransformation,
      opName,
      operator,
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType),
      inputTransformation.getParallelism)

    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }
}

object StreamExecWatermarkAssigner {
  def createRowTimeWatermarkAssigner(
      cluster: RelOptCluster,
      traits: RelTraitSet,
      inputNode: RelNode,
      rowtimeField: String,
      watermarkOffset: Long): StreamExecWatermarkAssigner = {
    new StreamExecWatermarkAssigner(
      cluster: RelOptCluster,
      traits: RelTraitSet,
      inputNode: RelNode,
      rowtimeField: String,
      watermarkOffset: Long)
  }

  def createIngestionTimeWatermarkAssigner(
      cluster: RelOptCluster,
      traits: RelTraitSet,
      inputNode: RelNode): StreamExecWatermarkAssigner = {
    new StreamExecWatermarkAssigner(
      cluster: RelOptCluster,
      traits: RelTraitSet,
      inputNode: RelNode,
      null,
      0)
  }
}
