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
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.WatermarkAssigner
import org.apache.flink.table.runtime.WatermarkAssignerOperator

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

class StreamExecWatermarkAssigner (
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputNode: RelNode,
    rowtimeField: String,
    watermarkOffset: Long)
  extends WatermarkAssigner(cluster, traits, inputNode, rowtimeField, watermarkOffset)
  with RowStreamExecRel {

  override def copy(
    traitSet: RelTraitSet,
    inputs: util.List[RelNode]): RelNode = {
    new StreamExecWatermarkAssigner(
      cluster, traitSet, inputs.get(0), rowtimeField, watermarkOffset)
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val in = input.asInstanceOf[RowStreamExecRel]
    val inputTransformation = in.translateToPlan(tableEnv)

    val rowtimeIndex = getRowType.getFieldNames.indexOf(rowtimeField)
    val watermarkOperator = new WatermarkAssignerOperator(rowtimeIndex, watermarkOffset)

    new OneInputTransformation[BaseRow, BaseRow](
        inputTransformation,
        s"WatermarkAssigner(rowtime: $rowtimeField, offset: $watermarkOffset)",
        watermarkOperator,
        FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BaseRow]),
        inputTransformation.getParallelism)
  }
}
