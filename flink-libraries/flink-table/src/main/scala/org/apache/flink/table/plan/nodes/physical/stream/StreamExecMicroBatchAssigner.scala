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
import org.apache.flink.table.runtime.operator.bundle.MicroBatchAssignerOperator

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

class StreamExecMicroBatchAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputNode: RelNode,
    intervalMs: Long)
  extends SingleRel(cluster, traits, inputNode)
  with RowStreamExecRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecMicroBatchAssigner(cluster, traitSet, inputs.get(0), intervalMs)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("interval", intervalMs + "ms")
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val in = input.asInstanceOf[RowStreamExecRel]
    val inputTransformation = in.translateToPlan(tableEnv)
    val intervalMs = tableEnv.getConfig.getMicroBatchTriggerTime

    new OneInputTransformation[BaseRow, BaseRow](
      inputTransformation,
      s"MicroBatchAssigner(intervalMs: $intervalMs)",
      new MicroBatchAssignerOperator(intervalMs),
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BaseRow]),
      inputTransformation.getParallelism)
  }
}
