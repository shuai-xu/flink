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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}
import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.streaming.api.transformations.{PartitionTransformation, StreamTransformation}
import org.apache.flink.streaming.runtime.partitioner._
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.nodes.common.CommonExchange
import org.apache.flink.table.plan.util.StreamExecUtil
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._

/**
  *
  * This RelNode represents a change of partitioning of the input elements.
  **/

class StreamExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends CommonExchange(cluster, traitSet, relNode, relDistribution)
  with StreamExecRel {

  private val DEFAULT_MAX_PARALLELISM = 1 << 7

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): StreamExecExchange = {
    new StreamExecExchange(cluster, traitSet, newInput, newDistribution)
  }

  override def translateToPlan(
    tableEnv: StreamTableEnvironment, queryConfig: StreamQueryConfig)
    : StreamTransformation[BaseRow] = {
    val input = getInput.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    val inputType = input.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
    val outputRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BaseRow])

    relDistribution.getType match {
      case RelDistribution.Type.SINGLETON =>
        val partitioner = new GlobalPartitioner[BaseRow]
        val transformation = new PartitionTransformation(
          input,
          partitioner.asInstanceOf[StreamPartitioner[BaseRow]],
          DataExchangeMode.PIPELINED)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // TODO Eliminate duplicate keys
        val selector = StreamExecUtil.getKeySelector(
          relDistribution.getKeys.map(_.toInt).toArray,
          inputType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        val partitioner = new KeyGroupStreamPartitioner(selector, DEFAULT_MAX_PARALLELISM)
        val transformation = new PartitionTransformation(
          input,
          partitioner.asInstanceOf[StreamPartitioner[BaseRow]],
          DataExchangeMode.PIPELINED)
        transformation.setOutputType(outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
        transformation
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }
}
