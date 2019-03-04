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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.expressions.ExpressionUtils.{isRowtimeAttribute, isTimeIntervalLiteral}
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.plan.`trait`.{MiniBatchInterval, MiniBatchIntervalTrait, MiniBatchIntervalTraitDef, MiniBatchMode}
import org.apache.flink.table.plan.logical.{SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecGroupWindowAggregate, StreamExecTableSourceScan, StreamExecWatermarkAssigner, StreamPhysicalRel}
import org.apache.flink.table.plan.schema.IntermediateDataStreamTable
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, MiniBatchIntervalInferUtil}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptRule.{operand, _}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.commons.math3.util.ArithmeticUtils

import scala.collection.JavaConverters._

/**
  * rule that infer the minibatch interval of watermark assigner.
  */
class MiniBatchIntervalInferRule extends RelOptRule(
  operand(classOf[StreamPhysicalRel], any()),
  "MiniBatchIntervalInferRule") {

  /**
    * Get all children RelNodes of a RelNode.
    *
    * @param parent The parent RelNode
    * @return All child nodes
    */
  def getChildRelNodes(parent: RelNode): Seq[RelNode] = {
    parent.getInputs.asScala.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rel = call.rel(0).asInstanceOf[StreamPhysicalRel]
    val minibatchIntervalTrait = rel.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
    val children = getChildRelNodes(rel)
    val config = FlinkRelOptUtil.getTableConfig(rel)
    val miniBatchWindowEnabled = config.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_MINI_BATCH_WINDOW_ENABLED)
    val minibatchEnabled = config.getConf.contains(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
    val minibatchLatency = config.getConf.getLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)

    val updatedTrait = rel match {
      case w: StreamExecGroupWindowAggregate =>
        if (!miniBatchWindowEnabled) {
          MiniBatchIntervalTrait.NONE
        } else {
          w.window match {
            case TumblingGroupWindow(_, timeField, size)
              if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
              var emitInterval = size.asInstanceOf[Literal].value.asInstanceOf[Long]
              // consider early fire interval if defined.
              if (w.emitStrategy.earlyFireInterval > 0) {
                emitInterval = ArithmeticUtils.gcd(emitInterval, w.emitStrategy.earlyFireInterval)
              }
              // set to 0L: only consider the size of window closest to the source
              new MiniBatchIntervalTrait(MiniBatchInterval(emitInterval, MiniBatchMode.RowTime))

            case SlidingGroupWindow(_, timeField, size, slide)
              if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) =>
              // paneSize of sliding window.
              var emitInterval = ArithmeticUtils.gcd(
                size.asInstanceOf[Literal].value.asInstanceOf[Long],
                slide.asInstanceOf[Literal].value.asInstanceOf[Long])
              // consider early fire interval if defined.
              if (w.emitStrategy.earlyFireInterval > 0) {
                emitInterval = ArithmeticUtils.gcd(emitInterval, w.emitStrategy.earlyFireInterval)
              }
              new MiniBatchIntervalTrait(MiniBatchInterval(emitInterval, MiniBatchMode.RowTime))

            // TODO: session window do not support EMIT strategy currently.
            case _ => minibatchIntervalTrait
          }
        }

      case _: StreamExecWatermarkAssigner => MiniBatchIntervalTrait.NONE

      case _ => if (rel.requireWatermark && minibatchEnabled) {
        new MiniBatchIntervalTrait(MiniBatchInterval(
          minibatchIntervalTrait.getMiniBatchInterval.interval, MiniBatchMode.RowTime))
      } else if (minibatchIntervalTrait.getMiniBatchInterval.interval == 0 && minibatchEnabled) {
        // set proctime minibatch interval to the root node.
        Preconditions.checkArgument(minibatchLatency > 0,
          "MiniBatch latency must be greater than 0.", null)
        new MiniBatchIntervalTrait(MiniBatchInterval(minibatchLatency, MiniBatchMode.ProcTime))
      } else {
        minibatchIntervalTrait
      }
    }

    // propagate parent's MiniBatchInterval to children.
    val updatedChildren = children.map( c => {
      val originTrait = c.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
      val newChild = if (originTrait != updatedTrait) {
        /**
          * calc new MiniBatchIntervalTrait according parent's minibatchInterval
          * and the child's original minibatchInterval.
          */
        val inferredTrait = new MiniBatchIntervalTrait(
          MiniBatchIntervalInferUtil.mergedMiniBatchInterval(
          originTrait.getMiniBatchInterval, updatedTrait.getMiniBatchInterval))
        c.copy(c.getTraitSet.plus(inferredTrait), c.getInputs)
      } else {
        c
      }
      // add minibatch watermark assigner node.
      if (isTableSourceScan(newChild) &&
        newChild.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
          .getMiniBatchInterval.mode == MiniBatchMode.ProcTime) {
        StreamExecWatermarkAssigner.createIngestionTimeWatermarkAssigner(
          newChild.getCluster,
          newChild.getTraitSet,
          newChild.copy(newChild.getTraitSet.plus(MiniBatchIntervalTrait.NONE),
            newChild.getInputs))
      } else {
        newChild
      }
    })
    // update parent if a child was updated
    if (children != updatedChildren) {
      call.transformTo(rel.copy(rel.getTraitSet, updatedChildren.asJava))
    }
  }

  private def isTableSourceScan(node: RelNode): Boolean = node match {
    case scan: StreamExecDataStreamScan =>
      // scan is not an intermediate datastream
      !scan.dataStreamTable.isInstanceOf[IntermediateDataStreamTable[_]]
    case _: StreamExecTableSourceScan => true
    case _ => false
  }
}

object MiniBatchIntervalInferRule {
  val INSTANCE: RelOptRule = new MiniBatchIntervalInferRule
}
