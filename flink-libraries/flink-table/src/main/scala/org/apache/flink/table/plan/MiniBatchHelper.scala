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

package org.apache.flink.table.plan

import org.apache.flink.streaming.api.bundle.{BundleTrigger, CombinedBundleTrigger, CountBundleTrigger, TimeBundleTrigger}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations._

import scala.collection.mutable
import _root_.scala.collection.JavaConverters._
import _root_.java.util.{List => JList}

import org.apache.flink.streaming.api.operators.StreamOperator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.operator.bundle.{BundleOperator, KeyedBundleOperator}
import org.apache.flink.util.Preconditions

object MiniBatchHelper {

  private def assignTime(op: StreamOperator[_], newTime: Long): Unit = {
    Preconditions.checkArgument(
      op.isInstanceOf[KeyedBundleOperator[_, _, _, _]] ||
        op.isInstanceOf[BundleOperator[_, _, _, _]])

    val triggerField = op.getClass.getDeclaredField("bundleTrigger")
    triggerField.setAccessible(true)
    val trigger: BundleTrigger[_] = triggerField.get(op).asInstanceOf[BundleTrigger[_]]
    if (newTime > 0L) {
      if (trigger.isInstanceOf[CombinedBundleTrigger[_]]) {
        val triggersField = trigger.getClass.getDeclaredField("triggers")
        triggersField.setAccessible(true)
        val triggers = triggersField.get(trigger).asInstanceOf[Array[BundleTrigger[_]]]
        triggers.zipWithIndex.foreach(
          t => {
            if (t._1.isInstanceOf[TimeBundleTrigger[_]]) {
              val timeTrigger = t._1.asInstanceOf[TimeBundleTrigger[_]]
              val timeoutField = timeTrigger.getClass.getDeclaredField("timeout")
              timeoutField.setAccessible(true)
              timeoutField.set(timeTrigger, newTime)
            }
          }
        )
      }
    } else {
      val countTrigger = new CountBundleTrigger[BaseRow](1)
      val bundleTrigger = new CombinedBundleTrigger[BaseRow](countTrigger)
      triggerField.set(op, bundleTrigger)
    }

  }

  private def isTargetOp(transformation: StreamTransformation[_]): Boolean =
    transformation.isInstanceOf[OneInputTransformation[_, _]] &&
      (transformation.asInstanceOf[OneInputTransformation[_, _]].getOperator
        .isInstanceOf[KeyedBundleOperator[_, _, _, _]] ||
        transformation.asInstanceOf[OneInputTransformation[_, _]].getOperator
          .isInstanceOf[BundleOperator[_, _, _, _]])

  def visit(
    transformation: StreamTransformation[_],
    visitedMap: mutable.Map[StreamTransformation[_], Long],
    level: Int,
    timeAmount: Long): Long = {

    var children: List[StreamTransformation[_]] = transformation match {
      case one: OneInputTransformation[_, _] => List(one.getInput)
      case two: TwoInputTransformation[_, _, _] => List(two.getInput1, two.getInput2)
      case union: UnionTransformation[_] => union.getInputs.asScala.toList
      case sink: SinkTransformation[_] => List(sink.getInput)
      case _: SourceTransformation[_] => Nil
      case split: SplitTransformation[_] => List(split.getInput)
      case select: SelectTransformation[_] => List(select.getInput)
      case partition: PartitionTransformation[_] => List(partition.getInput)
      case _ => Nil
    }

    var isAssigned: Boolean = false
    var assignedTime: Long = 0
    var allTime: Long = 0
    val isThisTargetOp = isTargetOp(transformation)
    val found = children.find(visitedMap.contains(_))
    if (found.isDefined) {
      children = found.get :: children.filter(!_.eq(found.get))
    }
    for(child <- children) {
      var sum: Long = 0
      if (!visitedMap.contains(child)) {
        visitedMap += child -> 0
        val isChildTargetOp = isTargetOp(child)
        val newLevel = if (isChildTargetOp) level + 1 else level
        sum = if (!isAssigned) {
          visit(child, visitedMap, newLevel, timeAmount)
        } else {
          visit(child, visitedMap, 1, timeAmount - assignedTime)
        }
        visitedMap += child -> sum
      } else {
        // already found via another path
        sum = visitedMap(child)
      }
      if (isThisTargetOp && !isAssigned && level > 0 && sum > 0) {
        isAssigned = true
        assignedTime = (timeAmount - sum) / level
        allTime = sum + assignedTime
        assignTime(transformation
          .asInstanceOf[OneInputTransformation[_, _]]
          .getOperator, assignedTime)
      } else {
        if (allTime == 0) {
          allTime = sum
        }
      }
    }
    if (isThisTargetOp && !isAssigned && level > 0) {
      isAssigned = true
      assignedTime = (timeAmount - allTime) / level
      allTime += assignedTime
      assignTime(transformation.asInstanceOf[OneInputTransformation[_, _]].getOperator,
        assignedTime)
    }
    allTime
  }

  def assignTriggerTimeEqually(execEnv: StreamExecutionEnvironment, timeAmount: Long): Unit = {

    val transField = classOf[StreamExecutionEnvironment].getDeclaredField("transformations")
    transField.setAccessible(true)
    val transformations: JList[StreamTransformation[_]] =
      transField.get(execEnv).asInstanceOf[JList[StreamTransformation[_]]]
    val sinks: List[StreamTransformation[_]] =
      transformations.asScala.filter(_.isInstanceOf[SinkTransformation[_]]).toList

    val visitedMap = mutable.Map[StreamTransformation[_], Long]()
    sinks.foreach(s => {
      visitedMap += s -> 0
      visit(s, visitedMap, 0, timeAmount)
    })

  }
}

