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

import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

trait StreamExecRel[T] extends FlinkPhysicalRel with StreamExecNode[T] {

  /**
    * Whether the [[FlinkRelNode]] produces update and delete changes.
    */
  def producesUpdates: Boolean = false

  /**
    * Whether the [[FlinkRelNode]] requires retraction messages or not.
    */
  def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  /**
    * Whether the [[FlinkRelNode]] consumes retraction messages instead of forwarding them.
    * The node might or might not produce new retraction messages.
    */
  def consumesRetractions: Boolean = false

  /**
    * Whether the [[FlinkRelNode]] produces retraction messages.
    */
  def producesRetractions: Boolean = false

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[StreamExecRel[T]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    require(ordinalInParent >= 0 && ordinalInParent < getInputs.size())
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[StreamExecRel[T]])
  }

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this
}

trait RowStreamExecRel extends StreamExecRel[BaseRow]
