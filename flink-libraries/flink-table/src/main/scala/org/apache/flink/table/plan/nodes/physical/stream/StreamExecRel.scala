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

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel

import org.apache.calcite.rel.RelNode

trait StreamExecRel[T] extends FlinkPhysicalRel {

  private var reusedTransformation: Option[StreamTransformation[T]] = None

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv    The [[StreamTableEnvironment]] of the translated Table.
    * @return StreamTransformation
    */
  def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[T] = {
    reusedTransformation match {
      case Some(transformation) => transformation
      case _ =>
        val transformation = translateToPlanInternal(tableEnv)
        reusedTransformation = Some(transformation)
        transformation
    }
  }

  /**
    * Internal method, Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv    The [[StreamTableEnvironment]] of the translated Table.
    * @return StreamTransformation
    */
  protected def translateToPlanInternal(tableEnv: StreamTableEnvironment): StreamTransformation[T]

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
}

trait RowStreamExecRel extends StreamExecRel[BaseRow]
