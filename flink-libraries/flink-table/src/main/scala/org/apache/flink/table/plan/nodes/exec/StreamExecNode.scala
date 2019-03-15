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
package org.apache.flink.table.plan.nodes.exec

import org.apache.flink.streaming.api.transformations._
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.util.StreamExecUtil

trait StreamExecNode[T] extends ExecNode[StreamTableEnvironment, T] {

  /**
    * Describes the state digest of the ExecNode which is used to set the
    * user-specified ID of the translated stream operator. The user-specified ID
    * is used to assign the same operator ID across job restarts.
    * It is important for the mapping of operator state to the operator.
    *
    * Note: Be careful to modify this method as it will affect the state reuse.
    */
  def getStateDigest(pw: ExecNodeWriter): ExecNodeWriter

  override def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[T] = {
    val transformation = super.translateToPlan(tableEnv)

    // set the uid if the translated stream operator has state
    this match {
      case source: StreamExecTableSourceScan =>
        StreamExecUtil.setUid(
          tableEnv.getConfig, getSourceTransformation(transformation), source)
        transformation

      case _ =>
        StreamExecUtil.setUid(tableEnv.getConfig, transformation, this)
          .asInstanceOf[StreamTransformation[T]]
    }
  }

  private def getSourceTransformation(
      transformation: StreamTransformation[_]): StreamTransformation[_] = {
    transformation match {
      case oneInputTransformation: OneInputTransformation[_, _] =>
        getSourceTransformation(oneInputTransformation.getInput)
      case partitionTransformation: PartitionTransformation[_] =>
        getSourceTransformation(partitionTransformation.getInput)
      case _: SourceTransformation[_] | _: SourceV2Transformation[_] => transformation
    }
  }
}
