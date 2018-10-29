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

package org.apache.flink.table.plan.nodes.physical

import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.resource.RelResource

/**
  * Base class for flink physical node.
  */
trait FlinkPhysicalRel extends FlinkRelNode {

  /**
    * Defines how much resource the rel will take
    */
  private[flink] var resource: RelResource = _

  /**
    * Defines how many partitions the data of the node will output
    */
  private[flink] var resultPartitionCount = -1

  /**
    * Sets rel resource.
    */
  def setResource(resource: RelResource): Unit = {
    this.resource = resource
  }

  def setResultPartitionCount(count: Int): Unit = {
    resultPartitionCount = count
  }
}
