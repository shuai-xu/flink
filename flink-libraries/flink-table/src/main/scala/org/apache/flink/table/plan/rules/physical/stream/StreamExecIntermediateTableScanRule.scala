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

import org.apache.flink.table.plan.nodes.logical.FlinkLogicalIntermediateTableScan
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecIntermediateTableScan

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions

class StreamExecIntermediateTableScanRule
  extends ConverterRule(
    classOf[FlinkLogicalIntermediateTableScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecIntermediateTableScanRule") {

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalIntermediateTableScan]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.STREAMEXEC)
    new StreamExecIntermediateTableScan(rel.getCluster, newTrait, scan.getTable, rel.getRowType)
  }
}

object StreamExecIntermediateTableScanRule {
  val INSTANCE: RelOptRule = new StreamExecIntermediateTableScanRule
}
