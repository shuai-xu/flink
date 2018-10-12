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

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexInputRef
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCoTableValuedAggregate
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecCoGroupTableValuedAggregate}

import scala.collection.JavaConversions._

class StreamExecCoGroupTableValuedAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalCoTableValuedAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecCoGroupTableValuedAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val connect = rel.asInstanceOf[FlinkLogicalCoTableValuedAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.STREAMEXEC)

    val leftKeyIndex =
      connect.groupKey1.map(e => Integer.valueOf(e.asInstanceOf[RexInputRef].getIndex))
    val rightKeyIndex = connect.groupKey2.map(e =>
      Integer.valueOf(
        e.asInstanceOf[RexInputRef].getIndex - connect.getLeft.getRowType.getFieldCount
      )
    )
    if (leftKeyIndex.size != rightKeyIndex.size) {
      throw new TableException(
        "Left keySet size of coAggregate should equal to right keySet size.")
    }

    val leftRequiredTraitSet = connect.getLeft.getTraitSet
      .replace(FlinkConventions.STREAMEXEC)
      .replace(getDistribution(leftKeyIndex))

    val rightRequiredTraitSet = connect.getRight.getTraitSet
      .replace(FlinkConventions.STREAMEXEC)
      .replace(getDistribution(rightKeyIndex))

    val newLeft = RelOptRule.convert(connect.getLeft, leftRequiredTraitSet)
    val newRight = RelOptRule.convert(connect.getRight, rightRequiredTraitSet)
    new StreamExecCoGroupTableValuedAggregate(
      connect.getCluster,
      traitSet,
      newLeft,
      newRight,
      connect.lRexCall,
      connect.rRexCall,
      connect.groupKey1,
      connect.groupKey2
    )
  }

  def getDistribution(keyIndex: Seq[Integer]): FlinkRelDistribution = {
    if (keyIndex.size == 0) {
      FlinkRelDistribution.SINGLETON
    } else {
      FlinkRelDistribution.hash(keyIndex)
    }
  }
}

object StreamExecCoGroupTableValuedAggregateRule {
  val INSTANCE: ConverterRule = new StreamExecCoGroupTableValuedAggregateRule
}
