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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelFieldCollation, RelNode, RelWriter}
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

final class LogicalSegmentTop(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    groupKeys: ImmutableBitSet,
    fieldCollation: RelFieldCollation,
    withTies: ImmutableBitSet)
  extends SegmentTop(cluster, traits, input, groupKeys, fieldCollation, withTies) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new LogicalSegmentTop(cluster, traitSet, inputs.get(0), groupKeys,
      fieldCollation, withTies)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    def bitSetToString(bitSet: ImmutableBitSet): String = {
      bitSet.toList.map(idx => "$" + idx).mkString(", ")
    }

    val collation = s"$$${fieldCollation.getFieldIndex} ${fieldCollation.getDirection}"
    super.explainTerms(pw)
      .itemIf("groupBy", bitSetToString(groupKeys), groupKeys.cardinality > 0)
      .item("orderBy", s"$collation")
      .item("withTies", bitSetToString(withTies))
  }

}

object LogicalSegmentTop {

  def create(
      input: RelNode,
      groupKeys: ImmutableBitSet,
      fieldCollation: RelFieldCollation,
      withTies: ImmutableBitSet): LogicalSegmentTop = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalSegmentTop(input.getCluster, traits, input, groupKeys,
      fieldCollation, withTies)
  }
}
