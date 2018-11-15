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

package com.alibaba.blink.table.plan.rules.datastream

import java.util

import com.alibaba.blink.table.plan.rules.utils.HTableRelUtil._
import com.alibaba.blink.table.sources.HBaseDimensionTableSource
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.nodes.physical.stream.StreamTableJoinHTable
import org.apache.flink.table.plan.schema.BaseRowSchema

import scala.collection.JavaConverters._

class StreamExecJoinHTableSourceRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAMEXEC,
    "StreamExecJoinHTableSourceRule"){

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val joinInfo = join.analyzeCondition

    def findAimNode(rel: RelNode): Boolean = {
      rel match {
          case rel: RelSubset =>
            findAimNode(rel.getRelList.get(0))
          // do not search sub join node
          case rel: FlinkLogicalCalc =>
            if (hasSelectionOnly(rel) && (rel.getInput(0)
              .isInstanceOf[FlinkLogicalTableSourceScan] ||
              rel.getInput(0).isInstanceOf[RelSubset])) {
              findAimNode(rel.getInput(0))
            } else { // do not support calc on hbase table source
              false
            }

          case rel: FlinkLogicalTableSourceScan => hasHBaseTableSource(rel)

          case _ =>
            false
      }
    }
    // this join require exactly one equi-join condition and
    // currently not support any non-equi-join conditions
    // and only the rowkey field of a HBaseTableSource can be join key
    (join.getJoinType == JoinRelType.INNER || join.getJoinType == JoinRelType.LEFT) &&
      !joinInfo.pairs().isEmpty && joinInfo.pairs().size() == 1 &&
      joinInfo.pairs().get(0).target == 0 && joinInfo.isEqui && findAimNode(join.getRight)
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[FlinkLogicalJoin]
    val joinInfo = join.analyzeCondition
    val hTableSource: HBaseDimensionTableSource[_] = extractHTableSource(join.getRight)
    assert(hTableSource != null)

    val outputFieldsSet = join.getRowType.getFieldList.asScala.map(_.getName).toSet
    if (!outputFieldsSet.contains(hTableSource.getKeyName)) {
      hTableSource.setSelectKey(false)
    }

    var requiredTraitSet = join.getInput(0).getTraitSet.replace(
      FlinkConventions.STREAMEXEC)

    // if partitioning enabled, use the join key as partition key
    if (hTableSource.partitioning) {
      val pair = joinInfo.pairs.get(0)
      // HBaseTable's rowKey index always be zero
      val leftKeyIdx = pair.source
      requiredTraitSet = requiredTraitSet.plus(FlinkRelDistribution.hash(
        new util.ArrayList(leftKeyIdx)))
    }

    val convLeft = RelOptRule.convert(join.getInput(0), requiredTraitSet)
    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAMEXEC)
    new StreamTableJoinHTable(
      join.getCluster,
      providedTraitSet,
      convLeft,
      hTableSource,
      new BaseRowSchema(join.getRowType),
      join.getCondition,
      join.getRowType,
      joinInfo,
      joinInfo.pairs,
      join.getJoinType,
      null,
      description)
  }
}

object StreamExecJoinHTableSourceRule {
  val INSTANCE: RelOptRule = new StreamExecJoinHTableSourceRule
}
