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

package org.apache.flink.table.util


import java.math.BigDecimal

import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{AND, EQUALS, GREATER_THAN, LESS_THAN}
import org.apache.flink.table.plan.cost.FlinkRelMdHandlerTestBase
import org.apache.flink.table.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalRank, FlinkLogicalWindowAggregate}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecHashWindowAggregate, BatchExecLocalHashWindowAggregate, BatchExecWindowAggregateBase}
import org.apache.flink.table.util.FlinkRelMdUtil
import org.junit.Assert.assertEquals
import org.junit.Test

class FlinkRelMdUtilTest {

  @Test
  def testMakeNamePropertiesSelectivityRexNodeOnWinAgg(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder

    def doMakeNamePropertiesSelectivityRexNode(winAgg: SingleRel, pred: RexNode): RexNode = {
      winAgg match {
        case agg: LogicalWindowAggregate =>
          FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(agg, pred)
        case agg: FlinkLogicalWindowAggregate =>
          FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(agg, pred)
        case agg: BatchExecWindowAggregateBase =>
          FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(agg, pred)
        case _ => throw new IllegalArgumentException()
      }
    }

    def doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(winAgg: SingleRel): Unit = {
      relBuilder.clear()
      relBuilder.push(winAgg)

      val namePropertiesSelectivityNode = rexBuilder.makeCall(
        RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC,
        rexBuilder.makeApproxLiteral(new BigDecimal(0.5)))
      val pred = relBuilder.call(
        GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000))

      val newPred = doMakeNamePropertiesSelectivityRexNode(winAgg, pred)
      assertEquals(namePropertiesSelectivityNode.toString, newPred.toString)

      val pred1 = relBuilder.call(AND,
        relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
        relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
        relBuilder.call(GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000)))
      val newPred1 = doMakeNamePropertiesSelectivityRexNode(winAgg, pred1)
      assertEquals(relBuilder.call(AND,
        relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
        relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
        namePropertiesSelectivityNode).toString, newPred1.toString)
    }

    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getLogicalWindowAgg)
    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getFlinkLogicalWindowAgg)
    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getGlobalWindowAggWithLocalAgg)
    doTestMakeNamePropertiesSelectivityRexNodeOnWinAgg(wrapper.getGlobalWindowAggWithoutLocalAgg)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMakeNamePropertiesSelectivityRexNodeOnBatchExecLocalWinAgg(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder
    val winAggWithLocalAgg = wrapper.getLocalWindowAgg
    relBuilder.push(winAggWithLocalAgg)

    val pred = relBuilder.call(
      GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000))
    FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(winAggWithLocalAgg, pred)
  }

  @Test
  def testMakeNamePropertiesSelectivityRexNodeOnWinAggWithAuxGrouping(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder
    val winAgg = wrapper.getGlobalWindowAggWithLocalAggWithAuxGrouping
    relBuilder.push(winAgg)

    val namePropertiesSelectivityNode = rexBuilder.makeCall(
      RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC,
      rexBuilder.makeApproxLiteral(new BigDecimal(0.5)))
    val pred = relBuilder.call(
      GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000))

    val newPred = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(winAgg, pred)
    assertEquals(namePropertiesSelectivityNode.toString, newPred.toString)

    val pred1 = relBuilder.call(AND,
      relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
      relBuilder.call(GREATER_THAN, relBuilder.field(3), relBuilder.literal(1000000)))
    val newPred1 = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(winAgg, pred1)
    assertEquals(relBuilder.call(AND,
      relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(10)),
      relBuilder.call(EQUALS, relBuilder.field(2), relBuilder.literal(10)),
      namePropertiesSelectivityNode).toString, newPred1.toString)
  }

  @Test
  def testSplitPredicateOnRank(): Unit = {
    val wrapper = new RelMdHandlerTestWrapper()
    val relBuilder = wrapper.relBuilder
    val rexBuilder = relBuilder.getRexBuilder
    val rank = wrapper.getFlinkLogicalRank
    relBuilder.push(rank)

    // age > 23
    val pred1 = relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23))
    val (nonRankPred1, rankPred1) = FlinkRelMdUtil.splitPredicateOnRank(rank, pred1)
    assertEquals(pred1.toString, nonRankPred1.get.toString)
    assertEquals(None, rankPred1)

    // rk < 2
    val pred2 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2))
    val (nonRankPred2, rankPred2) = FlinkRelMdUtil.splitPredicateOnRank(rank, pred2)
    assertEquals(None, nonRankPred2)
    assertEquals(pred2.toString, rankPred2.get.toString)

    // age > 23 and rk < 2
    val pred3 = relBuilder.and(
        relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)),
        relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)))
    val (nonRankPred3, rankPred3) = FlinkRelMdUtil.splitPredicateOnRank(rank, pred3)
    assertEquals(
      relBuilder.call(GREATER_THAN, relBuilder.field(2), relBuilder.literal(23)).toString,
      nonRankPred3.get.toString)
    assertEquals(
      relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(2)).toString,
      rankPred3.get.toString)
  }

  private class RelMdHandlerTestWrapper extends FlinkRelMdHandlerTestBase {
    super.setUp()

    def getLogicalWindowAgg: LogicalWindowAggregate = logicalWindowAgg

    def getFlinkLogicalWindowAgg: FlinkLogicalWindowAggregate = flinkLogicalWindowAgg

    def getGlobalWindowAggWithLocalAgg: BatchExecHashWindowAggregate = globalWindowAggWithLocalAgg

    def getGlobalWindowAggWithoutLocalAgg: BatchExecHashWindowAggregate =
      globalWindowAggWithoutLocalAgg

    def getLocalWindowAgg: BatchExecLocalHashWindowAggregate = localWindowAgg

    def getGlobalWindowAggWithLocalAggWithAuxGrouping: BatchExecHashWindowAggregate =
      globalWindowAggWithoutLocalAggWithAuxGrouping

    def getGlobalWindowAggWithoutLocalAggWithAuxGrouping: BatchExecHashWindowAggregate =
      globalWindowAggWithoutLocalAggWithAuxGrouping

    def getFlinkLogicalRank: FlinkLogicalRank = flinkLogicalRank
  }

}
