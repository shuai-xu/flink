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

package org.apache.flink.table.plan.cost

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalExpand
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule.{buildExpandRowType, createExpandProjects}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdUniqueKeysTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetUniqueKeysOnTableScan(): Unit = {
    val ts = relBuilder.scan("student").build()
    val uniqueKeys = mq.getUniqueKeys(ts)
    assertTrue(uniqueKeys.size() == 1)
    assertEquals(uniqueKeys.head, ImmutableBitSet.of(0))

    val ts1 = relBuilder.scan("t1").build()
    val uniqueKeys1 = mq.getUniqueKeys(ts1)
    assertNull(uniqueKeys1)

    val ts2 = relBuilder.scan("t2").build()
    val uniqueKeys2 = mq.getUniqueKeys(ts2)
    assertTrue(uniqueKeys2.isEmpty)
  }

  @Test
  def testAreColumnsUniqueOnAgg(): Unit = {
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0)), mq.getUniqueKeys(aggWithAuxGroup))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 1)),
      mq.getUniqueKeys(aggWithAuxGroupAndExpand))
  }

  @Test
  def testGetUniqueKeysOnExpand(): Unit = {
    val ts = relBuilder.scan("student").build()
    val expandOutputType = buildExpandRowType(
      ts.getCluster.getTypeFactory, ts.getRowType, Array.empty[Integer])
    val expandProjects = createExpandProjects(
      ts.getCluster.getRexBuilder,
      ts.getRowType,
      expandOutputType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)
      ), Array.empty[Integer])
    val expand = new FlinkLogicalExpand(
      ts.getCluster, ts.getTraitSet, ts, expandOutputType, expandProjects, 4)
    val uniqueKeys = mq.getUniqueKeys(expand)
    assertTrue(uniqueKeys.size() == 1)
    assertEquals(uniqueKeys.head, ImmutableBitSet.of(0))
  }

  @Test
  def testGetUniqueKeysOnJoin(): Unit = {
    // left is t3, right student, join condition is t3.id = student.id
    val innerJoin = relBuilder.scan("t3").scan("student").join(JoinRelType.INNER,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertEquals(mq.getUniqueKeys(innerJoin).toSet,
      Set(ImmutableBitSet.of(0), ImmutableBitSet.of(2), ImmutableBitSet.of(0, 2)))
    val fullJoin = relBuilder.scan("t3").scan("student").join(JoinRelType.FULL,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertEquals(mq.getUniqueKeys(fullJoin).toSet,
      Set(ImmutableBitSet.of(0, 2)))
    val leftJoin = relBuilder.scan("t3").scan("student").join(JoinRelType.LEFT,
      relBuilder.call(EQUALS, relBuilder.field(2, 0, 0), relBuilder.field(2, 1, 0))).build
    assertEquals(mq.getUniqueKeys(leftJoin).toSet,
      Set(ImmutableBitSet.of(0), ImmutableBitSet.of(0, 2)))
  }

  @Test
  def testGetUniqueKeysOnProject(): Unit = {
    // PRJ ($0=1, $0, cast($0 AS Int), $1, 2.1, 2)
    val ts = relBuilder.scan("student").build()
    relBuilder.push(ts)
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.cast(relBuilder.field(0), INTEGER),
      relBuilder.field(1),
      relBuilder.getRexBuilder.makeLiteral(
        2.1D, relBuilder.getTypeFactory.createSqlType(DOUBLE), true),
      relBuilder.getRexBuilder.makeLiteral(
        2L, relBuilder.getTypeFactory.createSqlType(BIGINT), true))
    val prj = relBuilder.project(exprs).build()
    assertEquals(mq.getUniqueKeys(prj).toSet, Set(ImmutableBitSet.of(1)))
    assertEquals(
      mq.getUniqueKeys(prj, true).toSet, Set(ImmutableBitSet.of(1), ImmutableBitSet.of(2)))
  }

  @Test
  def testGetUniqueKeysOnWindowAggWithAuxGroup(): Unit = {
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
      ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
      mq.getUniqueKeys(logicalWindowAggWithAuxGroup))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
      ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
      mq.getUniqueKeys(flinkLogicalWindowAggWithAuxGroup))
    assertEquals(null, mq.getUniqueKeys(localWindowAggWithAuxGrouping))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
      ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
      mq.getUniqueKeys(globalWindowAggWithLocalAggWithAuxGrouping))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(0, 4),
      ImmutableBitSet.of(0, 5), ImmutableBitSet.of(0, 6)),
      mq.getUniqueKeys(globalWindowAggWithoutLocalAggWithAuxGrouping))
  }

  @Test
  def testGetUniqueKeysOnRank(): Unit = {
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0)), mq.getUniqueKeys(flinkLogicalRank))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0)),
      mq.getUniqueKeys(flinkLogicalRowNumber))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0), ImmutableBitSet.of(2, 4)),
      mq.getUniqueKeys(flinkLogicalRowNumberWithOutput))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0)), mq.getUniqueKeys(globalBatchExecRank))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0)), mq.getUniqueKeys(localBatchExecRank))
    assertEquals(ImmutableSet.of(ImmutableBitSet.of(0)), mq.getUniqueKeys(streamExecRowNumber))
  }

}
