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

import org.apache.calcite.rel._
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdUniqueColumnsTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[IllegalArgumentException])
  def testGetUniqueColumnsOnTableScanWithNullColumns(): Unit = {
    val ts1 = relBuilder.scan("t1").build()
    mq.getUniqueColumns(ts1, null)
  }

  @Test
  def testGetUniqueColumnsOnTableScan(): Unit = {
    val ts1 = relBuilder.scan("t1").build()
    val ts2 = relBuilder.scan("t2").build()
    val ts3 = relBuilder.scan("t3").build()
    val ts7 = relBuilder.scan("t7").build()

    val empty = ImmutableBitSet.of()
    assertEquals(empty, mq.getUniqueColumns(ts1, empty))
    assertEquals(empty, mq.getUniqueColumns(ts2, empty))
    assertEquals(empty, mq.getUniqueColumns(ts3, empty))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts3, ImmutableBitSet.of(0)))

    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(ts2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts3, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts7, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts7, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(ts7, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueColumns(ts7, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testGetUniqueColumnsOnFilter(): Unit = {
    relBuilder.scan("t1")
    // filter: $0 <= 2 and $1 > 10
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr2 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10D))
    val filter1 = relBuilder.filter(List(expr1, expr2)).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(filter1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(filter1, ImmutableBitSet.of(0, 1)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // filter: $0 <= 2 and $1 > 10
    val expr3 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val expr4 = relBuilder.call(GREATER_THAN, relBuilder.field(1), relBuilder.literal(10D))
    val filter2 = relBuilder.filter(List(expr3, expr4)).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(filter2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(filter2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(filter2, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueColumns(filter2, ImmutableBitSet.of(1, 2)))
  }

  @Test
  def testGetUniqueColumnsOnProject(): Unit = {
    relBuilder.scan("t7")
    // project: $0, $1, $2
    val proj1 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.field(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj1, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj1, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj1, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueColumns(proj1, ImmutableBitSet.of(1, 2)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: $0 + $1, $1 * 2, 1
    val proj2 = relBuilder.project(List(
      relBuilder.call(PLUS, relBuilder.field(0), relBuilder.field(1)),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.literal(1))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj2, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(proj2, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(proj2, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(proj2, ImmutableBitSet.of(0, 1, 2)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: $0, $1 * 2, $2, 1, 2
    val proj3 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.field(2),
      relBuilder.literal(1),
      relBuilder.literal(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj3, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(proj3, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(proj3, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(proj3, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj3, ImmutableBitSet.of(0, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueColumns(proj3, ImmutableBitSet.of(3, 4)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: $0, $0 as a1, $2
    val proj4 = relBuilder.project(List(
      relBuilder.field(0),
      relBuilder.alias(relBuilder.field(0), "a1"),
      relBuilder.field(2))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj4, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj4, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj4, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(proj4, ImmutableBitSet.of(1, 2)))

    relBuilder.clear()
    relBuilder.scan("t7")
    // project: true, 1
    val proj5 = relBuilder.project(List(
      relBuilder.literal(true),
      relBuilder.literal(1))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj5, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(proj5, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(proj5, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueColumnsOnCalc(): Unit = {
    val ts = relBuilder.scan("t7").build()
    // project: $0, $1 * 2, $2, 1, 2  condition: a > 1
    relBuilder.push(ts)
    val projects = List(
      relBuilder.field(0),
      relBuilder.call(MULTIPLY, relBuilder.field(1), relBuilder.literal(2)),
      relBuilder.field(2),
      relBuilder.literal(1),
      relBuilder.literal(2))
    val condition = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    val outputRowType = relBuilder.push(ts).project(projects).build().getRowType
    val calc = buildCalc(ts, outputRowType, projects, List(condition))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(calc, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(calc, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(calc, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(calc, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(calc, ImmutableBitSet.of(0, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3), mq.getUniqueColumns(calc, ImmutableBitSet.of(3, 4)))
  }

  @Test
  def testGetUniqueColumnsOnUnion(): Unit = {
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(union, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(union, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(union, ImmutableBitSet.of(0, 1)))

    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(unionAll, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1), mq.getUniqueColumns(unionAll, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(unionAll, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueColumnsOnExchange(): Unit = {
    val ts = relBuilder.scan("t3").build()
    val exchange = LogicalExchange.create(ts, RelDistributions.SINGLETON)
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(exchange, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueColumnsOnSort(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(sort, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(limitBatchExec, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(sortBatchExec, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(sortLimitBatchExec, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueColumnsOnJoin(): Unit = {
    // inner join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(innerJoin, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueColumns(innerJoin, ImmutableBitSet.of(2, 3, 4)))
    assertEquals(ImmutableBitSet.of(3, 4), mq.getUniqueColumns(innerJoin, ImmutableBitSet.of(3, 4)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(innerJoin, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(innerJoin, ImmutableBitSet.of(0, 1, 3, 4, 5)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(innerJoinOnOneSideUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueColumns(innerJoinOnOneSideUniqueKeys, ImmutableBitSet.of(2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(innerJoinOnOneSideUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(innerJoinOnScore, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueColumns(innerJoinOnScore, ImmutableBitSet.of(2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(innerJoinOnScore, ImmutableBitSet.of(1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(innerJoinOnScore, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (no unique keys)
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(innerJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueColumns(innerJoinOnNoneUniqueKeys, ImmutableBitSet.of(2, 3)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(innerJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(innerJoinWithNonEquiCond, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 3, 4),
      mq.getUniqueColumns(innerJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 3, 4)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(innerJoinWithoutEquiCond, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueColumns(innerJoinWithoutEquiCond, ImmutableBitSet.of(2, 3, 4)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(innerJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // left outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(leftJoin, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(leftJoin, ImmutableBitSet.of(1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(leftJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(0, 4, 5),
      mq.getUniqueColumns(leftJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(leftJoinOnScore, ImmutableBitSet.of(1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(leftJoinOnScore, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (no unique keys)
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(leftJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(leftJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(0, 2, 3),
      mq.getUniqueColumns(leftJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3)))

    // right outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueColumns(rightJoin, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueColumns(rightJoin, ImmutableBitSet.of(1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(rightJoinOnRHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // left join keys are unique and right join keys are not unique
    assertEquals(ImmutableBitSet.of(4, 5),
      mq.getUniqueColumns(rightJoinOnLHSUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(rightJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueColumns(rightJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 2, 3)))

    // full outer join
    // both left join keys and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(fullJoin, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(fullJoin, ImmutableBitSet.of(1, 2, 3, 4)))

    // left join keys are not unique and right join keys are unique
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(fullJoinOnOneSideUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3, 4)))

    // neither left join keys nor right join keys are unique (non join columns have unique columns)
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(fullJoinOnScore, ImmutableBitSet.of(1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(fullJoinOnScore, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))

    // neither left join keys nor right join keys are unique (no unique keys)
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(fullJoinOnNoneUniqueKeys, ImmutableBitSet.of(0, 1, 2, 3)))

    // with non-equi join condition
    assertEquals(ImmutableBitSet.of(0, 3, 4),
      mq.getUniqueColumns(fullJoinWithNonEquiCond, ImmutableBitSet.of(0, 1, 3, 4)))

    // without equi join condition
    assertEquals(ImmutableBitSet.of(0, 2),
      mq.getUniqueColumns(fullJoinWithoutEquiCond, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
  }

  @Test
  def testGetUniqueColumnsOnSemiJoin(): Unit = {
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(semiJoin, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(antiJoin, ImmutableBitSet.of(0, 1)))
  }

  @Test
  def testGetUniqueColumnsOnAggregate(): Unit = {
    val agg1 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(relBuilder.field("score")),
      relBuilder.count(false, "c", relBuilder.field("id")),
      relBuilder.avg(false, "a", relBuilder.field("age"))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(agg1, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2), mq.getUniqueColumns(agg1, ImmutableBitSet.of(1, 2)))

    val agg2 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(relBuilder.field("id"), relBuilder.field("age")),
      relBuilder.avg(false, "s", relBuilder.field("score")),
      relBuilder.avg(false, "h", relBuilder.field("height"))).build()
    assertEquals(ImmutableBitSet.of(0), mq.getUniqueColumns(agg2, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 2, 3),
      mq.getUniqueColumns(agg2, ImmutableBitSet.of(0, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(agg2, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(2, 3), mq.getUniqueColumns(agg2, ImmutableBitSet.of(2, 3)))

    val agg3 = relBuilder.scan("student").aggregate(
      relBuilder.groupKey(),
      relBuilder.count(false, "c", relBuilder.field("id")),
      relBuilder.avg(false, "a", relBuilder.field("age"))).build()
    assertEquals(ImmutableBitSet.of(0, 1), mq.getUniqueColumns(agg3, ImmutableBitSet.of(0, 1)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(aggWithAuxGroup, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(aggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(aggWithAuxGroup, ImmutableBitSet.of(0, 2)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(aggWithAuxGroup, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(aggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
  }

  @Test
  def testGetUniqueColumnsOnLogicalOverWindow(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 4, 5),
      mq.getUniqueColumns(logicalOverWindow, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(logicalOverWindow, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueColumns(logicalOverWindow, ImmutableBitSet.of(0, 1, 2, 4)))
  }

  @Test
  def testGetUniqueColumnsOnBatchExecOverWindow(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 4, 5),
      mq.getUniqueColumns(overWindowAgg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(overWindowAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueColumns(overWindowAgg, ImmutableBitSet.of(0, 1, 2, 4)))
  }

  @Test
  def testGetUniqueColumnsOnValues(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
      mq.getUniqueColumns(values, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(1, 3, 6),
      mq.getUniqueColumns(values, ImmutableBitSet.of(1, 3, 6)))
  }

  @Test
  def testGetUniqueColumnsOnExpand(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(aggWithExpand, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(aggWithExpand, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(2), mq.getUniqueColumns(aggWithExpand, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueColumns(aggWithExpand, ImmutableBitSet.of(2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(aggWithExpand, ImmutableBitSet.of(1, 2)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0)))
    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(2),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2)))
    assertEquals(ImmutableBitSet.of(3),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(3)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(2, 3),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2, 3)))
    assertEquals(ImmutableBitSet.of(2, 3, 4),
      mq.getUniqueColumns(aggWithAuxGroupAndExpand, ImmutableBitSet.of(2, 3, 4)))
  }

  @Test
  def testGetUniqueColumnsOnFlinkLogicalWindowAggregate(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
      mq.getUniqueColumns(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(3, 4, 5, 6),
      mq.getUniqueColumns(flinkLogicalWindowAgg, ImmutableBitSet.of(3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 3, 4, 5, 6),
      mq.getUniqueColumns(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(flinkLogicalWindowAgg, ImmutableBitSet.of(0, 1, 2)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueColumns(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(flinkLogicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2, 3)))
  }

  @Test
  def testGetUniqueColumnsOnLogicalWindowAggregate(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6),
      mq.getUniqueColumns(logicalWindowAgg, ImmutableBitSet.of(0, 1, 2, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(3, 4, 5, 6),
      mq.getUniqueColumns(logicalWindowAgg, ImmutableBitSet.of(3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 3, 4, 5, 6),
      mq.getUniqueColumns(logicalWindowAgg, ImmutableBitSet.of(0, 3, 4, 5, 6)))
    assertEquals(ImmutableBitSet.of(0, 1),
      mq.getUniqueColumns(logicalWindowAgg, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(logicalWindowAgg, ImmutableBitSet.of(0, 1, 2)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueColumns(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(logicalWindowAggWithAuxGroup, ImmutableBitSet.of(0, 1, 2, 3)))
  }

  @Test
  def testGetUniqueColumnsOnBatchExecWindowAggregate(): Unit = {
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(localWindowAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(localWindowAgg, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(localWindowAgg, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 3),
      mq.getUniqueColumns(localWindowAgg, ImmutableBitSet.of(1, 3)))

    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(globalWindowAggWithLocalAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 3),
      mq.getUniqueColumns(globalWindowAggWithLocalAgg, ImmutableBitSet.of(1, 3)))

    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 2)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 3),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAgg, ImmutableBitSet.of(1, 3)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueColumns(localWindowAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(localWindowAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2),
      mq.getUniqueColumns(localWindowAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3),
      mq.getUniqueColumns(localWindowAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2, 3)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2), mq.getUniqueColumns(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3), mq.getUniqueColumns(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2, 3)))

    assertEquals(ImmutableBitSet.of(1),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(1)))
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1)))
    assertEquals(ImmutableBitSet.of(0, 1, 2), mq.getUniqueColumns(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2)))
    assertEquals(ImmutableBitSet.of(0, 1, 2, 3), mq.getUniqueColumns(
      globalWindowAggWithoutLocalAggWithAuxGrouping, ImmutableBitSet.of(0, 1, 2, 3)))
  }

  @Test
  def testGetUniqueColumnsOnRank(): Unit = {
    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(flinkLogicalRank, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(flinkLogicalRank, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueColumns(flinkLogicalRank, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueColumns(flinkLogicalRank, ImmutableBitSet.of(1, 2, 3, 4)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueColumns(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueColumns(flinkLogicalRowNumberWithOutput, ImmutableBitSet.of(1, 2, 3, 4)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(globalBatchExecRank, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(globalBatchExecRank, ImmutableBitSet.of(1, 2, 3)))
    assertEquals(ImmutableBitSet.of(0, 4),
      mq.getUniqueColumns(globalBatchExecRank, ImmutableBitSet.of(0, 1, 2, 3, 4)))
    assertEquals(ImmutableBitSet.of(1, 2, 3, 4),
      mq.getUniqueColumns(globalBatchExecRank, ImmutableBitSet.of(1, 2, 3, 4)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(localBatchExecRank, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(localBatchExecRank, ImmutableBitSet.of(1, 2, 3)))

    assertEquals(ImmutableBitSet.of(0),
      mq.getUniqueColumns(streamExecRowNumber, ImmutableBitSet.of(0, 1, 2, 3)))
    assertEquals(ImmutableBitSet.of(1, 2, 3),
      mq.getUniqueColumns(streamExecRowNumber, ImmutableBitSet.of(1, 2, 3)))
  }
}

