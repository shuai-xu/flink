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
package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{AggPhaseEnforcer, TableConfigOptions}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecGlobalGroupAggregate, StreamExecGroupAggregate, StreamExecJoin, StreamExecLocalGroupAggregate}
import org.apache.flink.table.runtime.utils.StreamingWithAggTestBase.{AggMode, LocalGlobalOff}
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Pair
import org.apache.commons.lang3.SystemUtils

import java.io.{PrintWriter, StringWriter}
import java.util.{List => JList}

import org.junit.Before

abstract class StreamPlanTestBase(
    aggMode: AggMode = LocalGlobalOff,
    miniBatch: MiniBatchMode = MiniBatchOff) extends TableTestBase {
  protected val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, Long, Long)]("A", 'a1, 'a2, 'a3)
  streamUtil.addTable[(Int, Long, Long)]("B", 'b1, 'b2, 'b3)

  @Before
  def before(): Unit = {
    val tableConfig = streamUtil.tableEnv.getConfig
    miniBatch match {
      case MiniBatchOn =>
        tableConfig.getConf.setBoolean(
          TableConfigOptions.SQL_EXEC_MINI_BATCH_WINDOW_ENABLED, true)
        tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, true)
        tableConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
        tableConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE, 3L)
      case MiniBatchOff =>
        tableConfig.getConf.setBoolean(
          TableConfigOptions.SQL_EXEC_MINI_BATCH_WINDOW_ENABLED, false)
        tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, false)
        tableConfig.getConf.remove(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
    }

    streamUtil.tableEnv.getConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    if (aggMode.isLocalAggEnabled) {
      streamUtil.tableEnv.getConfig.getConf.setString(
        TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, AggPhaseEnforcer.TWO_PHASE.toString)
    } else {
      streamUtil.tableEnv.getConfig.getConf.setString(
        TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, AggPhaseEnforcer.ONE_PHASE.toString)
    }
  }

  def verifyPlanAndTrait(sql: String): Unit = {
    val table = streamUtil.tableEnv.sqlQuery(sql)
    val relNode = table.getRelNode
    val optimized = streamUtil.tableEnv.optimize(relNode)
    val sw = new StringWriter
    val planWriter = new JoinRelWriter(new PrintWriter(sw))
    optimized.explain(planWriter)
    val plan = SystemUtils.LINE_SEPARATOR + sw.toString
    streamUtil.verifyPlan(this.name.getMethodName, plan)
  }
}

class JoinRelWriter(pw: PrintWriter) extends RelWriterImpl(pw,
  SqlExplainLevel.EXPPLAN_ATTRIBUTES, false) {
  override def explain_(rel: RelNode, values: JList[Pair[String, AnyRef]]): Unit = {
    val pairs = rel match {
      case join: StreamExecJoin => join.explainJoin
      case agg: StreamExecGroupAggregate => agg.explainAgg
      case localAgg: StreamExecLocalGroupAggregate => localAgg.explainAgg
      case globalAgg: StreamExecGlobalGroupAggregate => globalAgg.explainAgg
      case _ => values
    }
    super.explain_(rel, pairs)
  }
}
