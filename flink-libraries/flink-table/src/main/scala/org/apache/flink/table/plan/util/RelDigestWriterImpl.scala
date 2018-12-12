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

package org.apache.flink.table.plan.util

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCorrelate, BatchExecGroupAggregateBase, BatchExecOverAggregate}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Calc, Join}
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Pair

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

/**
  * Row type is part of the digest for the rare occasion that similar
  * expressions have different types, e.g.
  * "WITH
  * t1 AS (SELECT CAST(a as BIGINT) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as BIGINT)),
  * t2 AS (SELECT CAST(a as DOUBLE) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as DOUBLE))
  * SELECT t1.*, t2.* FROM t1, t2 WHERE t1.b = t2.b"
  *
  * the physical plan is:
  * {{{
  *  HashJoin(where=[=(b, b0)], join=[a, b, a0, b0], joinType=[InnerJoin],
  *    isBroadcast=[true], build=[right])
  *  :- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
  *  :  +- Exchange(distribution=[hash[a]])
  *  :     +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
  *  :        +- Calc(select=[CAST(a) AS a, b])
  *  :           +- ScanTable(table=[[x]], fields=[a, b, c])
  *  +- Exchange(distribution=[broadcast])
  *     +- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
  *        +- Exchange(distribution=[hash[a]])
  *           +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
  *              +- Calc(select=[CAST(a) AS a, b])
  *                 +- ScanTable(table=[[x]], fields=[a, b, c])
  * }}}
  *
  * The sub-plan of `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])`
  * are different because `CAST(a) AS a` has different types, where one is BIGINT type
  * and another is DOUBLE type.
  *
  * If use the result of `RelOptUtil.toString(aggregate, SqlExplainLevel.DIGEST_ATTRIBUTES)`
  * on `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])` as digest,
  * we will get incorrect result. So rewrite `explain_` method of `RelWriterImpl` to
  * add row-type to digest value.
  */
class RelDigestWriterImpl(sw: StringWriter, tableConfig: TableConfig)
  extends RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false) {

  override def explain_(rel: RelNode, values: util.List[Pair[String, AnyRef]]): Unit = {
    val inputs = rel.getInputs
    val mq = rel.getCluster.getMetadataQuery
    if (!mq.isVisibleInExplain(rel, getDetailLevel)) {
      // render children in place of this, at same level
      inputs.foreach(_.explain(this))
      return
    }

    val s = new StringBuilder
    s.append(rel.getRelTypeName)
    var j = 0
    s.append("(")
    values.foreach {
      case value if value.right.isInstanceOf[RelNode] => // do nothing
      case value =>
        if (j != 0) s.append(", ")
        j += 1
        s.append(value.left).append("=[").append(value.right).append("]")
    }
    if (j > 0) {
      s.append(",")
    }
    s.append("rowType=[").append(rel.getRowType.toString).append("]")
    // if the given rel contains non-deterministic `SqlOperator`,
    // add a unique id to distinguish each other
    if (!tableConfig.getNondeterministicOperatorReuse && findNonDeterministicOperator(rel)) {
      s.append(",nonDeterministicId=[")
        .append(RelDigestWriterImpl.nonDeterministicIdCounter.incrementAndGet()).append("]")
    }
    s.append(")")

    pw.println(s)
    inputs.foreach(_.explain(this))
  }

  /**
    * Return true if the given rel does not contain non-deterministic `SqlOperator`
    * (e.g. op in `RexCall`, op in `SqlAggFunction`), otherwise false.
    */
  private def findNonDeterministicOperator(rel: RelNode): Boolean = {
    rel match {
      case c: Calc =>
        val program = c.getProgram
        if (program.getCondition != null) {
          val condition = program.expandLocalRef(program.getCondition)
          if (!RexUtil.isDeterministic(condition)) {
            return true
          }
        }
        val projection = program.getProjectList.map(program.expandLocalRef)
        projection.exists(p => !RexUtil.isDeterministic(p))
      case j: Join => !RexUtil.isDeterministic(j.getCondition)
      case a: BatchExecGroupAggregateBase =>
        a.getAggCallList.exists(c => !c.getAggregation.isDeterministic)
      case o: BatchExecOverAggregate =>
        o.aggregateCalls.exists(c => !c.getAggregation.isDeterministic)
      case c: BatchExecCorrelate =>
        if (!RexUtil.isDeterministic(c.scan.getCall)) {
          return false
        }
        c.condition match {
          case Some(condition) => RexUtil.isDeterministic(condition)
          case _ => false
        }
      case _ => false
    }
  }
}

object RelDigestWriterImpl {
  private val nonDeterministicIdCounter = new AtomicInteger(0)

  def getDigest(rel: RelNode): String = {
    val sw = new StringWriter
    val tableConfig = FlinkRelOptUtil.getTableConfig(rel)
    rel.explain(new RelDigestWriterImpl(sw, tableConfig))
    sw.toString
  }
}
