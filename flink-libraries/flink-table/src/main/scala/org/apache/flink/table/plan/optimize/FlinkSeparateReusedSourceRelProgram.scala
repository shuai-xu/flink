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
package org.apache.flink.table.plan.optimize

import com.google.common.collect.Sets
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.BatchExecRelShuttleImpl
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecRel, BatchExecReused, BatchExecTableSourceScan, BatchExecValues, RowBatchExecRel}

import scala.collection.JavaConversions._

/**
  * A FlinkSeparateReusedSourceRelProgram that finds out all reused source relNodes in the plan,
  * and copy them. We find only source relNodes are reused in calcite, so we only deal with
  * them here.
  *
  * NOTES: This program can be only applied on [[RowBatchExecRel]] tree.
  *
  * e.g. SQL: SELECT * FROM x x1, x x2 WHERE x1.a = x2.a
  * the physical plan tree is:
  * {{{
  *        ScanTableSource
  *            /      |
  *       Exchange    |
  *            \      |
  *             HashJoin
  *
  * }}}
  * With applying this optimizeProgram, the physical play tree is:
  * {{{
  *    ScanTableSource   ScanTableSource
  *            |         /
  *       Exchange      /
  *            \       /
  *             HashJoin
  *
  * }}}
  * @tparam OC OptimizeContext
  */
class FlinkSeparateReusedSourceRelProgram[OC <: OptimizeContext] extends FlinkOptimizeProgram[OC] {

  val shuttle = new SeparateReusedSourceRelShuttle

  def optimize(input: RelNode, context: OC): RelNode = {
    input match {
      case root: RowBatchExecRel => root.accept(shuttle)
      case _ => input
    }
  }

  class SeparateReusedSourceRelShuttle extends BatchExecRelShuttleImpl {

    private val identityHashSet = Sets.newIdentityHashSet[BatchExecRel[_]]()

    override def visit(scanTableSource: BatchExecTableSourceScan): BatchExecRel[_] = {
      val newScan = super.visit(scanTableSource).asInstanceOf[BatchExecTableSourceScan]
      recordOrCopy(newScan)
    }

    override def visit(values: BatchExecValues): BatchExecRel[_] = {
      val newValues = super.visit(values).asInstanceOf[BatchExecValues]
      recordOrCopy(newValues)
    }

    private def recordOrCopy(rel: BatchExecRel[_]): BatchExecRel[_] = {
      if (identityHashSet.contains(rel)) {
        rel.copy(rel.getTraitSet, rel.getInputs).asInstanceOf[BatchExecRel[_]]
      } else {
        identityHashSet.add(rel)
        rel
      }
    }

    protected override def visitInputs(batchExecRel: BatchExecRel[_]): BatchExecRel[_] = {
      batchExecRel.getInputs.zipWithIndex.foreach {
        case (input, index) =>
          val clonedInput = input.asInstanceOf[BatchExecRel[_]].accept(this)
          if (clonedInput ne input) {
            batchExecRel.replaceInput(index, clonedInput)
          }
      }
      batchExecRel
    }

    // do not visit inputs
    override def visit(reused: BatchExecReused): BatchExecRel[_] = reused
  }
}
