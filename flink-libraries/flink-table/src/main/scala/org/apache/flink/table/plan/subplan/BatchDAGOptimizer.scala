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

package org.apache.flink.table.plan.subplan

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.rel.RelNode

import scala.collection.mutable

class BatchDAGOptimizer(sinks: Seq[LogicalNode], tEnv: BatchTableEnvironment)
  extends DAGOptimizer(sinks, tEnv) {

  protected lazy val blockPlan: Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val relNodeBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(sinks, tEnv)
    // optimize recursively RelNodeBlock
    relNodeBlocks.foreach(optimize)
    relNodeBlocks
  }

  override def getOptimizedRelNodeBlock(): Seq[RelNodeBlock] = blockPlan

  override def explain(): String = {
    val sb = new StringBuilder
    val visitedBlocks = mutable.Set[RelNodeBlock]()

    def visitBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
      if (!visitedBlocks.contains(block)) {
        block.children.foreach(visitBlock(_, isSinkBlock = false))
        if (isSinkBlock) {
          sb.append("[[Sink]]")
        } else {
          sb.append(s"[[IntermediateTable=${block.getOutputTableName}]]")
        }
        sb.append(System.lineSeparator)
        sb.append(FlinkRelOptUtil.toString(block.getOptimizedPlan))
        sb.append(System.lineSeparator)
        visitedBlocks += block
      }
    }

    blockPlan.foreach(visitBlock(_, isSinkBlock = true))
    sb.toString
  }

  private def optimize(block: RelNodeBlock): Unit = {
    block.children.foreach { child =>
      if (child.getNewOutputNode.isEmpty) {
        optimize(child)
      }
    }

    val originTree = block.getPlan
    val optimizedTree = tEnv.optimize(originTree)
    tEnv.addQueryPlan(originTree, optimizedTree)

    optimizedTree match {
      case n: BatchExecSink[_] => // ignore
      case _ =>
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(name, optimizedTree)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
    }
    block.setOptimizedPlan(optimizedTree)
  }

  private def registerIntermediateTable(name: String, relNode: RelNode): Unit = {
    val table = new IntermediateRelNodeTable(relNode)
    tEnv.registerTableInternal(name, table)
  }

}
