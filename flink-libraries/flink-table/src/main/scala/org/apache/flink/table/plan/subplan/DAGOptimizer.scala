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

import org.apache.flink.table.plan.schema.IntermediateRelNodeTable
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.logical.LogicalNode

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}

import java.util.IdentityHashMap

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Defines an optimizer to optimize a DAG instead of a simple tree.
  *
  * @param sinks A dag which is composed by [[LogicalNode]]
  * @param tEnv  all sinkNodes belongs to
  */
abstract class DAGOptimizer(sinks: Seq[LogicalNode], tEnv: TableEnvironment) {

  /**
    * Fetch optimized RelNode DAG. Each Root node in the DAG is an expanded RelNode, that means,
    *         it does not including [[IntermediateRelNodeTable]]. Besides, the reused node will be
    *         converted to the same RelNode.
    *
    * @return a list of RelNode represents an optimized RelNode DAG.
    *
    */
  def getOptimizedDag(): Seq[RelNode] = {
    // expand IntermediateTableScan in each RelNodeBlock
    expandIntermediateTableScan(getOptimizedRelNodeBlock.map(_.getOptimizedPlan))
  }

  /**
    * Explain the optimized RelNode DAG.
    *
    * @return string values which explains the optimized RelNode DAG.
    */
  def explain(): String = {
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
        sb.append(FlinkRelOptUtil.toString(block.getOptimizedPlan, withRetractTraits = true))
        sb.append(System.lineSeparator)
        visitedBlocks += block
      }
    }
    val blockPlan = getOptimizedRelNodeBlock()
    blockPlan.foreach(visitBlock(_, isSinkBlock = true))
    sb.toString
  }

  /**
    * Decompose RelNode DAG into multiple [[RelNodeBlock]]s, optimize recursively each
    * [[RelNodeBlock]], return optimized [[RelNodeBlock]]s.
    *
    * @return optimized [[RelNodeBlock]]s.
    */
  protected def getOptimizedRelNodeBlock(): Seq[RelNodeBlock]

  private def expandIntermediateTableScan(nodes: Seq[RelNode]): Seq[RelNode] = {

    class ExpandShuttle extends RelShuttleImpl {

      // ensure the same intermediateTable would be expanded to the same RelNode tree.
      private val expandedIntermediateTables =
        new IdentityHashMap[IntermediateRelNodeTable, RelNode]()

      override def visit(scan: TableScan): RelNode = {
        val intermediateTable = scan.getTable.unwrap(classOf[IntermediateRelNodeTable])
        if (intermediateTable != null) {
          expandedIntermediateTables.getOrElseUpdate(intermediateTable, {
            val underlyingRelNode = intermediateTable.relNode
            underlyingRelNode.accept(this)
          })
        } else {
          scan
        }
      }
    }

    val shuttle = new ExpandShuttle
    nodes.map(_.accept(shuttle))
  }

}
