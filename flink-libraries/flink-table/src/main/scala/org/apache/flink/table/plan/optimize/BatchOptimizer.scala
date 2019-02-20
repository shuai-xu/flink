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

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink
import org.apache.flink.table.plan.optimize.program.{BatchOptimizeContext, FlinkBatchPrograms}
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable
import org.apache.flink.table.plan.util.SameRelObjectShuttle
import org.apache.flink.util.Preconditions
import org.apache.calcite.plan.{Context, Contexts, RelOptPlanner}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.calcite.FlinkChainContext

/**
  * Query optimizer for Batch.
  */
class BatchOptimizer(tEnv: BatchTableEnvironment) extends AbstractOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val sinkBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, tEnv)
    // optimize recursively RelNodeBlock
    sinkBlocks.foreach(optimizeBlock)
    sinkBlocks
  }

  private def optimizeBlock(block: RelNodeBlock): Unit = {
    block.children.foreach { child =>
      if (child.getNewOutputNode.isEmpty) {
        optimizeBlock(child)
      }
    }

    val originTree = block.getPlan
    val optimizedTree = optimizeTree(originTree)

    optimizedTree match {
      case _: BatchExecSink[_] => // ignore
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

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(relNode: RelNode): RelNode = {
    val config = tEnv.getConfig
    val programs = config.getCalciteConfig.getBatchPrograms
      .getOrElse(FlinkBatchPrograms.buildPrograms(config.getConf))
    Preconditions.checkNotNull(programs)

    val optimizedPlan = programs.optimize(relNode, new BatchOptimizeContext {
      override def getContext: Context = FlinkChainContext.chain(
        tEnv.getFrameworkConfig.getContext, Contexts.of(tEnv.getFlinkPlanner))

      override def getRelOptPlanner: RelOptPlanner = tEnv.getPlanner
    })

    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    optimizedPlan.accept(new SameRelObjectShuttle())
  }

}
