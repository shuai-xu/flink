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

import java.util

import org.apache.calcite.rel.RelNode
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.{BatchExecRelShuttleImpl, BatchExecRelVisitorImpl}
import org.apache.flink.table.util.{FlinkRelOptUtil, RelDigestWriterImpl}

import scala.collection.JavaConversions._

/**
  * A FlinkOptimizeProgram that finds out duplicated sub-plan in the plan,
  * then use the same sub-plan for all the references.
  *
  * NOTES: This program can be only applied on [[RowBatchExecRel]] tree.
  *
  * @tparam OC OptimizeContext
  */
class FlinkReuseSubPlanProgram[OC <: OptimizeContext] extends FlinkOptimizeProgram[OC] {

  def optimize(input: RelNode, context: OC): RelNode = {
    val tableConfig = FlinkRelOptUtil.getTableConfig(input)

    input match {
      case root: RowBatchExecRel if tableConfig.getSubPlanReuse =>
        // 1. find out all reusable sub-plan
        val findReuseSubPlan = new FindReuseSubPlanVisitor
        root.accept(findReuseSubPlan)
        val reusableResult = findReuseSubPlan.getReusableResult
        if (reusableResult.hasReusableNode) {
          // 2. create ReusedBatchExec node by the result of step 1
          root.accept(new ReuseSubPlanShuttleImpl(reusableResult, tableConfig.getTableSourceReuse))
        } else {
          root
        }
      case _ => input
    }
  }

  class ReuseSubPlanShuttleImpl(
      reusableResult: ReusableResult,
      isTableSourceReuseEnabled: Boolean
  ) extends BatchExecRelShuttleImpl {
    // Some nodes are same object, uses this map to record how many times a node will be visited
    //
    // e.g. (SELECT a FROM t WHERE a > 10) UNION ALL (SELECT b FROM t WHERE b > 10)
    // UnionBatchExec(union=[a])
    //   CalcBatchExec(select=[a], where=[>(a, 10)])
    //     ScanTableBatchExec(table=[[t]], fields=[a, b, c])
    //   CalcBatchExec(select=[b], where=[>(b, 10)])
    //     ScanTableBatchExec(table=[[t]], fields=[a, b, c])
    //
    // the two `ScanTableBatchExec` are same object
    private val reusableNodeVisitedTimes = new util.IdentityHashMap[BatchExecRel[_], Integer]()

    override def visit(reused: BatchExecReused): BatchExecRel[_] = {
      throw new TableException("should not reach here, ReusedBatchExec has not been created yet.")
    }

    override protected def visitInputs(batchExecRel: BatchExecRel[_]): BatchExecRel[_] = {
      // TODO provide more strategies to pick the reusable Exchange node
      // for different scheduling strategies ???

      // nodes in reusable sub-plan are not in `mapNodeToDigest` (for performance sake),
      // so the digest of a node may be null.
      val digest = reusableResult.mapNodeToDigest(batchExecRel)
      if (digest == null) {
        return batchExecRel
      }

      val reusableNodes = reusableResult.reusableNodes(digest)
      require(reusableNodes.nonEmpty)
      if (reuse(reusableNodes)) {
        val visitedTimes = reusableNodeVisitedTimes.getOrElseUpdate(batchExecRel, 0) + 1
        reusableNodeVisitedTimes.put(batchExecRel, visitedTimes)
        // currently, the first same BatchExecRel which is found in the plan by depth-first search
        // algorithm will be used as reusable BatchExecRel node.
        val reusableNode = reusableNodes.head
        if ((batchExecRel eq reusableNode) && visitedTimes == 1) {
          batchExecRel
        } else {
          reusableNode.genReuseId()
          new BatchExecReused(reusableNode.getCluster, reusableNode.getTraitSet, reusableNode)
        }
      } else {
        super.visitInputs(batchExecRel)
      }
    }

    /**
      * Returns true if the reusableNodes can be reused, else false.
      */
    private def reuse(reusableNodes: util.List[BatchExecRel[_]]): Boolean = {
      if (reusableNodes.size > 1) {
        if (isTableSourceNode(reusableNodes.head)) {
          // TableSource node can be reused if reuse TableSource enabled
          isTableSourceReuseEnabled
        } else {
          true
        }
      } else {
        false
      }
    }

    /**
      * Check the given node is a [[BatchExecTableSourceScan]] node or
      * an [[BatchExecExchange]] with [[BatchExecTableSourceScan]] as its input.
      */
    private def isTableSourceNode(reusableNode: BatchExecRel[_]): Boolean = {
      reusableNode match {
        case _: BatchExecTableSourceScan => true
        case e: BatchExecExchange => isTableSourceNode(e.getInput.asInstanceOf[BatchExecRel[_]])
        case _ => false
      }
    }

  }

  /**
    * Find out all reusable nodes
    */
  class FindReuseSubPlanVisitor extends BatchExecRelVisitorImpl[Unit] {
    // mapping the digest of exchange node to BatchExecRels
    private val reusableNodes = new util.HashMap[String, util.List[BatchExecRel[_]]]()
    // mapping the BatchExecRel to its digest
    // It's a heavy work to get the digest of a relNode
    private val mapNodeToDigest = new util.IdentityHashMap[BatchExecRel[_], String]()

    def getReusableResult: ReusableResult = {
      ReusableResult(reusableNodes, mapNodeToDigest)
    }

    override def visit(reused: BatchExecReused): Unit = {
      throw new TableException("should not reach here, ReusedBatchExec has not been created yet.")
    }

    override def visit(batchExec: BatchExecRel[_]): Unit = {
      throw new TableException(s"Unknown BatchExecRel: ${batchExec.getClass.getCanonicalName}")
    }

    override protected def visitInputs(batchExecRel: BatchExecRel[_]): Unit = {
      // the same sub-plan should have same digest value,
      // uses `explain` with `RelDigestWriterImpl` to get the digest of a sub-plan.
      val key = RelDigestWriterImpl.getDigest(batchExecRel)
      mapNodeToDigest.put(batchExecRel, key)
      val nodes = reusableNodes.getOrElseUpdate(key, new util.ArrayList[BatchExecRel[_]]())
      nodes.add(batchExecRel)
      // the batchExecRel will be reused if there are more than one nodes with same digest,
      // so there is no need to visit a reused node's inputs.
      if (nodes.size() == 1) {
        super.visitInputs(batchExecRel)
      }
    }
  }

  case class ReusableResult(
      reusableNodes: util.HashMap[String, util.List[BatchExecRel[_]]],
      mapNodeToDigest: util.IdentityHashMap[BatchExecRel[_], String]
  ) {
    // Return true if the result has more than one nodes for per-digest, else false.
    def hasReusableNode: Boolean = reusableNodes.values().exists(_.size() > 1)
  }

}
