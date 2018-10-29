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
import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.batch.{BatchExecRelShuttleImpl, BatchExecRelVisitorImpl}

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * A FlinkOptimizeProgram that finds out all deadlocks in the plan, and resolves them.
  *
  * NOTES: This program can be only applied on [[RowBatchExecRel]] tree.
  *
  * Reused node (may be a [[BatchExecReused]] or a [[BatchExecBoundedDataStreamScan]]) might
  * lead to a deadlock when HashJoin or NestedLoopJoin have same reused input.
  * Sets Exchange node(if it does not exist, add one) as BATCH mode to break up the deadlock.
  *
  * e.g. SQL: WITH r AS (SELECT a, b FROM x limit 10)
  * SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b > 10 AND r2.b < 20
  * the physical plan is: (sub-plan reused is enabled)
  * {{{
  * Calc(select=[a, b, b0])
  * +- HashJoin(where=[=(a, a0)], join=[a, b, a0, b0], joinType=[InnerJoin], isBroadcast=[true],
  *      build=[left])
  * :- Calc(select=[a, b], where=[>(b, 10)])
  * :  +- Limit(offset=[0], limit=[10], global=[true], reuse_id=[1])
  * :     +- Exchange(distribution=[single])
  * :        +- Limit(offset=[0], limit=[10], global=[false])
  * :           +- ScanTableSource(table=[[x, source: [selectedFields=[a, b]]]], fields=[a, b])
  * +- Exchange(distribution=[broadcast])
  *    +- Calc(select=[a, b], where=[<(b, 20)])
  *       +- Reused(reference_id=[1])
  * }}}
  * the HashJoin's left input is probe side which could start to read data only after
  * build side has finished, so the Exchange node in HashJoin's left input requires BATCH mode
  * to block the stream. After this program is applied, The simplified plan is:
  * {{{
  *                ScanTableSource
  *                    |
  *                Limit(global=[false])
  *                    |
  *                Exchange(single)
  *                    |
  *                Limit(global=[true], reuse_id=[1]))
  *                /      \
  *        Calc(b>10)     Reused
  *               |        |
  *               |       Calc(b<20)
  *               |        |
  * (broadcast)Exchange   Exchange(exchange_mode=[BATCH]) add BATCH Exchange to breakup deadlock
  *    (build side)\       /(probe side)
  *                HashJoin
  *                   |
  *               Calc(select=[a])
  * }}}
  *
  * @tparam OC OptimizeContext
  */
class FlinkDeadlockBreakupProgram[OC <: OptimizeContext] extends FlinkOptimizeProgram[OC] {

  def optimize(input: RelNode, context: OC): RelNode = {
    input match {
      case root: RowBatchExecRel => root.accept(new DeadlockBreakupShuttleImpl)
      case _ => input
    }
  }

  class DeadlockBreakupShuttleImpl extends BatchExecRelShuttleImpl {

    private def rewriteJoin(
        join: BatchExecJoinBase,
        leftIsBuild: Boolean,
        distribution: FlinkRelDistribution): BatchExecRel[_] = {
      val (buildNode, probeNode) = if (leftIsBuild) {
        (join.getLeft.asInstanceOf[BatchExecRel[_]], join.getRight.asInstanceOf[BatchExecRel[_]])
      } else {
        (join.getRight.asInstanceOf[BatchExecRel[_]], join.getLeft.asInstanceOf[BatchExecRel[_]])
      }

      // 1. find all reused nodes in build side of join.
      val reusedNodesInBuildSide = findReusedNodesInBuildSide(buildNode)
      // 2. find all nodes from probe side of join
      val inputPathsOfProbeSide = buildInputPathsOfProbeSide(probeNode, reusedNodesInBuildSide)
      // 3. check whether all input paths have a barrier node (e.g. agg, sort)
      if (inputPathsOfProbeSide.nonEmpty && !hasBarrierNodeInInputPaths(inputPathsOfProbeSide)) {
        // 4. sets Exchange node(if does not exist, add one) as BATCH mode to break up the deadlock
        probeNode match {
          case e: BatchExecExchange =>
            e.setRequiredDataExchangeMode(DataExchangeMode.BATCH)
          case r: BatchExecReused if r.getInput.isInstanceOf[BatchExecExchange] =>
            // TODO create a cloned BatchExecExchange for PIPELINE output
            r.getInput.asInstanceOf[BatchExecExchange]
              .setRequiredDataExchangeMode(DataExchangeMode.BATCH)
          case _ =>
            val traitSet = probeNode.getTraitSet.replace(distribution)
            val e = new BatchExecExchange(
              probeNode.getCluster,
              traitSet,
              probeNode,
              distribution)
            e.setRequiredDataExchangeMode(DataExchangeMode.BATCH)
            val newInputs = if (leftIsBuild) List(join.getLeft, e) else List(e, join.getRight)
            return join.copy(join.getTraitSet, newInputs).asInstanceOf[BatchExecRel[_]]
        }
      }
      join
    }

    override def visit(hashJoin: BatchExecHashJoinBase): BatchExecRel[_] = {
      val newHashJoin = super.visit(hashJoin).asInstanceOf[BatchExecHashJoinBase]
      val joinInfo = hashJoin.joinInfo
      val columns = if (hashJoin.leftIsBuild) joinInfo.rightKeys else joinInfo.leftKeys
      val distribution = FlinkRelDistribution.hash(columns)
      rewriteJoin(newHashJoin, newHashJoin.leftIsBuild, distribution)
    }

    override def visit(nestedLoopJoin: BatchExecNestedLoopJoinBase): BatchExecRel[_] = {
      val newNlJoin = super.visit(nestedLoopJoin).asInstanceOf[BatchExecNestedLoopJoinBase]
      rewriteJoin(newNlJoin, newNlJoin.leftIsBuild, FlinkRelDistribution.ANY)
    }
  }

  /**
    * A reused node may be
    * 1. a [[BatchExecReused]], the transformation will be reused
    * 2. or a [[BatchExecBoundedDataStreamScan]], the transformation of
    * [[org.apache.flink.streaming.api.datastream.DataStream]] is reused.
    */
  private def isReusedNode(batchExecRel: BatchExecRel[_]): Boolean = {
    batchExecRel.isReused || batchExecRel.isInstanceOf[BatchExecBoundedDataStreamScan]
  }

  /**
    * Find all reused nodes in build side of join.
    */
  private def findReusedNodesInBuildSide(
      buildNode: BatchExecRel[_]): mutable.Set[BatchExecRel[_]] = {
    val nodesInBuildSide = Sets.newIdentityHashSet[BatchExecRel[_]]()
    buildNode.accept(new BatchExecRelVisitorImpl[Unit] {
      override protected def visitInputs(batchExecRel: BatchExecRel[_]): Unit = {
        if (isReusedNode(batchExecRel)) {
          nodesInBuildSide.add(batchExecRel)
        }
        super.visitInputs(batchExecRel)
      }
    })
    nodesInBuildSide
  }

  /**
    * Visit all nodes in probe side of join until to the reused nodes
    * which are in `reusedNodesInBuildSide` collection.
    * e.g. (sub-plan reused is enabled)
    * {{{
    *           table source
    *                |
    *              calc1 (reusedId=1)
    *              /   \
    *           agg1  reused
    *             |    |
    *           calc2 agg2
    * (build side) \   / (probe side)
    *            hash join
    * }}}
    * the input-path of join's probe side is [agg2, reused, calc1].
    *
    * e.g. (sub-plan reused is disabled)
    * {{{
    *             scan table
    *              /   \
    *           calc1   calc2
    * (build side) \   / (probe side)
    *            hash join
    * }}}
    * the input-path of join's probe side is [calc2, scan].
    */
  private def buildInputPathsOfProbeSide(
      probeNode: BatchExecRel[_],
      reusedNodesInBuildSide: mutable.Set[BatchExecRel[_]]): List[Array[BatchExecRel[_]]] = {
    val result = new mutable.ListBuffer[Array[BatchExecRel[_]]]()
    val stack = new mutable.Stack[BatchExecRel[_]]()

    if (reusedNodesInBuildSide.isEmpty) {
      return result.toList
    }

    probeNode.accept(new BatchExecRelVisitorImpl[Unit] {
      override protected def visitInputs(batchExecRel: BatchExecRel[_]): Unit = {
        stack.push(batchExecRel)
        // NOTES: BatchExecReused is a dummy node, its `isReused` method always returns false.
        if (isReusedNode(batchExecRel) && reusedNodesInBuildSide.contains(batchExecRel)) {
          result.add(stack.toArray.reverse)
        } else {
          super.visitInputs(batchExecRel)
        }
        stack.pop()
      }
    })

    require(stack.isEmpty)
    result.toList
  }

  /**
    * Returns true if all input-paths have barrier node (e.g. agg, sort), otherwise false.
    */
  private def hasBarrierNodeInInputPaths(
      inputPathsOfProbeSide: List[Array[BatchExecRel[_]]]): Boolean = {
    require(inputPathsOfProbeSide.nonEmpty)

    /** Return true if the successor of join in the input-path is build node, otherwise false */
    def checkJoinBuildSide(
        buildNode: RelNode,
        idxOfJoin: Int,
        inputPath: Array[BatchExecRel[_]]): Boolean = {
      if (idxOfJoin < inputPath.length - 1) {
        val nextNode = inputPath(idxOfJoin + 1)
        // next node is build node of hash join
        buildNode eq nextNode
      } else {
        false
      }
    }

    inputPathsOfProbeSide.forall {
      inputPath =>
        var idx = 0
        var hasBarrierNode = false
        // should exclude the reused node (at last position in path)
        while (!hasBarrierNode && idx < inputPath.length - 1) {
          val node = inputPath(idx)
          hasBarrierNode = if (node.isBarrierNode) {
            true
          } else {
            node match {
              case h: BatchExecHashJoinBase =>
                val buildNode = if (h.leftIsBuild) h.getLeft else h.getRight
                checkJoinBuildSide(buildNode, idx, inputPath)
              case n: BatchExecNestedLoopJoinBase =>
                val buildNode = if (n.leftIsBuild) n.getLeft else n.getRight
                checkJoinBuildSide(buildNode, idx, inputPath)
              case _ => false
            }
          }
          idx += 1
        }
        hasBarrierNode
    }
  }
}
