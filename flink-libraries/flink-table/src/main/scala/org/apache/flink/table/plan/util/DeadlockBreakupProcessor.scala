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

import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.exec.{ExecNode, ExecNodeVisitor}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecBoundedStreamScan, _}

import com.google.common.collect.{Maps, Sets}
import org.apache.calcite.rel.{RelNode, RelVisitor}

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A DeadlockBreakupHandler that finds out all deadlocks in the plan, and resolves them.
  *
  * NOTES: This program can be only applied on [[BatchExecRel]] DAG.
  *
  * Reused node (may be a [[BatchExecRel]] which has more than one outputs or
  * a [[BatchExecBoundedStreamScan]] which transformation is used for different scan)
  * might lead to a deadlock when HashJoin or NestedLoopJoin have same reused inputs.
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
  * :           +- ScanTableSource(table=[[builtin, default, x,
  *                 source: [selectedFields=[a, b]]]], fields=[a, b])
  * +- Exchange(distribution=[broadcast])
  *    +- Calc(select=[a, b], where=[<(b, 20)])
  *       +- Reused(reference_id=[1])
  * }}}
  * the HashJoin's left input is probe side which could start to read data only after
  * build side has finished, so the Exchange node in HashJoin's left input requires BATCH mode
  * to block the stream. After this handler is applied, The simplified plan is:
  * {{{
  *                ScanTableSource
  *                    |
  *                Limit(global=[false])
  *                    |
  *                Exchange(single)
  *                    |
  *                Limit(global=[true], reuse_id=[1]))
  *                /      \
  *        Calc(b>10)     Calc(b<20)
  *               |        |
  * (broadcast)Exchange   Exchange(exchange_mode=[BATCH]) add BATCH Exchange to breakup deadlock
  *    (build side)\       /(probe side)
  *                HashJoin
  *                   |
  *               Calc(select=[a])
  * }}}
  */
class DeadlockBreakupProcessor {

  // TODO change arguments to sinks: Seq[BatchExecNode[_]]
  def process(sinks: Seq[RelNode]): Seq[RelNode] = {
    // TODO remove this
    require(sinks.head.isInstanceOf[BatchExecRel[_]])

    val finder = new ReuseNodeFinder()
    sinks.foreach(sink => finder.go(sink))
    sinks.foreach(
      sink => sink.asInstanceOf[BatchExecRel[_]].accept(new DeadlockBreakupShuttleImpl(finder))
    )
    sinks
  }

  /**
    * Find reuse node.
    * A reuse node has more than one output or is BatchExecBoundedStreamScan which DataStream
    * object is held by different BatchExecBoundedStreamScans.
    */
  class ReuseNodeFinder extends RelVisitor {
    // map a node object to its visited times. the visited times of a reused node is more than one
    private val visitedTimes = Maps.newIdentityHashMap[RelNode, Integer]()
    // different BatchExecBoundedStreamScans may have same DataStream object
    // map DataStream object to BatchExecBoundedStreamScans
    private val mapDataStreamToScan =
    Maps.newIdentityHashMap[DataStream[_], util.List[BatchExecBoundedStreamScan]]()

    /**
      * Return true if the visited time of the given node is more than one,
      * or the node is a [[BatchExecBoundedStreamScan]] and its [[DataStream]] object is hold
      * by different [[BatchExecBoundedStreamScan]]s. else false.
      */
    def isReusedNode(node: RelNode): Boolean = {
      if (visitedTimes.getOrDefault(node, 0) > 1) {
        true
      } else {
        node match {
          case scan: BatchExecBoundedStreamScan =>
            val dataStream = scan.boundedStreamTable.dataStream
            val scans = mapDataStreamToScan.get(dataStream)
            scans != null && scans.size() > 1
          case _ => false
        }
      }
    }

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      val times = visitedTimes.getOrDefault(node, 0)
      visitedTimes.put(node, times + 1)
      node match {
        case scan: BatchExecBoundedStreamScan =>
          val dataStream = scan.boundedStreamTable.dataStream
          val scans = mapDataStreamToScan.getOrElseUpdate(
            dataStream, new util.ArrayList[BatchExecBoundedStreamScan]())
          scans.add(scan)
        case _ => // do nothing
      }
      super.visit(node, ordinal, parent)
    }
  }

  class DeadlockBreakupShuttleImpl(finder: ReuseNodeFinder) extends BatchExecNodeVisitor {

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
      val reusedNodesInBuildSide = findReusedNodesInBuildSide(buildNode, finder)
      // 2. find all nodes from probe side of join
      val inputPathsOfProbeSide = buildInputPathsOfProbeSide(
        probeNode, reusedNodesInBuildSide, finder)
      // 3. check whether all input paths have a barrier node (e.g. agg, sort)
      if (inputPathsOfProbeSide.nonEmpty && !hasBarrierNodeInInputPaths(inputPathsOfProbeSide)) {
        // 4. sets Exchange node(if does not exist, add one) as BATCH mode to break up the deadlock
        probeNode match {
          case e: BatchExecExchange =>
            // TODO create a cloned BatchExecExchange for PIPELINE output
            e.setRequiredDataExchangeMode(DataExchangeMode.BATCH)
          case _ =>
            val traitSet = probeNode.getTraitSet.replace(distribution)
            val e = new BatchExecExchange(
              probeNode.getCluster,
              traitSet,
              probeNode,
              distribution)
            e.setRequiredDataExchangeMode(DataExchangeMode.BATCH)
            join.replaceInputNode(if (leftIsBuild) 1 else 0, e)
        }
      }
      join
    }

    override def visit(hashJoin: BatchExecHashJoinBase): Unit = {
      super.visit(hashJoin)
      val joinInfo = hashJoin.joinInfo
      val columns = if (hashJoin.leftIsBuild) joinInfo.rightKeys else joinInfo.leftKeys
      val distribution = FlinkRelDistribution.hash(columns)
      rewriteJoin(hashJoin, hashJoin.leftIsBuild, distribution)
    }

    override def visit(nestedLoopJoin: BatchExecNestedLoopJoinBase): Unit = {
      super.visit(nestedLoopJoin)
      rewriteJoin(nestedLoopJoin, nestedLoopJoin.leftIsBuild, FlinkRelDistribution.ANY)
    }
  }

  /**
    * Find all reused nodes in build side of join.
    */
  private def findReusedNodesInBuildSide(
      buildNode: BatchExecRel[_],
      finder: ReuseNodeFinder): Set[BatchExecRel[_]] = {
    val nodesInBuildSide = Sets.newIdentityHashSet[BatchExecRel[_]]()
    buildNode.accept(new ExecNodeVisitor {
      override protected def visitInputs(batchExecNode: ExecNode[_, _]): Unit = {
        val batchExecRel = batchExecNode.asInstanceOf[BatchExecRel[_]]
        if (finder.isReusedNode(batchExecRel)) {
          nodesInBuildSide.add(batchExecRel)
        }
        super.visitInputs(batchExecNode)
      }
    })
    nodesInBuildSide.toSet
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
      reusedNodesInBuildSide: Set[BatchExecRel[_]],
      finder: ReuseNodeFinder): List[Array[BatchExecRel[_]]] = {
    val result = new mutable.ListBuffer[Array[BatchExecRel[_]]]()
    val stack = new mutable.Stack[BatchExecRel[_]]()

    if (reusedNodesInBuildSide.isEmpty) {
      return result.toList
    }

    probeNode.accept(new ExecNodeVisitor {
      override protected def visitInputs(batchExecNode: ExecNode[_, _]): Unit = {
        val batchExecRel = batchExecNode.asInstanceOf[BatchExecRel[_]]
        stack.push(batchExecRel)
        if (finder.isReusedNode(batchExecRel) &&
          isReusedNodeInBuildSide(batchExecRel, reusedNodesInBuildSide)) {
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
    * Returns true if the given rel is in `reusedNodesInBuildSide`, else false.
    * NOTES: We treat different [[BatchExecBoundedStreamScan]]s with same [[DataStream]]
    * object as the same.
    */
  private def isReusedNodeInBuildSide(
      batchExecRel: BatchExecRel[_],
      reusedNodesInBuildSide: Set[BatchExecRel[_]]): Boolean = {
    if (reusedNodesInBuildSide.contains(batchExecRel)) {
      true
    } else {
      batchExecRel match {
        case scan: BatchExecBoundedStreamScan =>
          reusedNodesInBuildSide.exists {
            case reusedScan: BatchExecBoundedStreamScan =>
              reusedScan.boundedStreamTable.dataStream eq scan.boundedStreamTable.dataStream
            case _ => false
          }
        case _ => false
      }
    }
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
