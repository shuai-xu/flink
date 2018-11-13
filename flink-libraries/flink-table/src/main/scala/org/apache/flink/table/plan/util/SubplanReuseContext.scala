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

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.{Maps, Sets}
import org.apache.calcite.rel.core.Exchange
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.util.RelDigestWriterImpl

import scala.collection.JavaConversions._

/**
  * The Context holds sub-plan reuse information.
  *
  * <p>If two sub-plan ([[RelNode]] tree with leaf node) have same digest, they are in a reusable
  * sub-plan group. In a reusable sub-plan group, the leftmost sub-plan is reused sub-plan and the
  * remaining will reuse the leftmost one.
  * <p>Uses reuse id to distinguish different reusable sub-plan group, the reuse id of each sub-plan
  * is same in a group.
  *
  * <p>e.g.
  * {{{
  *       Join
  *     /      \
  * Filter1  Filter2
  *    |        |
  * Project1 Project2
  *    |        |
  *  Scan1    Scan2
  * }}}
  * Project1-Scan1 and Project2-Scan2 have same digest, so they are in a reusable sub-plan group.
  */
class SubplanReuseContext(root: RelNode, config: TableConfig) {
  // mapping a relNode to its digest
  private val mapNodeToDigest = Maps.newIdentityHashMap[RelNode, String]()
  // mapping the digest to RelNodes
  private val mapDigestToReusableNodes = new util.HashMap[String, util.List[RelNode]]()
  // mapping the digest to reuse id
  private val mapDigestToReuseId = new util.HashMap[String, Integer]()

  val visitor = new ReusableSubplanVisitor()
  visitor.go(root)

  /**
    * Return the digest of the given rel node.
    */
  def getRelDigest(node: RelNode): String = RelDigestWriterImpl.getDigest(node)

  /**
    * Return reuse id for the given node.
    * If the give node is not a reusable node, throw TableException
    */
  def getReuseId(node: RelNode): Integer = {
    val digest = mapNodeToDigest.get(node)
    if (digest == null) {
      throw new TableException(s"${node.getRelTypeName}(id=${node.getId}) is not found")
    }
    val reuseId = mapDigestToReuseId.get(digest)
    if (reuseId == null) {
      throw new TableException(s"${node.getRelTypeName}(id=${node.getId}) is not a reusable node")
    }
    reuseId
  }

  /**
    * Returns true if the given node can be reused by other nodes, else false.
    * The nodes with same digest are reusable,
    * and the head node of node-list is reused by remaining nodes.
    */
  def reusedByOtherNode(node: RelNode): Boolean = {
    val reusableNodes = getReusableNodes(node)
    if (isReusableNodes(reusableNodes)) {
      node eq reusableNodes.head
    } else {
      false
    }
  }

  /**
    * Returns true if the given node can reuse other node, else false.
    * The nodes with same digest are reusable,
    * and the non-head node of node-list can reuse the head node.
    */
  def reuseOtherNode(node: RelNode): Boolean = {
    val reusableNodes = getReusableNodes(node)
    if (isReusableNodes(reusableNodes)) {
      node ne reusableNodes.head
    } else {
      false
    }
  }

  /**
    * Returns reusable nodes which have same digest.
    */
  private def getReusableNodes(node: RelNode): List[RelNode] = {
    val digest = mapNodeToDigest.get(node)
    if (digest == null) {
      // the node is in the reused sub-plan (not the root node of the sub-plan)
      List.empty[RelNode]
    } else {
      mapDigestToReusableNodes.get(digest).toList
    }
  }

  /**
    * Returns true if the given nodes can be reused, else false.
    */
  private def isReusableNodes(reusableNodes: List[RelNode]): Boolean = {
    if (reusableNodes.size() > 1) {
      if (isTableSource(reusableNodes.head)) {
        // TableSource node can be reused if reuse TableSource enabled
        config.getTableSourceReuse
      } else {
        true
      }
    } else {
      false
    }
  }

  /**
    * Returns true if the given node is a TableSourceScan or
    * an [[Exchange]] with TableSourceScan as its input, else false.
    */
  private def isTableSource(node: RelNode): Boolean = {
    node match {
      case _: FlinkLogicalTableSourceScan | _: PhysicalTableSourceScan => true
      case e: Exchange => isTableSource(e.getInput)
      case _ => false
    }
  }

  class ReusableSubplanVisitor extends RelVisitor {
    private val visitedNodes = Sets.newIdentityHashSet[RelNode]()
    private val reuseIdCounter = new AtomicInteger(0)

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      if (visitedNodes.contains(node)) {
        throw new IllegalArgumentException(
          "Plan has same reference nodes, please rewrite plan with SameRelObjectShuttle first!")
      }
      visitedNodes.add(node)

      // the same sub-plan should have same digest value,
      // uses `explain` with `RelDigestWriterImpl` to get the digest of a sub-plan.
      val digest = RelDigestWriterImpl.getDigest(node)
      mapNodeToDigest.put(node, digest)
      val nodes = mapDigestToReusableNodes.getOrElseUpdate(digest, new util.ArrayList[RelNode]())
      nodes.add(node)
      // the node will be reused if there are more than one nodes with same digest,
      // so there is no need to visit a reused node's inputs.
      if (isReusableNodes(nodes.toList)) {
        // add reuse id for reused node's digest
        if (!mapDigestToReuseId.contains(digest)) {
          val reuseId = reuseIdCounter.incrementAndGet()
          mapDigestToReuseId.put(digest, reuseId)
        }
      } else {
        super.visit(node, ordinal, parent)
      }
    }
  }

}
