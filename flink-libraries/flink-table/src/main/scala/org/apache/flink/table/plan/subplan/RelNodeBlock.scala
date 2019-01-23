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

import org.apache.flink.table.api.{TableConfigOptions, TableEnvironment, TableException}
import org.apache.flink.table.plan.logical.{LogicalNode, SinkNode}
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.schema.RelTable
import org.apache.flink.table.plan.util.{SubplanReuseContext, SubplanReuseShuttle}
import org.apache.flink.util.Preconditions

import com.google.common.collect.Sets
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A [[RelNodeBlock]] is a sub-tree in the [[RelNode]] plan. All [[RelNode]]s in
  * each block have only one [[Sink]] output.
  * The nodes in different block will be optimized independently.
  *
  * For example: (Table API)
  *
  * {{{-
  *  val sourceTable = tEnv.scan("test_table").select('a, 'b, 'c)
  *  val leftTable = sourceTable.filter('a > 0).select('a as 'a1, 'b as 'b1)
  *  val rightTable = sourceTable.filter('c.isNotNull).select('b as 'b2, 'c as 'c2)
  *  val joinTable = leftTable.join(rightTable, 'a1 === 'b2)
  *  joinTable.where('a1 >= 70).select('a1, 'b1).writeToSink(sink1)
  *  joinTable.where('a1 < 70 ).select('a1, 'c2).writeToSink(sink2)
  * }}}
  *
  * the RelNode DAG is:
  *
  * {{{-
  * Sink(sink1)     Sink(sink2)
  *    |               |
  * Project(a1,b1)  Project(a1,c2)
  *    |               |
  * Filter(a1>=70)  Filter(a1<70)
  *       \          /
  *        Join(a1=b2)
  *       /           \
  * Project(a1,b1)  Project(b2,c2)
  *      |             |
  * Filter(a>0)     Filter(c is not null)
  *      \           /
  *      Project(a,b,c)
  *          |
  *       TableScan
  * }}}
  *
  * This [[RelNode]] DAG will be decomposed into three [[RelNodeBlock]]s, the break-point
  * is the [[RelNode]](`Join(a1=b2)`) which data outputs to multiple [[Sink]]s.
  * <p>Notes: Although `Project(a,b,c)` has two parents (outputs),
  * they eventually merged at `Join(a1=b2)`. So `Project(a,b,c)` is not a break-point.
  * <p>the first [[RelNodeBlock]] includes TableScan, Project(a,b,c), Filter(a>0),
  * Filter(c is not null), Project(a1,b1), Project(b2,c2) and Join(a1=b2)
  * <p>the second one includes Filter(a1>=70), Project(a1,b1) and Sink(sink1)
  * <p>the third one includes Filter(a1<70), Project(a1,c2) and Sink(sink2)
  * <p>And the first [[RelNodeBlock]] is the child of another two.
  *
  * The [[RelNodeBlock]] plan is:
  * {{{-
  * RelNodeBlock2  RelNodeBlock3
  *        \            /
  *        RelNodeBlock1
  * }}}
  *
  * The optimizing order is from child block to parent. The optimized result (RelNode)
  * will be registered into tables first, and then be converted to a new TableScan which is the
  * new output node of current block and is also the input of its parent blocks.
  *
  * @param outputNode A RelNode of the output in the block, which could be a [[Sink]] or
  * other RelNode which data outputs to multiple [[Sink]]s.
  */
class RelNodeBlock(val outputNode: RelNode, tEnv: TableEnvironment) {
  // child (or input) blocks
  private val childBlocks = mutable.LinkedHashSet[RelNodeBlock]()

  // After this block has been optimized, the result will be converted to a new TableScan as
  // new output node
  private var newOutputNode: Option[RelNode] = None

  private var outputTableName: Option[String] = None

  private var optimizedPlan: Option[RelNode] = None

  private var updateAsRetract: Boolean = false

  def addChild(block: RelNodeBlock): Unit = childBlocks += block

  def children: Seq[RelNodeBlock] = childBlocks.toSeq

  def setNewOutputNode(newNode: RelNode): Unit = newOutputNode = Option(newNode)

  def getNewOutputNode: Option[RelNode] = newOutputNode

  def setOutputTableName(name: String): Unit = outputTableName = Option(name)

  def getOutputTableName: String = outputTableName.orNull

  def setOptimizedPlan(rel: RelNode): Unit = this.optimizedPlan = Option(rel)

  def getOptimizedPlan: RelNode = optimizedPlan.orNull

  def setUpdateAsRetraction(updateAsRetract: Boolean): Unit = {
    // set child block updateAsRetract, a child may have multi father.
    if (updateAsRetract) {
      this.updateAsRetract = true
    }
  }

  def isUpdateAsRetraction: Boolean = updateAsRetract

  def getChildBlockOutputNode(node: RelNode): Option[RelNodeBlock] = {
    val find = children.filter(_.outputNode.equals(node))
    if (find.isEmpty) {
      None
    } else {
      Preconditions.checkArgument(find.size == 1)
      Some(find.head)
    }
  }

  /**
    * Get new plan of this block. The child blocks (inputs) will be replace with new RelNodes (the
    * optimized result of child block).
    *
    * @return New plan of this block
    */
  def getPlan: RelNode = {
    val shuttle = new RelNodeBlockShuttle
    outputNode.accept(shuttle)
  }

  private class RelNodeBlockShuttle extends RelShuttleImpl {

    override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
      val block = getChildBlockOutputNode(parent)
      block match {
        case Some(b) => b.getNewOutputNode.get
        case _ => super.visitChild(parent, i, child)
      }
    }

    override def visitChildren(rel: RelNode): RelNode = {
      val block = getChildBlockOutputNode(rel)
      block match {
        case Some(b) => b.getNewOutputNode.get
        case _ => super.visitChildren(rel)
      }
    }
  }

}

/**
  * Holds information to build [[RelNodeBlock]].
  */
class RelNodeWrapper(relNode: RelNode) {
  // parent nodes of `relNode`
  private val parentNodes = Sets.newIdentityHashSet[RelNode]()
  // output nodes of some blocks that data of `relNode` outputs to
  private val blockOutputNodes = Sets.newIdentityHashSet[RelNode]()
  // stores visited parent nodes when builds RelNodeBlock
  private val visitedParentNodes = Sets.newIdentityHashSet[RelNode]()

  def addParentNode(parent: Option[RelNode]): Unit = {
    parent match {
      case Some(p) => parentNodes.add(p)
      case None => // Ignore
    }
  }

  def addVisitedParentNode(parent: Option[RelNode]): Unit = {
    parent match {
      case Some(p) =>
        require(parentNodes.contains(p))
        visitedParentNodes.add(p)
      case None => // Ignore
    }
  }

  def addBlockOutputNode(blockOutputNode: RelNode): Unit = blockOutputNodes.add(blockOutputNode)

  /**
    * Returns true if all parent nodes had been visited, else false
    */
  def allParentNodesVisited: Boolean = parentNodes.size() == visitedParentNodes.size()

  /**
    * Returns true if number of `blockOutputNodes` is greater than 1, else false
    */
  def hasMultipleBlockOutputNodes: Boolean = blockOutputNodes.size() > 1

  /**
    * Returns the output node of the block that the `relNode` belongs to
    */
  def getBlockOutputNode: RelNode = {
    if (hasMultipleBlockOutputNodes) {
      // If has multiple block output nodes, the `relNode` is a break-point.
      // So the `relNode` is the output node of the block that the `relNode` belongs to
      relNode
    } else {
      // the `relNode` is not a break-point
      require(blockOutputNodes.size == 1)
      blockOutputNodes.head
    }
  }
}

/**
  * Builds [[RelNodeBlock]] plan
  */
class RelNodeBlockPlanBuilder private(tEnv: TableEnvironment) {

  private val node2Wrapper = new util.IdentityHashMap[RelNode, RelNodeWrapper]()
  private val node2Block = new util.IdentityHashMap[RelNode, RelNodeBlock]()

  private val isUnionAllAsBreakPointDisabled = tEnv.config.getConf.getBoolean(
    TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED)


  /**
    * Decompose the [[RelNode]] plan into many [[RelNodeBlock]]s,
    * and rebuild [[RelNodeBlock]] plan.
    *
    * @param  sinks RelNode DAG to decompose
    * @return Sink-RelNodeBlocks, each Sink-RelNodeBlock is a tree.
    */
  def buildRelNodeBlockPlan(sinks: Seq[RelNode]): Seq[RelNodeBlock] = {
    sinks.foreach(buildRelNodeWrappers(_, None))
    buildBlockOutputNodes(sinks)
    sinks.map(buildBlockPlan)
  }

  private def buildRelNodeWrappers(node: RelNode, parent: Option[RelNode]): Unit = {
    node2Wrapper.getOrElseUpdate(node, new RelNodeWrapper(node)).addParentNode(parent)
    node.getInputs.foreach(child => buildRelNodeWrappers(child, Some(node)))
  }

  private def buildBlockPlan(node: RelNode): RelNodeBlock = {
    val currentBlock = new RelNodeBlock(node, tEnv)
    buildBlock(node, currentBlock, createNewBlock = false)
    currentBlock
  }

  private def buildBlock(
      node: RelNode,
      currentBlock: RelNodeBlock,
      createNewBlock: Boolean): Unit = {
    val hasDiffBlockOutputNodes = node2Wrapper(node).hasMultipleBlockOutputNodes
    val isTableFunctionScan = node.isInstanceOf[TableFunctionScan]
    // TableFunctionScan cannot be optimized individually,
    // so TableFunctionScan is not a break-point even though it has multiple parents
    val isBreakPoint = hasDiffBlockOutputNodes && !isTableFunctionScan

    if (isUnionAllAsBreakPointDisabled && isUnionAllNode(node)) {
      // Does not create new block for union all node, delay the creation operation to its inputs.
      val createNewBlockForChildren = if (isBreakPoint) true else createNewBlock
      node.getInputs.foreach(child => buildBlock(child, currentBlock, createNewBlockForChildren))
    } else {
      if (createNewBlock || isBreakPoint) {
        val childBlock = node2Block.getOrElseUpdate(node, new RelNodeBlock(node, tEnv))
        currentBlock.addChild(childBlock)
        node.getInputs.foreach(child => buildBlock(child, childBlock, createNewBlock = false))
      } else {
        node.getInputs.foreach(child => buildBlock(child, currentBlock, createNewBlock = false))
      }
    }
  }

  private def isUnionAllNode(node: RelNode): Boolean = node match {
    case unionRel: org.apache.calcite.rel.core.Union => unionRel.all
    case _ => false
  }

  private def buildBlockOutputNodes(sinks: Seq[RelNode]): Unit = {
    // init sink block output node
    sinks.foreach(sink => node2Wrapper.get(sink).addBlockOutputNode(sink))

    val unvisitedNodeQueue: util.Deque[RelNode] = new util.ArrayDeque[RelNode]()
    unvisitedNodeQueue.addAll(sinks)
    while (unvisitedNodeQueue.nonEmpty) {
      val node = unvisitedNodeQueue.removeFirst()
      val wrapper = node2Wrapper.get(node)
      require(wrapper != null)
      val blockOutputNode = wrapper.getBlockOutputNode
      buildBlockOutputNodes(None, node, blockOutputNode, unvisitedNodeQueue)
    }
  }

  private def buildBlockOutputNodes(
      parent: Option[RelNode],
      node: RelNode,
      curBlockOutputNode: RelNode,
      unvisitedNodeQueue: util.Deque[RelNode]): Unit = {
    val wrapper = node2Wrapper.get(node)
    require(wrapper != null)
    wrapper.addBlockOutputNode(curBlockOutputNode)
    wrapper.addVisitedParentNode(parent)

    // the node can be visited only when its all parent nodes have been visited
    if (wrapper.allParentNodesVisited) {
      val newBlockOutputNode = if (wrapper.hasMultipleBlockOutputNodes) {
        // if the node has different output node, the node is the output node of current block.
        node
      } else {
        curBlockOutputNode
      }
      node.getInputs.foreach { input =>
        buildBlockOutputNodes(Some(node), input, newBlockOutputNode, unvisitedNodeQueue)
      }
      unvisitedNodeQueue.remove(node)
    } else {
      // visit later
      unvisitedNodeQueue.addLast(node)
    }
  }

}

object RelNodeBlockPlanBuilder {

  /**
    * Decompose the [[LogicalNode]] trees into [[RelNodeBlock]] trees. First, convert LogicalNode
    * trees to RelNode trees. Second, reuse same sub-plan in different trees. Third, decompose the
    * RelNode dag to [[RelNodeBlock]] trees.
    *
    * @param  sinkNodes SinkNodes belongs to a LogicalNode plan.
    * @return Sink-RelNodeBlocks, each Sink-RelNodeBlock is a tree.
    */
  def buildRelNodeBlockPlan(
      sinkNodes: Seq[LogicalNode],
      tEnv: TableEnvironment): Seq[RelNodeBlock] = {

    // checks sink node
    sinkNodes.foreach {
      case _: SinkNode => // do nothing
      case o => throw new TableException(s"Error node: $o, Only SinkNode is supported.")
    }
    // convert LogicalNode tree to RelNode tree
    val relNodeTrees = sinkNodes.map(_.toRelNode(tEnv.getRelBuilder))
    // merge RelNode tree to RelNode dag
    val relNodeDag = reuseRelNodes(relNodeTrees)
    val builder = new RelNodeBlockPlanBuilder(tEnv)
    builder.buildRelNodeBlockPlan(relNodeDag)
  }

  /**
    * Reuse common subPlan in different RelNode tree, generate a RelNode dag
    *
    * @param relNodes RelNode trees
    * @return RelNode dag which reuse common subPlan in each tree
    */
  private def reuseRelNodes(relNodes: Seq[RelNode]): Seq[RelNode] = {

    class ExpandTableScanShuttle extends RelShuttleImpl {

      /**
        * Converts [[LogicalTableScan]] the result [[RelNode]] tree by calling [[RelTable]]#toRel
        */
      override def visit(scan: TableScan): RelNode = {

        scan match {
          case scan: LogicalTableScan =>
            val relTable = scan.getTable.unwrap(classOf[RelTable])
            if (relTable != null) {
              val relNode = relTable.toRel(RelOptUtil.getContext(scan.getCluster), scan.getTable)
              relNode.accept(this)
            } else {
              scan
            }
          case _ => scan
        }
      }
    }
    // expand RelTable in TableScan
    val shuttle = new ExpandTableScanShuttle
    val convertedRelNodes = relNodes.map(_.accept(shuttle))
    // reuse subPlan with same digest in input RelNode trees
    val context = new SubplanReuseContext(false, convertedRelNodes: _*)
    val reuseShuttle = new SubplanReuseShuttle(context)
    convertedRelNodes.map(_.accept(reuseShuttle))
  }

}

