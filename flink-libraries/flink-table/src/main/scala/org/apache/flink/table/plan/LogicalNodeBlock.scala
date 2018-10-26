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

package org.apache.flink.table.plan

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.core.{CorrelationId, TableScan}
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalJoin, LogicalTableFunctionScan, LogicalTableScan}
import org.apache.calcite.rex.{RexCall, RexFieldAccess, RexInputRef, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.schema.{RelTable, RowSchema}
import org.apache.flink.table.util.ComplexDimTVF
import org.apache.flink.util.Preconditions

import scala.collection.JavaConversions._
import scala.collection.mutable

object LogicalNodeBlockPlanBuilder {

  /**
    * Decompose the [[LogicalNode]] plan into many [[LogicalNodeBlock]]s,
    * and rebuild [[LogicalNodeBlock]] plan.
    *
    * @param sinkNodes SinkNodes belongs to a some LogicalNode plan.
    * @return Sink-LogicalNodeBlocks, each Sink-LogicalNodeBlock is a tree.
    */
  def buildLogicalNodeBlockPlan(
      sinkNodes: Seq[LogicalNode],
      tEnv: TableEnvironment): Seq[LogicalNodeBlock] = {

    // checks sink node
    sinkNodes.foreach {
      case _: SinkNode => // do nothing
      case o => throw TableException(s"Error node: $o, Only SinkNode is supported.")
    }
    val builder = new LogicalNodeBlockPlanBuilder(tEnv)
    builder.buildLogicalNodeBlockPlan(sinkNodes)
  }

  /**
    * Builds [[LogicalNodeBlock]] plan
    */
  private class LogicalNodeBlockPlanBuilder(tEnv: TableEnvironment) {
    private val node2Wrapper = new java.util.IdentityHashMap[LogicalNode, LogicalNodeWrapper]()
    private val node2Block = new java.util.IdentityHashMap[LogicalNode, LogicalNodeBlock]()
    val relNodeWrapperVisitor = new RelNodeWrapperVisitor
    val relNodeBlockBuilder = new RelNodeBlockBuilder(relNodeWrapperVisitor, tEnv)

    def buildLogicalNodeBlockPlan(sinkNodes: Seq[LogicalNode]): Seq[LogicalNodeBlock] = {
      sinkNodes.foreach {
        s => buildNodeWrappers(s, None)
      }

      sinkNodes.map {
        s => buildBlockPlan(s, None)
      }
    }

    private def buildNodeWrappers(node: LogicalNode, parent: Option[LogicalNode]): Unit = {
      node2Wrapper.getOrElseUpdate(node, new LogicalNodeWrapper(node)).addParentNode(parent)
      node match {
        case LogicalRelNode(rel) =>
          relNodeWrapperVisitor.logicalNodeParent = parent
          relNodeWrapperVisitor.go(rel)
        case _ =>
          node.children.foreach(c => buildNodeWrappers(c, Some(node)))
      }
    }

    /**
      * Traverses recursively from sink node to source node. Creates a new [[LogicalNodeBlock]]
      * under the following circumstances:
      * 1. meets a [[LogicalNode]] which is a [[SinkNode]].
      * 2. meets a [[LogicalNode]] which has more than one parent nodes. Note: if current node is
      *    union all node, but unionAll is forbidden as break point based on config,
      *    try to create a new [[LogicalNodeBlock]] for it's children instead of
      *    create a new [[LogicalNodeBlock]] for current union node.
      */
    private def buildBlockPlan(
        node: LogicalNode,
        block: Option[LogicalNodeBlock],
        createNewBlockForChildren: Boolean = false): LogicalNodeBlock = {

      val currentBlock = block match {
        case Some(b) => b
        case None => node2Block.getOrElseUpdate(node, new LogicalNodeBlock(node, tEnv))
      }

      val forbidUnionAllAsBreakPoint = tEnv.config
                                       .isForbidUnionAllAsBreakPointInSubsectionOptimization

      node match {
        case LogicalRelNode(rel) =>
          relNodeBlockBuilder.buildBlock(
            rel,
            currentBlock,
            createNewBlock = false,
            createNewBlockForChildren = createNewBlockForChildren)
        case _ =>
          node
          .children
          .foreach(child => {
            val childHasMultipleParents = node2Wrapper(child).hasMultipleParents
            if (isUnionAllNode(child) && forbidUnionAllAsBreakPoint) {
              val createNewBlockForChildrenOfChild = if (childHasMultipleParents) {
                true
              } else {
                createNewBlockForChildren
              }
              buildBlockPlan(child, Some(currentBlock), createNewBlockForChildrenOfChild)
            } else {
              if (createNewBlockForChildren || childHasMultipleParents) {
                val childBlock = node2Block.getOrElseUpdate(
                  child,
                  new LogicalNodeBlock(child, tEnv)
                )
                currentBlock.addChild(childBlock)
                buildBlockPlan(child, Some(childBlock))
              } else {
                buildBlockPlan(child, Some(currentBlock))
              }
            }
          })
      }
      currentBlock
    }

    private def isUnionAllNode(node: LogicalNode): Boolean = {
      node match {
        case LogicalRelNode(rel) => rel match {
          case unionRel: org.apache.calcite.rel.core.Union => unionRel.all
          case _ => false
        }
        case union: Union => union.all
        case _ => false
      }
    }
  }

  /**
    * Holds the parent (output) nodes of a [[LogicalNode]].
    */
  class LogicalNodeWrapper(logicalNode: LogicalNode) {
    private val parentNodes = new java.util.IdentityHashMap[LogicalNode, Boolean]()

    def addParentNode(parent: Option[LogicalNode]): Unit = {
      parent match {
        case Some(p) => parentNodes.put(p, true)
        case None => // do nothing
      }
    }

    def hasMultipleParents: Boolean = parentNodes.size > 1
  }

  private class RelNodeWrapper(relNode: RelNode) {
    var parentNodes: mutable.ArrayBuffer[Any] = new mutable.ArrayBuffer[Any]()

    def addParentNode(parent: Any): Unit = {
      if (parent != null && !parentNodes.contains(parent)) {
        parentNodes += parent
      }
    }

    def hasMultipleParents: Boolean = parentNodes.size > 1
  }

  private class RelNodeWrapperVisitor extends RelVisitor {
    val wrappers: mutable.ArrayBuffer[(RelNode, RelNodeWrapper)] =
      new mutable.ArrayBuffer[(RelNode, RelNodeWrapper)]()
    var logicalNodeParent: Option[LogicalNode] = None

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case scan: LogicalTableScan =>
          val relTable = scan.getTable.unwrap(classOf[RelTable])
          if (relTable != null) {
            val relNode = relTable.toRel(RelOptUtil.getContext(scan.getCluster), scan.getTable)
            getOrCreate(relNode).addParentNode(parent)
            super.visit(relNode, ordinal, parent)
          } else {
            getOrCreate(node).addParentNode(parent)
          }
        case _ =>
          getOrCreate(node).addParentNode(parent)
          super.visit(node, ordinal, parent)
      }
    }

    def getOrCreate(node: RelNode): RelNodeWrapper = {
      val wrapperOpt = get(node)
      val wrapper = if (wrapperOpt.isDefined) {
        wrapperOpt.get
      } else {
        val newWrapper: RelNodeWrapper = new RelNodeWrapper(node)
        wrappers += ((node, newWrapper))
        newWrapper
      }
      if (logicalNodeParent.isDefined) {
        wrapper.addParentNode(logicalNodeParent.get)
        logicalNodeParent = None
      }
      wrapper
    }

    def get(node: RelNode): Option[RelNodeWrapper] = {
      for ((rel, wrapper) <- wrappers) {
        if (rel.equals(node)) {
          return Some(wrapper)
        }
      }
      None
    }
  }

  private class RelNodeBlockBuilder(
      relNodeWrapperVisitor: RelNodeWrapperVisitor,
      tEnv: TableEnvironment) {

    val blocks: mutable.ArrayBuffer[(RelNode, LogicalNodeBlock)] =
      new mutable.ArrayBuffer[(RelNode, LogicalNodeBlock)]()

    val forbidUnionAllAsBreakPoint = tEnv.config
                                     .isForbidUnionAllAsBreakPointInSubsectionOptimization

    def buildBlock(
      node: RelNode,
      currentBlock: LogicalNodeBlock,
      createNewBlock: Boolean = false,
      createNewBlockForChildren: Boolean = false): Unit = {
      val relNode = convertToRelNode(node)
      val wrapper = relNodeWrapperVisitor.get(relNode)
      val hasMultipleParents = wrapper.isDefined && wrapper.get.hasMultipleParents
      if (isUnionAllNode(relNode) && forbidUnionAllAsBreakPoint) {
        // Does not create new block for union all node, delay the creation operation to its inputs.
        val createNewBlockForUnionChildren = if (hasMultipleParents) {
          true
        } else {
          createNewBlockForChildren || createNewBlock
        }
        visitChildren(relNode, currentBlock, createNewBlockForUnionChildren)
      } else {
        if (createNewBlock || hasMultipleParents) {
          val childBlock = getOrCreate(relNode)
          currentBlock.addChild(childBlock)
          visitChildren(relNode, childBlock, createNewBlockForChildren)
        } else {
          visitChildren(relNode, currentBlock, createNewBlockForChildren)
        }
      }
    }

    def visitChildren(
        node: RelNode,
        currentBlock: LogicalNodeBlock,
        createNewBlockForChildren: Boolean = false): Unit = {
      val relNode = convertToRelNode(node)
      relNode match {
        case s: SingleRel =>
          buildBlock(s.getInput, currentBlock, createNewBlockForChildren)
        case b: BiRel =>
          buildBlock(b.getLeft, currentBlock, createNewBlockForChildren)
          buildBlock(b.getRight, currentBlock, createNewBlockForChildren)
        case _ =>
          relNode.getInputs.foreach(buildBlock(_, currentBlock, createNewBlockForChildren))
      }
    }

    private def convertToRelNode(node: RelNode): RelNode = {
      node match {
        case scan: LogicalTableScan =>
          val relTable = scan.getTable.unwrap(classOf[RelTable])
          if (relTable != null) {
            relTable.toRel(RelOptUtil.getContext(scan.getCluster), scan.getTable)
          } else {
            scan
          }
        case _ =>
          node
      }
    }

    def getOrCreate(node: RelNode): LogicalNodeBlock = {
      for ((rel, block) <- blocks) {
        if (rel.equals(node)) {
          return block
        }
      }
      val block: LogicalNodeBlock = new LogicalNodeBlock(LogicalRelNode(node), tEnv)
      blocks += ((node, block))
      block
    }

    private def isUnionAllNode(node: RelNode): Boolean = node match {
      case unionRel: org.apache.calcite.rel.core.Union => unionRel.all
      case _ => false
    }
  }

}

/**
  * A [[LogicalNodeBlock]] is a sub-tree in the [[LogicalNode]] plan. All [[LogicalNode]]s in
  * each block have at most one parent (output) node.
  * The nodes in different block will be optimized independently.
  *
  * For example: (Table API)
  *
  * {{{-
  *  val table = tEnv.scan("test_table").select('a, 'b, 'c)
  *  table.where('a >= 70).select('a, 'b).writeToSink(sink1)
  *  table.where('a < 70 ).select('a, 'c).writeToSink(sink2)
  * }}}
  *
  * the LogicalNode plan is:
  *
  * {{{-
  *        CatalogNode
  *            |
  *       Project(a,b,c)
  *        /          \
  * Filter(a>=70)  Filter(a<70)
  *     |              |
  * Project(a,b)  Project(a,c)
  *     |              |
  * SinkNode(sink1) SinkNode(sink2)
  * }}}
  *
  * This [[LogicalNode]] plan will be decomposed into three [[LogicalNodeBlock]]s, the break-point
  * is a [[LogicalNode]] which has more than one parent nodes.
  * the first [[LogicalNodeBlock]] includes CatalogNode and Project('a,'b,'c)
  * the second one includes Filter('a>=70), Project('a,'b) and SinkNode(sink1)
  * the third one includes Filter('a<70), Project('a,'c), SinkNode(sink2)
  * And the first [[LogicalNodeBlock]] is the child of another two.
  * The [[LogicalNodeBlock]] plan is:
  *
  * {{{-
  *         LogicalNodeBlock1
  *          /            \
  * LogicalNodeBlock2  LogicalNodeBlock3
  * }}}
  *
  * The optimizing order is from child block to parent. The optimized result (DataSet or DataStream)
  * will be registered into tables first, and then be converted to a new CatalogNode which is the
  * new output node of current block and is also the input of its parent blocks.
  *
  * @param outputNode A LogicalNode of the output in the block, which could be a [[SinkNode]] or
  *                   a LogicalNode with more than one parent nodes.
  */
class LogicalNodeBlock(val outputNode: LogicalNode, tEnv: TableEnvironment) {
  // child (or input) blocks
  private val childBlocks = mutable.LinkedHashSet[LogicalNodeBlock]()

  // After this block has been optimized, the result will be converted to a new CatalogNode as
  // new output node
  private var newOutputNode: Option[LogicalNode] = None

  private var outputTableName: Option[String] = None

  private var optimizedPlan: Option[RelNode] = None

  private var updateAsRetract: Boolean = false

  def addChild(block: LogicalNodeBlock): Unit = childBlocks += block

  def children: Seq[LogicalNodeBlock] = childBlocks.toSeq

  def setNewOutputNode(newNode: LogicalNode): Unit = newOutputNode = Option(newNode)

  def getNewOutputNode: Option[LogicalNode] = newOutputNode

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

  /** propagate need-retraction to all input blocks */
  def propagateUpdateAsRetraction(): Unit = {
    if (updateAsRetract) {
      children.foreach { child =>
        child.setUpdateAsRetraction(true)
        child.propagateUpdateAsRetraction()
      }
    }
  }

  def isChildBlockOutputNode(node: LogicalNode): Option[LogicalNodeBlock] = {
    val find = children.filter(_.outputNode eq node)
    if (find.isEmpty) {
      None
    } else {
      Preconditions.checkArgument(find.size == 1)
      Some(find.head)
    }
  }

  def isChildBlockOutputRelNode(node: RelNode): Option[LogicalNodeBlock] = {
    val find = children.filter(_.outputNode match {
      case LogicalRelNode(rel) if rel.equals(node) => true
      case _ => false
    })
    if (find.isEmpty) {
      None
    } else {
      Preconditions.checkArgument(find.size == 1)
      Some(find.head)
    }
  }

  /**
    * Get new logical node plan of this block. The child blocks (inputs) will be replace with
    * new CatalogNodes (the optimized result of child block).
    *
    * @return New LogicalNode plan of this block
    */
  def getLogicalPlan: LogicalNode = {

    val shuttle = new RelNodeBlockShuttle

    def createNewLogicalPlan(node: LogicalNode): LogicalNode = {
      node match {
        case LogicalRelNode(rel) =>
          val newRel = rel.accept(shuttle)
          LogicalRelNode(newRel)
        case _ =>
          val newChildren = node.children.map {
            n =>
              val block = isChildBlockOutputNode(n)
              block match {
                case Some(b) => b.getNewOutputNode.get
                case None => createNewLogicalPlan(n)
              }
          }
          cloneLogicalNode(node, newChildren)
      }
    }

    createNewLogicalPlan(outputNode)
  }

  private def cloneLogicalNode(node: LogicalNode, children: Seq[LogicalNode]): LogicalNode = {
    node match {
      case CatalogNode(tableName, rowType) => CatalogNode(tableName, rowType)
      case LogicalRelNode(relNode) => LogicalRelNode(relNode)
      case l: LogicalTableFunctionCall => l
      case u: UnaryNode =>
        Preconditions.checkArgument(children.length == 1)
        val child = children.head
        u match {
          case Filter(condition, _) => Filter(condition, child)
          case Aggregate(grouping, aggregate, _) => Aggregate(grouping, aggregate, child)
          case Limit(offset, fetch, _) => Limit(offset, fetch, child)
          case WindowAggregate(grouping, window, property, aggregate, _) =>
            WindowAggregate(grouping, window, property, aggregate, child)
          case Distinct(_) => Distinct(child)
          case SinkNode(_, sink) => SinkNode(child, sink)
          case AliasNode(aliasList, _) => AliasNode(aliasList, child)
          case Project(projectList, _) => Project(projectList, child)
          case Sort(order, _) => Sort(order, child)
          case LogicalTableValuedAggregateCall(call, groupKey, _) =>
            LogicalTableValuedAggregateCall(call, groupKey, child)
          case _ => throw TableException(s"Unsupported UnaryNode node: $node")
        }
      case b: BinaryNode =>
        Preconditions.checkArgument(children.length == 2)
        val left = children.head
        val right = children.last
        b match {
          case Join(_, _, joinType, condition, correlated) =>
            Join(left, right, joinType, condition, correlated)
          case Union(_, _, all) => Union(left, right, all)
          case Intersect(_, _, all) => Intersect(left, right, all)
          case Minus(_, _, all) => Minus(left, right, all)
          case _ => throw TableException(s"Unsupported BinaryNode node: $node")
        }
      case _ => throw TableException(s"Unsupported LogicalNode node: $node")
    }
  }

  private class RelNodeBlockShuttle extends RelShuttleImpl {

    override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
      val block = isChildBlockOutputRelNode(parent)
      if (block.isDefined) {
        val logicalNode = block.get.getNewOutputNode.get
        logicalNode.toRelNode(tEnv.getRelBuilder)
      } else {
        super.visitChild(parent, i, child)
      }
    }

    override def visitChildren(rel: RelNode): RelNode = {
      val block = isChildBlockOutputRelNode(rel)
      if (block.isDefined) {
        val logicalNode = block.get.getNewOutputNode.get
        logicalNode.toRelNode(tEnv.getRelBuilder)
      } else {
        super.visitChildren(rel)
      }
    }

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

    /**
      * TODO if right input is a ComplexDimTVF then rewrite this correlate to a LogicalJoin
      * with a new right input of DimensionTable for SQLGen scenario. It's a temporary solution
      * and will'be replaced later.
      */
    override def visit(correlate: LogicalCorrelate): RelNode = {
      correlate.getRight match {
        case scan: LogicalTableFunctionScan =>
          val tableFunction = scan.getCall.asInstanceOf[RexCall].op.asInstanceOf[TableSqlFunction]
            .getTableFunction
          tableFunction match {
            case complexDimTVF: ComplexDimTVF =>
              val uniqueId = complexDimTVF.dimTable.hashCode() & 0xfffffff
              val newTableName = s"${complexDimTVF.dimTable.explainSource()}_$uniqueId"
              if (tEnv.getTable(newTableName).isEmpty) {
                tEnv.registerTableSource(newTableName, complexDimTVF.dimTable)
              }

              val leftKey = correlate.getRight.asInstanceOf[LogicalTableFunctionScan]
                .getCall.asInstanceOf[RexCall].operands.get(0)
                .asInstanceOf[RexFieldAccess].getField.getName
              val leftRowType = new RowSchema(correlate.getLeft.getRowType)
              val leftKeyIndex = leftRowType.fieldNames.zipWithIndex
                .find(_._1 == leftKey).getOrElse(
                throw new ValidationException(s"could find key $leftKey from left input"))._2

              // construct a LogicTableScan
              val newRight = tEnv.getRelBuilder.scan(newTableName).peek()
              val rightKeyIndex = new RowSchema(newRight.getRowType)
                .fieldNames.zipWithIndex
                .find(_._1 == complexDimTVF.joinKey).getOrElse(
                throw new ValidationException(
                  s"could find key ${complexDimTVF.joinKey} " +
                    s"from right input"))._2 + leftRowType.arity

              val operands4Condition: ImmutableList[RexNode] = ImmutableList.of(
                RexInputRef.of(leftKeyIndex, correlate.getLeft.getRowType),
                RexInputRef.of(rightKeyIndex, correlate.getRowType))

              val joinCondition = tEnv.getRelBuilder.getRexBuilder.makeCall(
                tEnv.getTypeFactory.createSqlType(SqlTypeName.BOOLEAN),
                SqlStdOperatorTable.EQUALS,
                operands4Condition)

              // rewrite current correlate to join
              val join: LogicalJoin = LogicalJoin.create(
                correlate.getLeft,
                newRight,
                joinCondition,
                ImmutableSet.of[CorrelationId](),
                correlate.getJoinType.toJoinType,
                false,
                ImmutableList.of[RelDataTypeField]())

              // visit new join node
              super.visit(join)
            case _ =>
              super.visit(correlate)
          }
        case _ => super.visit(correlate)
      }
    }
  }

}
