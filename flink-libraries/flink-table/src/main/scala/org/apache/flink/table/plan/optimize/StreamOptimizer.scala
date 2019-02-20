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

import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.{FlinkChainContext, FlinkTypeFactory}
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecIntermediateTableScan, StreamPhysicalRel}
import org.apache.flink.table.plan.optimize.program.{FlinkStreamPrograms, StreamOptimizeContext}
import org.apache.flink.table.plan.schema.IntermediateRelNodeTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.SameRelObjectShuttle
import org.apache.flink.table.sinks.BaseRetractStreamTableSink
import org.apache.flink.util.Preconditions
import org.apache.calcite.plan.{Context, Contexts, RelOptPlanner}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexBuilder
import java.util

import scala.collection.JavaConversions._

/**
  * Query optimizer for Stream.
  */
class StreamOptimizer(tEnv: StreamTableEnvironment) extends AbstractOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val sinkBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, tEnv)
    // infer updateAsRetraction property for sink block
    sinkBlocks.foreach { sinkBlock =>
      val retractionFromRoot = sinkBlock.outputNode match {
        case n: Sink =>
          n.sink.isInstanceOf[BaseRetractStreamTableSink[_]]
        case o =>
          o.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE).sendsUpdatesAsRetractions
      }
      sinkBlock.setUpdateAsRetraction(retractionFromRoot)
    }

    if (sinkBlocks.size == 1) {
      // If there is only one sink block, the given relational expressions are a simple tree
      // (only one root), not a dag. So many operations (e.g. `infer updateAsRetraction property`,
      // `propagate updateAsRetraction property`) can be omitted to save optimization time.
      val block = sinkBlocks.head
      val optimizedTree = optimizeTree(
        block.getPlan,
        block.isUpdateAsRetraction,
        isSinkBlock = true)
      block.setOptimizedPlan(optimizedTree)
      return sinkBlocks
    }

    // infer updateAsRetraction property for all input blocks
    sinkBlocks.foreach(b => inferUpdateAsRetraction(b, b.isUpdateAsRetraction, isSinkBlock = true))
    // propagate updateAsRetraction property to all input blocks
    sinkBlocks.foreach(propagateUpdateAsRetraction)
    // clear the intermediate result
    sinkBlocks.foreach(resetIntermediateResult)
    // optimize recursively RelNodeBlock
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    sinkBlocks
  }

  private def optimizeBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child, isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case s: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree(
          s,
          updatesAsRetraction = block.isUpdateAsRetraction,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree(
          o,
          updatesAsRetraction = block.isUpdateAsRetraction,
          isSinkBlock = isSinkBlock)
        val isAccRetract = optimizedPlan.getTraitSet
          .getTrait(AccModeTraitDef.INSTANCE).getAccMode == AccMode.AccRetract
        val rowType = optimizedPlan.getRowType
        val fieldExpressions = getExprsWithTimeAttribute(o.getRowType, rowType)
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedPlan, isAccRetract, fieldExpressions)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param updatesAsRetraction True if request updates as retraction messages.
    * @param isSinkBlock True if the given block is sink block.
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(
      relNode: RelNode,
      updatesAsRetraction: Boolean,
      isSinkBlock: Boolean): RelNode = {

    val config = tEnv.getConfig
    val programs = config.getCalciteConfig.getStreamPrograms
      .getOrElse(FlinkStreamPrograms.buildPrograms(config.getConf))
    Preconditions.checkNotNull(programs)

    val optimizeNode = programs.optimize(relNode, new StreamOptimizeContext() {
      override def getContext: Context = FlinkChainContext.chain(
        tEnv.getFrameworkConfig.getContext, Contexts.of(tEnv.getFlinkPlanner))

      override def getRelOptPlanner: RelOptPlanner = tEnv.getPlanner

      override def getRexBuilder: RexBuilder = tEnv.getRelBuilder.getRexBuilder

      override def updateAsRetraction: Boolean = updatesAsRetraction

      override def needFinalTimeIndicatorConversion: Boolean = isSinkBlock
    })

    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    optimizeNode.accept(new SameRelObjectShuttle())
  }

  /**
    * Infer UpdateAsRetraction property for each block.
    * NOTES: this method should not change the original RelNode tree.
    *
    * @param block              The [[RelNodeBlock]] instance.
    * @param retractionFromRoot Whether the sink need update as retraction messages.
    * @param isSinkBlock        True if the given block is sink block.
    */
  private def inferUpdateAsRetraction(
      block: RelNodeBlock,
      retractionFromRoot: Boolean,
      isSinkBlock: Boolean): Unit = {

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          inferUpdateAsRetraction(child, retractionFromRoot = false, isSinkBlock=false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case n: Sink =>
        require(isSinkBlock)
        val optimizedPlan = optimizeTree(n, retractionFromRoot, isSinkBlock = true)
        block.setOptimizedPlan(optimizedPlan)

      case o =>
        val optimizedPlan = optimizeTree(o, retractionFromRoot, isSinkBlock = isSinkBlock)
        val rowType = optimizedPlan.getRowType
        val fieldExpressions = getExprsWithTimeAttribute(o.getRowType, rowType)
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedPlan, isAccRetract = false, fieldExpressions)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.getRelNode)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Propagate updateAsRetraction property to all input blocks
    *
    * @param block The [[RelNodeBlock]] instance.
    */
  private def propagateUpdateAsRetraction(block: RelNodeBlock): Unit = {

    // process current block
    def shipUpdateAsRetraction(rel: RelNode, updateAsRetraction: Boolean): Unit = {
      rel match {
        case _: StreamExecDataStreamScan | _: StreamExecIntermediateTableScan =>
          val scan = rel.asInstanceOf[TableScan]
          val retractionTrait = scan.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
          if (retractionTrait.sendsUpdatesAsRetractions || updateAsRetraction) {
            val tableName = scan.getTable.getQualifiedName.last
            val retractionBlocks = block.children.filter(_.getOutputTableName eq tableName)
            Preconditions.checkArgument(retractionBlocks.size <= 1)
            if (retractionBlocks.size == 1) {
              retractionBlocks.head.setUpdateAsRetraction(true)
            }
          }
        case ser: StreamPhysicalRel => ser.getInputs.foreach { e =>
          if (ser.needsUpdatesAsRetraction(e) || (updateAsRetraction && !ser.consumesRetractions)) {
            shipUpdateAsRetraction(e, updateAsRetraction = true)
          } else {
            shipUpdateAsRetraction(e, updateAsRetraction = false)
          }
        }
      }
    }

    shipUpdateAsRetraction(block.getOptimizedPlan, block.isUpdateAsRetraction)
    block.children.foreach(propagateUpdateAsRetraction)
  }


  /**
    * Reset the intermediate result including newOutputNode and outputTableName
    *
    * @param block the [[RelNodeBlock]] instance.
    */
  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

  private def registerIntermediateTable(
      tEnv: StreamTableEnvironment,
      name: String,
      relNode: RelNode,
      isAccRetract: Boolean,
      fields: Array[Expression]): Unit = {
    val uniqueKeys = getUniqueKeys(tEnv, relNode)
    val monotonicity = FlinkRelMetadataQuery
      .reuseOrCreate(tEnv.getRelBuilder.getCluster.getMetadataQuery)
      .getRelModifiedMonotonicity(relNode)
    val statistic = FlinkStatistic.builder()
      .uniqueKeys(uniqueKeys)
      .monotonicity(monotonicity)
      .build()

    val table = new IntermediateRelNodeTable(
      relNode,
      isAccRetract,
      statistic)
    tEnv.registerTableInternal(name, table)
  }

  /**
    * Mark Expression to RowtimeAttribute or ProctimeAttribute for time indicators
    */
  private def getExprsWithTimeAttribute(
      preRowType: RelDataType,
      postRowType: RelDataType): Array[Expression] = {

    preRowType.getFieldNames.zipWithIndex.map {
      case (name, index) =>
        val field = postRowType.getFieldList.get(index)
        val relType = field.getValue
        val relName = field.getName
        val expression = UnresolvedFieldReference(relName)

        relType match {
          case _ if FlinkTypeFactory.isProctimeIndicatorType(relType) =>
            ProctimeAttribute(expression)
          case _ if FlinkTypeFactory.isRowtimeIndicatorType(relType) => RowtimeAttribute(expression)
          case _ if !relName.equals(name) => Alias(expression, name)
          case _ => expression
        }
    }.toArray[Expression]
  }

  private def getUniqueKeys(
      tEnv: StreamTableEnvironment,
      relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(tEnv.getRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.filter(_.nonEmpty).map { uniqueKey =>
        val keys = new util.HashSet[String]()
        uniqueKey.asList().foreach { idx =>
          keys.add(rowType.getFieldNames.get(idx))
        }
        keys
      }
    } else {
      null
    }
  }

}
