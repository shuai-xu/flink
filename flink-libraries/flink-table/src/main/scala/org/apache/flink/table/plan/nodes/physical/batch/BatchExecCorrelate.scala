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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexNode, RexProgram}
import org.apache.calcite.sql.SemiJoinType
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.nodes.common.CommonCorrelate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.dataformat.BaseRow

/**
  * Flink RelNode which matches along with join a user defined table function.
  */
class BatchExecCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    projectProgram: Option[RexProgram],
    val scan: FlinkLogicalTableFunctionScan,
    val condition: Option[RexNode],
    relRowType: RelDataType,
    joinType: SemiJoinType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonCorrelate
  with RowBatchExecRel {

  override def deriveRowType(): RelDataType = relRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    copy(traitSet, inputs.get(0), projectProgram, relRowType)
  }

  /**
    * Note: do not passing member 'child' because singleRel.replaceInput may update 'input' rel.
    */
  def copy(
      traitSet: RelTraitSet,
      child: RelNode,
      projectProgram: Option[RexProgram],
      outputType: RelDataType): RelNode = {
     super.supplement(new BatchExecCorrelate(
       cluster,
       traitSet,
       child,
       projectProgram,
       scan,
       condition,
       outputType,
       joinType,
       ruleDescription))
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def toString: String = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    s"${
      correlateToString(
        input.getRowType,
        rexCall,
        sqlFunction,
        getExpressionString)
    } select(${selectToString(relRowType)})"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
        .item("invocation", scan.getCall)
        .item("correlate", correlateToString(
          inputNode.getRowType,
          rexCall, sqlFunction,
          getExpressionString))
        .item("select", selectToString(relRowType))
        .item("rowType", relRowType)
        .item("joinType", joinType)
        .itemIf("condition", condition.orNull, condition.isDefined)
        .itemIf("reuse_id", getReuseId, isReused)
  }

  /**
    * Internal method, translates the [[BatchExecRel]] node into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): StreamTransformation[BaseRow] = {
    val inputTransformation =
      getInput.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig)
    val operatorCtx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val transformation = generateCorrelateTransformation(
      tableEnv,
      operatorCtx,
      inputTransformation,
      inputNode.getRowType,
      projectProgram,
      scan,
      condition,
      relRowType,
      joinType,
      resultPartitionCount,
      retainHeader = false,
      getExpressionString,
      ruleDescription)
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }
}
