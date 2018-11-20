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
package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExpandCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.plan.nodes.calcite.Expand
import org.apache.flink.table.plan.util.ExpandUtil
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

class StreamExecExpand(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    outputRowType: RelDataType,
    projects: util.List[util.List[RexNode]],
    expandIdIndex: Int,
    ruleDescription: String)
  extends Expand(cluster, traitSet, input, outputRowType, projects, expandIdIndex)
  with StreamExecRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecExpand(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      projects,
      expandIdIndex,
      ruleDescription
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("projects", ExpandUtil.projectsToString(projects, input.getRowType, getRowType))
  }

  private def getOperatorName: String = {
    s"StreamExecExpand: ${rowType.getFieldList.map(_.getName).mkString(", ")}"
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val inputTransformation = getInput.asInstanceOf[StreamExecRel].translateToPlan(tableEnv)
    val inputType = DataTypes.internal(inputTransformation.getOutputType)
    val outputType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[GenericRow])

    val ctx = CodeGeneratorContext(config)
    val substituteStreamOperator = ExpandCodeGenerator.generateExpandOperator(
      ctx,
      inputType,
      outputType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      config,
      projects,
      ruleDescription,
      retainHeader = true)

    val transformation = new OneInputTransformation(
      inputTransformation,
      getOperatorName,
      substituteStreamOperator,
      outputType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      inputTransformation.getParallelism)
    transformation
  }
}
