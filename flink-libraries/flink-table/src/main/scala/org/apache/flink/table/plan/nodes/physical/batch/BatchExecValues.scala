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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.nodes.common.CommonValue

import scala.collection.JavaConversions._

/**
  * StreamTransformation RelNode for a LogicalValues.
  */
class BatchExecValues(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  rowRelDataType: RelDataType,
  tuples: ImmutableList[ImmutableList[RexLiteral]],
  description: String)
  extends CommonValue(cluster, traitSet, rowRelDataType, tuples, description)
    with RowBatchExecRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    super.supplement(new BatchExecValues(
      cluster,
      traitSet,
      getRowType,
      getTuples,
      description
    ))
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def toString: String = {
    s"Values(values: (${getRowType.getFieldNames.toList.mkString(", ")}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("values", valuesFieldsToString)
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
    val inputFormat = generatorInputFormat(tableEnv)
    val transformation = tableEnv.streamEnv
      .createInput(inputFormat, inputFormat.getProducedType, description)
      .getTransformation
    transformation.setParallelismLocked(true)
    tableEnv.getRUKeeper().addTransformation(this, transformation)
    transformation.setResources(resource.getReservedResourceSpec, resource.getPreferResourceSpec)
    transformation
  }

  private def valuesFieldsToString: String = {
    getRowType.getFieldNames.toList.mkString(", ")
  }

}

