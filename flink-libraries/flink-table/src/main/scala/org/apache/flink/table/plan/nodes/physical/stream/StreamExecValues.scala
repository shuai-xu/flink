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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.nodes.common.CommonValue
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.dataformat.BaseRow


/**
  * DataStream RelNode for LogicalValues.
  */
class StreamExecValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    outputSchema: BaseRowSchema,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
    description: String)
  extends CommonValue(cluster, traitSet, outputSchema.relDataType, tuples, description)
  with StreamExecRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecValues(
      cluster,
      traitSet,
      outputSchema,
      getTuples,
      description
    )
  }

  /**
    * Note: Override this method is very important because Calcite will compute relNode's digest
    * via this method and append a head name base on class name, w/o this overriding, digest header
    * will be the same parent class name, then may encounter 'weird' problems during optimization...
    */
  override def explainTerms(pw: RelWriter): RelWriter = super.explainTerms(pw)

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {
    if (queryConfig.isValuesSourceInputEnabled) {
      val inputFormat = generatorInputFormat(tableEnv)
      tableEnv.execEnv.createInput(inputFormat, inputFormat.getProducedType).getTransformation
    } else {
      // enable this feature when runtime support do checkpoint when source finished
      throw new TableException("Values source input is not supported currently. Probably " +
        "there is a where condition which always returns false in your query.")
    }
  }
}
