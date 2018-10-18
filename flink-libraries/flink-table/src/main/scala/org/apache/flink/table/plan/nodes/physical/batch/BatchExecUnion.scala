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

import java.util.{List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.streaming.api.transformations.{StreamTransformation, UnionTransformation}
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.BatchExecRelVisitor
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._

class BatchExecUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relList: JList[RelNode],
    rowRelDataType: RelDataType,
    all: Boolean)
  extends Union(cluster, traitSet, relList, all)
  with RowBatchExecRel {

  require(all, "Only support union all")

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {
    super.supplement(new BatchExecUnion(
      cluster,
      traitSet,
      inputs,
      rowRelDataType,
      all
    ))
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    // union will destroy collation trait. So does not push down collation requirement.
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val pushDownDistribution = requiredDistribution.getType match {
      case RANDOM_DISTRIBUTED | ROUND_ROBIN_DISTRIBUTED | BROADCAST_DISTRIBUTED =>
        requiredDistribution
      // apply strict hash distribution of each child to avoid inconsistent of shuffle of each child
      case HASH_DISTRIBUTED => FlinkRelDistribution.hash(requiredDistribution.getKeys)
      // range distribution cannot push down because partition's [lower, upper]  of each union child
      // may be different
      case RANGE_DISTRIBUTED => null
      // Singleton cannot push down. Singleton exchange limit the parallelism of later RelNode to 1.
      // Push down Singleton into input of union will destroy the limitation.
      case SINGLETON  => null
      // there is no need to push down Any distribution
      case  ANY => null
    }
    if (pushDownDistribution == null) {
      null
    } else {
      val relNodes = getInputs.map(RelOptRule.convert(_, pushDownDistribution))
      copy(getTraitSet.replace(pushDownDistribution), relNodes)
    }
  }

  override def accept[R](visitor: BatchExecRelVisitor[R]): R = visitor.visit(this)

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union", unionSelectionToString)
      .itemIf("reuse_id", getReuseId, isReused)
  }

  private def unionSelectionToString: String = {
    rowRelDataType.getFieldNames.mkString(", ")
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
    val transformations = getInputs.map(
      _.asInstanceOf[RowBatchExecRel].translateToPlan(tableEnv, queryConfig))
    new UnionTransformation(transformations)
  }
}
