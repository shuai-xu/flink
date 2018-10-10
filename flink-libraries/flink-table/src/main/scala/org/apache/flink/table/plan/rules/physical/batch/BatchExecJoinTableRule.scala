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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinTable
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.BaseRowSchema

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class BatchExecJoinTableRule extends ConverterRule(
    classOf[FlinkLogicalJoinTable],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCHEXEC,
    "BatchExecJoinTableRule") {

  def convert(rel: RelNode): RelNode = {
    val join: FlinkLogicalJoinTable = rel.asInstanceOf[FlinkLogicalJoinTable]
    val cluster = join.getCluster
    val tableSource = join.tableSource
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val tableSourceRowType = flinkTypeFactory.buildLogicalRowType(tableSource.getTableSchema, false)
    var requiredTraitSet = rel.getTraitSet.replace(FlinkConventions.BATCHEXEC)

    // if partitioning enabled, use the join key as partition key
    tableSource match {
      case partitionSource: DefinedDistribution if partitionSource.getPartitionField() != null &&
          join.lookupKeyPairs.nonEmpty =>
        requiredTraitSet = requiredTraitSet.plus(
          FlinkRelDistribution.hash(join.lookupKeyPairs.map(pair => Integer.valueOf(pair.source))))
      case _ =>
    }

    val convInput = RelOptRule.convert(join.getInput, requiredTraitSet)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.BATCHEXEC)

    new BatchExecJoinTable(
      cluster,
      providedTraitSet,
      new BaseRowSchema(convInput.getRowType),
      convInput,
      join.tableSource,
      new BaseRowSchema(tableSourceRowType),
      join.calcProgram,
      join.period,
      join.lookupKeyPairs.asJava,
      join.constantLookupKeys,
      join.newJoinRemainingCondition,
      join.checkedIndex.get,
      new BaseRowSchema(join.getRowType),
      join.joinInfo,
      join.joinType,
      description
    )
  }

}

object BatchExecJoinTableRule {
  val INSTANCE: RelOptRule = new BatchExecJoinTableRule()
}

