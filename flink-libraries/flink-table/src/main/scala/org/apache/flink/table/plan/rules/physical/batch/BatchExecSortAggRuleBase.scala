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

import org.apache.calcite.plan.{RelOptRule, RelTraitSet, RelOptCluster, RelOptRuleCall}
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecSortAggregate, BatchExecLocalSortAggregate}
import org.apache.flink.table.types.{DataTypes, InternalType}

import scala.collection.JavaConversions._

trait BatchExecSortAggRuleBase extends BatchExecAggRuleBase {

  protected def generateSortAggPhysicalNode(
      call: RelOptRuleCall,
      cluster: RelOptCluster,
      aggNames: Seq[String],
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      aggregates: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[InternalType]],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggProvidedTraitSet: RelTraitSet,
      outputType: RelDataType):Unit = {

    val input = call.rels(1)
    if (isTwoPhaseAggWorkable(aggregates, call)) {
      val localAggRelType = inferLocalAggType(
        input.getRowType, cluster, aggNames, groupSet, auxGroupSet, aggregates,
        aggBufferTypes.map(_.map(DataTypes.internal)))
      //localSortAgg
      var localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCHEXEC)
      if (groupSet.nonEmpty) {
        localRequiredTraitSet = localRequiredTraitSet.replace(createRelCollation(groupSet))
      }
      val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
      val providedLocalTraitSet = localRequiredTraitSet
      val localSortAgg = new BatchExecLocalSortAggregate(
        cluster,
        call.builder(),
        providedLocalTraitSet,
        newLocalInput,
        aggCallToAggFunction,
        localAggRelType,
        newLocalInput.getRowType,
        groupSet,
        auxGroupSet)

      //globalSortAgg
      val globalDistributions = if (groupSet.nonEmpty) {
        // global agg should use groupSet's indices as distribution fields
        val globalGroupSet = groupSet.indices
        val distributionFields = globalGroupSet.map(Integer.valueOf)
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      globalDistributions.foreach { globalDistribution =>
        //replace the RelCollation with EMPTY
        var requiredTraitSet = localSortAgg.getTraitSet
          .replace(globalDistribution).replace(RelCollations.EMPTY)
        if (groupSet.nonEmpty) {
          requiredTraitSet = requiredTraitSet.replace(createRelCollation(groupSet.indices.toArray))
        }
        val newInputForFinalAgg = RelOptRule.convert(localSortAgg, requiredTraitSet)
        val globalSortAgg = new BatchExecSortAggregate(
          cluster,
          call.builder(),
          aggProvidedTraitSet,
          newInputForFinalAgg,
          aggCallToAggFunction,
          outputType,
          newInputForFinalAgg.getRowType,
          groupSet.indices.toArray,
          (groupSet.length until groupSet.length + auxGroupSet.length).toArray,
          isMerge = true)
        call.transformTo(globalSortAgg)
      }
    }

    if (isOnePhaseAggWorkable(
      cluster, ImmutableBitSet.of(groupSet: _*), input, aggregates, call)) {
      val requiredDistributions = if (groupSet.nonEmpty) {
        val distributionFields = groupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      requiredDistributions.foreach { requiredDistribution =>
        var requiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCHEXEC)
          .replace(requiredDistribution)
        if (groupSet.nonEmpty) {
          requiredTraitSet = requiredTraitSet.replace(createRelCollation(groupSet))
        }
        val newInput = RelOptRule.convert(input, requiredTraitSet)
        //transform aggregate physical node
        val sortAgg = new BatchExecSortAggregate(
          cluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          outputType,
          newInput.getRowType,
          groupSet,
          auxGroupSet,
          isMerge = false
        )
        call.transformTo(sortAgg)
      }
    }
  }
}
