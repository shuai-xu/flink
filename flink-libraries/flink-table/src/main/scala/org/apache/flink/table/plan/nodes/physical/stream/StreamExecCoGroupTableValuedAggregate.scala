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

import java.util.{List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataview.DataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.functions.utils.CoTableValuedAggSqlFunction
import org.apache.flink.table.plan.nodes.calcite.CoTableValuedAggregate
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.{AggregateInfo, AggregateInfoList, CoAggregateInfo, StreamExecUtil}
import org.apache.flink.table.runtime.BaseRowKeySelector
import org.apache.flink.table.runtime.aggregate.{CoGroupTableValuedAggFunction, NonDeterministicCoGroupTableValuedAggFunction}
import org.apache.flink.table.runtime.operator.KeyedCoProcessOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging

/**
  * Flink RelNode which matches along with connect.
  *
  */
class StreamExecCoGroupTableValuedAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    lRexCall: RexCall,
    rRexCall: RexCall,
    groupKey1: Seq[RexNode],
    groupKey2: Seq[RexNode])
  extends CoTableValuedAggregate(
    cluster,
    traitSet,
    left,
    right,
    lRexCall,
    rRexCall,
    lRexCall.getOperator.asInstanceOf[CoTableValuedAggSqlFunction].externalResultType,
    groupKey1,
    groupKey2)
  with StreamExecRel
  with Logging {

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesUpdates = true

  override def consumesRetractions = true

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new StreamExecCoGroupTableValuedAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      lRexCall,
      rRexCall,
      groupKey1,
      groupKey2
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy1",
        groupingToString(left.getRowType, groupKey1, 0), groupKey1.nonEmpty)
      .itemIf("groupBy2", groupingToString(
        right.getRowType, groupKey2, left.getRowType.getFieldCount), groupKey2.nonEmpty)
      .item("coAggApply",
        buildAggregationToString(
        left.getRowType,
        right.getRowType,
        lRexCall: RexCall,
        rRexCall: RexCall))
  }

  override def toString: String = {
    s"CoGroupTableValuedAggregate(${
      if (groupKey1.nonEmpty) {
        s"groupBy1: (${groupingToString(left.getRowType, groupKey1, 0)}), " +
          s"groupBy2: (${groupingToString(
            right.getRowType, groupKey2, left.getRowType.getFieldCount)})"
      } else {
        ""
      }
    }coAggApply:(${
      buildAggregationToString(
        left.getRowType,
        right.getRowType,
        lRexCall: RexCall,
        rRexCall: RexCall)
    }))"
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    if (queryConfig.getMinIdleStateRetentionTime < 0) {
      LOG.warn("No state retention interval configured for a query which accumulates state. " +
        "Please provide a query configuration with valid retention interval to prevent " +
        "excessive " +
        "state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val needRetraction = StreamExecRetractionRules.isAccRetract(left) ||
      StreamExecRetractionRules.isAccRetract(right)

    val input1Types =
      lRexCall.getOperands.toArray
        .map(n => FlinkTypeFactory.toInternalType(n.asInstanceOf[RexNode].getType))
    val input2Types =
      rRexCall.getOperands.toArray
        .map(n => FlinkTypeFactory.toInternalType(n.asInstanceOf[RexNode].getType))

    val sqlFunction: CoTableValuedAggSqlFunction =
      lRexCall.getOperator.asInstanceOf[CoTableValuedAggSqlFunction]
    val accType = sqlFunction.externalAccType
    val resultType = sqlFunction.externalResultType
    val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
      0,
      sqlFunction.getFunction,
      accType,
      true)

    val aggInfoList = AggregateInfoList(Array[AggregateInfo](
      new CoAggregateInfo(
        null,
        sqlFunction.getFunction,
        0,
        input1Types.zipWithIndex.map(_._2),
        input2Types.zipWithIndex.map(_._2),
        Array(newExternalAccType),
        specs,
        resultType)))

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(tableEnv.getConfig, supportReference = true),
      tableEnv.getRelBuilder,
      needRetraction,
      needMerge = false,
      tableEnv.getConfig.getNullCheck)
      .bindInput(input1Types)
      .bindSecondInput(input2Types)

    val generateAggHandler: GeneratedCoTableValuedAggHandleFunction =
      generator.generateCoTableValuedAggHandler("CoGroupTableValuedAggHandler", aggInfoList)

    val outputRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(deriveRowType, classOf[BaseRow])
    val coTableValuedGroupAggFunction = if (sqlFunction.getFunction.isDeterministic) {
      new CoGroupTableValuedAggFunction(
        generateAggHandler,
        accType,
        generateRetraction,
        queryConfig
      )
    } else {
      new NonDeterministicCoGroupTableValuedAggFunction(
        generateAggHandler,
        accType,
        DataTypes.internal(outputRowType).asInstanceOf[BaseRowType],
        generateRetraction,
        queryConfig
      )
    }


    val operator = new KeyedCoProcessOperator(coTableValuedGroupAggFunction)
    operator.setRequireState(true)

    val leftTrans = getLeft.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    val rightTrans = getRight.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    val ret = new TwoInputTransformation(
      leftTrans,
      rightTrans,
      toString,
      operator,
      outputRowType,
      tableEnv.execEnv.getParallelism
    )

    if (groupKey1.isEmpty || groupKey2.isEmpty) {
      ret.forceNonParallel()
    }

    val selector1 = getSelector(leftTrans, input1Types, groupKey1)
    val selector2 = getSelector(rightTrans, input2Types, groupKey1)

    ret.setStateKeySelectors(selector1, selector2)
    ret.setStateKeyType(selector1.getProducedType)
    ret
  }

  def getSelector(
    inputTrans: StreamTransformation[BaseRow],
    inputTypes: Seq[InternalType],
    groupKeys: Seq[RexNode]): BaseRowKeySelector = {

    val inputRowType = inputTrans.getOutputType.asInstanceOf[BaseRowTypeInfo[_]]
    val groupingIndexs: Array[Int] =
      groupKeys.zipWithIndex.map(t => inputTypes.length + t._2).toArray
    StreamExecUtil.getKeySelector(groupingIndexs, inputRowType)
  }
}
