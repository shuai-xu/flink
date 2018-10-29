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
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableException}
import org.apache.calcite.util.{ImmutableBitSet, NumberUtil}
import org.apache.flink.table.api.{AggPhaseEnforcer, BatchTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.agg.BatchExecAggregateCodeGen
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.generatorCollect
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedOperator}
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction
import org.apache.flink.table.functions.{DeclarativeAggregateFunction, UserDefinedFunction, AggregateFunction => UserDefinedAggregateFunction}
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.common.CommonAggregate
import org.apache.flink.table.runtime.operator.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.types.{BaseRowType, DataTypes, InternalType}
import org.apache.flink.table.util.FlinkRelOptUtil

abstract class BatchExecGroupAggregateBase(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    val isMerge: Boolean,
    val isFinal: Boolean)
  extends SingleRel(cluster, traitSet, inputNode)
  with BatchExecAggregateCodeGen
  with CommonAggregate
  with RowBatchExecRel {

  if (grouping.isEmpty && auxGrouping.nonEmpty) {
    throw new TableException("auxGrouping should be empty if grouping is emtpy.")
  }

  // make input type available when generating RexNode
  lazy val builder: RelBuilder = relBuilder.values(inputRelDataType)

  lazy val aggregateCalls: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)
  lazy val aggregates: Seq[UserDefinedFunction] = aggCallToAggFunction.map(_._2)

  // currently put auxGrouping to aggBuffer in code-gen
  lazy val aggBufferNames: Array[Array[String]] = auxGrouping.zipWithIndex.map {
    case (_, index) => Array(s"aux_group$index")
  } ++ aggregates.zipWithIndex.toArray.map {
    case (a: DeclarativeAggregateFunction, index) =>
      val idx = auxGrouping.length + index
      a.aggBufferAttributes.map(attr => s"agg${idx}_${attr.name}").toArray
    case (_: UserDefinedAggregateFunction[_, _], index) =>
      val idx = auxGrouping.length + index
      Array(s"agg$idx")
  }

  lazy val aggBufferTypes: Array[Array[InternalType]] = auxGrouping.map { index =>
    Array(FlinkTypeFactory.toInternalType(inputRelDataType.getFieldList.get(index).getType))
  } ++ aggregates.map {
    case a: DeclarativeAggregateFunction =>
      a.aggBufferSchema.map(DataTypes.internal).toArray
    case a: UserDefinedAggregateFunction[_, _] =>
      Array(DataTypes.internal(getAccumulatorTypeOfAggregateFunction(a)))
  }.toArray[Array[InternalType]]

  lazy val groupKeyRowType = new BaseRowType(
    classOf[BinaryRow],
    grouping.map { index =>
      FlinkTypeFactory.toInternalType(inputRelDataType.getFieldList.get(index).getType)
    }, grouping.map(inputRelDataType.getFieldNames.get(_)))

  // get udagg instance names
  lazy val udaggs: Map[UserDefinedAggregateFunction[_, _], String] = aggregates
      .filter(a => a.isInstanceOf[UserDefinedAggregateFunction[_, _]])
      .map(a => a -> CodeGeneratorContext.udfFieldName(a)).toMap
      .asInstanceOf[Map[UserDefinedAggregateFunction[_, _], String]]

  override def deriveRowType(): RelDataType = rowRelDataType

  def getGrouping: Array[Int] = grouping

  def getAuxGrouping: Array[Int] = auxGrouping

  def getAggCallList: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)

  def getAggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)] = aggCallToAggFunction

  def getOutputRowType: BaseRowType

  private[flink] def getAggOperatorName(prefix: String): String = {
    val groupingStr = if (grouping.nonEmpty) {
      s"groupBy:(${groupingToString(inputRelDataType, grouping)}),"
    } else {
      ""
    }
    val auxGroupingStr = if (auxGrouping.nonEmpty) {
      s"auxGrouping:(${groupingToString(inputRelDataType, auxGrouping)}),"
    } else {
      ""
    }

    val projStr = s"select:(${
      aggregationToString(
        inputRelDataType,
        grouping,
        auxGrouping,
        rowRelDataType,
        aggCallToAggFunction.map(_._1),
        aggCallToAggFunction.map(_._2),
        isMerge,
        isFinal)
    }),"
    val reusedIdStr = if (isReused) {
      s"reuse_id:($getReuseId)"
    } else {
      ""
    }
    s"$prefix($groupingStr$auxGroupingStr$projStr$reusedIdStr)"
  }

  // ===============================================================================================

  private[flink] def codegenWithoutKeys(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: BaseRowType,
      outputType: BaseRowType,
      prefix: String): GeneratedOperator = {

    val config = tableEnv.config
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    // register udagg
    aggregates.filter(a => a.isInstanceOf[UserDefinedAggregateFunction[_, _]])
        .map(a => ctx.addReusableFunction(a))

    val (initAggBufferCode, doAggregateCode, aggOutputExpr) = genSortAggCodes(
      isMerge, isFinal, ctx, config, builder, getGrouping, getAuxGrouping, inputRelDataType,
      aggCallToAggFunction, aggregates, udaggs, inputTerm, inputType,
      aggBufferNames, aggBufferTypes, outputType)

    val processCode =
      s"""
         |if (!hasInput) {
         |  hasInput = true;
         |  // init agg buffer
         |  $initAggBufferCode
         |}
         |
         |${ctx.reuseInputUnboxingCode()}
         |$doAggregateCode
         |""".stripMargin.trim

    // if the input is empty in final phase, we should output default values
    val endInputCode = if (isFinal) {
      s"""
         |if (!hasInput) {
         |  $initAggBufferCode
         |}
         |${aggOutputExpr.code}
         |${generatorCollect(aggOutputExpr.resultTerm)}
         |""".stripMargin
    } else {
      s"""
         |if (hasInput) {
         |  ${aggOutputExpr.code}
         |  ${generatorCollect(aggOutputExpr.resultTerm)}
         |}""".stripMargin
    }

    val className =
      if (isFinal) s"${prefix}AggregateWithoutKeys" else s"Local${prefix}AggregateWithoutKeys"
    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputRelDataType, config)
  }

  /**
    * Check whether input data of current agg is skew on group keys.
    *
    * @param mq metadata query instance.
    * @return True if input data of current agg is skew on group keys, else false.
    */
  protected def isSkewOnGroupKeys(mq: RelMetadataQuery): Boolean = {
    if (grouping.isEmpty) {
      return false
    }
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val skewInfo = fmq.getSkewInfo(this.getInput)
    if (skewInfo == null) {
      return false
    }
    val skewMap = skewInfo.skewInfo
    grouping exists { k =>
      skewMap.get(k) match {
        case Some(skewValues) => skewValues.nonEmpty
        case _ => false
      }
    }
  }

  protected def getSkewPunishFactor: Int = {
    val tableConfig = FlinkRelOptUtil.getTableConfig(this)
    tableConfig.getParameters.getInteger(TableConfig.SQL_CBO_SKEW_PUNISH_FACTOR,
                                        TableConfig.SQL_CBO_SKEW_PUNISH_FACTOR_DEFAULT)
  }

  protected def isEnforceTwoStageAgg: Boolean = {
    val tableConfig = FlinkRelOptUtil.getTableConfig(this)
    val aggConfig = tableConfig.getParameters.getString(
      TableConfig.SQL_CBO_AGG_PHASE_ENFORCER,
      TableConfig.SQL_CBO_AGG_PHASE_ENFORCER_DEFAULT)
    AggPhaseEnforcer.TWO_PHASE.toString.equalsIgnoreCase(aggConfig)
  }
}
