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
package org.apache.flink.table.plan.util

import java.util

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction
import org.apache.calcite.sql.SqlRankFunction
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.dataview.DataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.expressions._
import org.apache.flink.table.runtime.functions.aggfunctions.CountDistinct.CountDistinctAggFunction
import org.apache.flink.table.runtime.functions.aggfunctions._
import org.apache.flink.table.codegen.expr.{ConcatAggFunction => _, _}
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, DeclarativeAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.types.{DataType, DataTypes}

import scala.collection.JavaConversions._

object AggregateUtil {

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]
  type JavaList[T] = java.util.List[T]

  def transformToBatchAggregateFunctions(
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      orderKeyIdx: Array[Int] = null)
    : (Array[Array[Int]],
      Array[Array[DataType]],
      Array[UserDefinedFunction]) = {

    val aggInfos = transformToAggregateInfoList(
      aggregateCalls,
      inputType,
      orderKeyIdx,
      Array.fill(aggregateCalls.size)(false),
      isStateBackedDataViews = false,
      needInputCount = false).aggInfos

    val aggFields = aggInfos.map(_.argIndexes)
    val bufferTypes = aggInfos.map(_.externalAccTypes)
    val functions = aggInfos.map(_.function)

    (aggFields, bufferTypes, functions)
  }

  def transformToStreamAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      needRetraction: Array[Boolean],
      needInputCount: Boolean,
      isStateBackendDataViews: Boolean): AggregateInfoList = {
    transformToAggregateInfoList(
      aggregateCalls,
      inputType,
      orderKeyIdx = null,
      needRetraction,
      needInputCount,
      isStateBackendDataViews)
  }

  /**
    * Transforms calcite aggregate calls to AggregateInfos.
    * @param aggregateCalls   the calcite aggregate calls
    * @param inputType        the input rel data type
    * @param orderKeyIdx      the index of order by field in the input, null if not over agg
    * @param needRetraction   whether the aggregate function need retract method
    * @param needInputCount   whether need to calculate the input counts, which is used in
    *                         aggregation with retraction input.If needed,
    *                         insert a count(1) aggregate into the agg list.
    * @param isStateBackedDataViews   whether the dataview in accumulator use state or heap
    */
  private[flink] def transformToAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      orderKeyIdx: Array[Int],
      needRetraction: Array[Boolean],
      needInputCount: Boolean,
      isStateBackedDataViews: Boolean)
    : AggregateInfoList = {

    val factory = new AggFunctionFactory(inputType, orderKeyIdx, needRetraction)
    var aggCalls = aggregateCalls

    var count1AggIndex: Option[Int] = None
    var count1AggInserted: Boolean = false
    // if need inputCount, find count1 in the existed aggregate calls first,
    // if not exist, insert a new count1 and remember the index
    if (needInputCount) {
      // find count1 in the existed aggregate calls first
      aggregateCalls.zipWithIndex.foreach { case (call, index) =>
        if (call.getAggregation.isInstanceOf[SqlCountAggFunction] &&
          call.filterArg < 0 &&
          call.getArgList.isEmpty &&
          !call.isApproximate &&
          !call.isDistinct) {
          count1AggIndex = Some(index)
        }
      }

      // count1 not exist in aggregateCalls, insert a count1 in it.
      val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
      if (count1AggIndex.isEmpty) {

        val count1 = AggregateCall.create(
          SqlStdOperatorTable.COUNT,
          false,
          false,
          new util.ArrayList[Integer](),
          -1,
          typeFactory.createTypeFromTypeInfo(Types.LONG, isNullable = false),
          "_$count1$_")

        count1AggIndex = Some(aggregateCalls.length)
        count1AggInserted = true
        aggCalls = aggregateCalls ++ Seq(count1)
      }
    }


    val aggInfos = aggCalls.zipWithIndex.map { case (call, index) =>
      val argIndexes = call.getAggregation match {
        case _: SqlRankFunction => orderKeyIdx
        case _ => call.getArgList.map(_.intValue()).toArray
      }
      val function = factory.createAggFunction(call, index)
      val (externalAccTypes, viewSpecs, externalResultType) = function match {
        case a: DeclarativeAggregateFunction =>
          val bufferTypes: Array[DataType] = a.aggBufferSchema.toArray
          (bufferTypes, Array.empty[DataViewSpec], a.getResultType)
        case a: AggregateFunction[_, _] =>
          val externalAccType = getAccumulatorTypeOfAggregateFunction(a)
          val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
            index,
            a,
            externalAccType,
            isStateBackedDataViews)
          (Array(newExternalAccType), specs, getResultTypeOfAggregateFunction(a))
        case _ => throw new TableException("")
      }

      AggregateInfo(
        call,
        function,
        index,
        argIndexes,
        externalAccTypes,
        viewSpecs,
        externalResultType)

    }.toArray

    AggregateInfoList(aggInfos, count1AggIndex, count1AggInserted)
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  def doAllSupportPartialMerge(aggInfos: Array[AggregateInfo]): Boolean = {
    val supportMerge = aggInfos.map(_.function).forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }

    //it means grouping without aggregate functions
    aggInfos.isEmpty || supportMerge
  }

  /**
    * Return true if all aggregates can be splitted. False otherwise.
    */
  def doAllSupportSplit(aggInfos: Array[AggregateInfo]): Boolean = {
    aggInfos.forall { aggInfo =>
      aggInfo.function match {
        case _: MinAggFunction | _: MinWithRetractAggFunction[_] |
             _: MaxAggFunction | _: MaxWithRetractAggFunction[_] |
             _: SumAggFunction | _: Sum0AggFunction | _: SumWithRetractAggFunction |
             _: CountAggFunction | _: Count1AggFunction |
             _: CountDistinctAggFunction | _: AvgAggFunction |
             _: SingleValueAggFunction |
             _: ConcatAggFunction | _: org.apache.flink.table.codegen.expr.ConcatAggFunction |
             _: ConcatWsAggFunction => true
        case _: FirstValueAggFunction[_] | _: FirstValueWithRetractAggFunction[_] |
             _: LastValueAggFunction[_] | _: LastValueWithRetractAggFunction[_] =>
          // only support aggregation without order
          aggInfo.agg.getArgList.size() == 1
        case _ => false
      }
    }
  }

  /**
    * Derives output row type from local aggregate
    */
  def inferLocalAggRowType(
    aggInfoList: AggregateInfoList,
    inputType: RelDataType,
    outputType: RelDataType,
    groupSet: Array[Int],
    typeFactory: FlinkTypeFactory): RelDataType = {

    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)

    val groupingNames = groupSet.map(inputType.getFieldNames.get(_))
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap { i =>
      aggInfoList.aggInfos(i).function match {
        case _: AggregateFunction[_, _] =>
          Array(outputType.getFieldNames.get(groupSet.length + i))
        case daf: DeclarativeAggregateFunction =>
          daf.aggBufferAttributes.map { a =>
            index += 1
            s"${a.name}$$$index"
          }
      }
    }

    typeFactory.buildRelDataType(
      groupingNames ++ aggBufferNames,
      groupingTypes ++ accTypes.map(DataTypes.internal))
  }

  private[flink] def asLong(expr: Expression): Long = expr match {
    case Literal(value: Long, DataTypes.INTERVAL_MILLIS) => value
    case Literal(value: Long, DataTypes.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }

  /**
    * Computes the positions of (window start, window end, row time).
    */
  private[flink] def computeWindowPropertyPos(
      properties: Seq[NamedWindowProperty]): (Option[Int], Option[Int], Option[Int]) = {

    val propPos = properties.foldRight(
      (None: Option[Int], None: Option[Int], None: Option[Int], 0)) {
      case (p, (s, e, rt, i)) => p match {
        case NamedWindowProperty(_, prop) =>
          prop match {
            case WindowStart(_) if s.isDefined =>
              throw TableException("Duplicate window start property encountered. This is a bug.")
            case WindowStart(_) =>
              (Some(i), e, rt, i - 1)
            case WindowEnd(_) if e.isDefined =>
              throw TableException("Duplicate window end property encountered. This is a bug.")
            case WindowEnd(_) =>
              (s, Some(i), rt, i - 1)
            case RowtimeAttribute(_) if rt.isDefined =>
              throw TableException("Duplicate window rowtime property encountered. This is a bug.")
            case RowtimeAttribute(_) =>
              (s, e, Some(i), i - 1)
            case ProctimeAttribute(_) =>
              // ignore this property, it will be null at the position later
              (s, e, rt, i - 1)
          }
      }
    }
    (propPos._1, propPos._2, propPos._3)
  }

  /**
    * Optimize max or min with retraction agg. MaxWithRetract can be optimized to Max if input is
    * update increasing.
    */
  def getNeedRetractions(
    groupSize: Int,
    needRetraction: Boolean,
    modifiedMono: RelModifiedMonotonicity,
    aggs: Seq[AggregateCall]): Array[Boolean] = {

    val needRetractionArray = Array.fill(aggs.size)(needRetraction)
    if (modifiedMono != null && needRetraction) {
      aggs.zipWithIndex.foreach(e => {
        e._1.getAggregation match {
          // if mono is decreasing add agg is min with retract, set needretraction to false
          case a: SqlMinMaxAggFunction
            if a.getKind == SqlKind.MIN && modifiedMono.fieldMonotonicities(groupSize + e._2) ==
              SqlMonotonicity.DECREASING => needRetractionArray(e._2) = false
          // if mono is increasing add agg is max with retract, set needretraction to false
          case a: SqlMinMaxAggFunction
            if a.getKind == SqlKind.MAX && modifiedMono.fieldMonotonicities(groupSize + e._2) ==
              SqlMonotonicity.INCREASING => needRetractionArray(e._2) = false
          case _ =>
        }
      })
    }

    needRetractionArray
  }

  /**
    * Compute field index of given timeField expression.
    */
  def timeFieldIndex(inputType: RelDataType, relBuilder: RelBuilder, timeField: Expression): Int = {
    timeField.toRexNode(relBuilder.values(inputType)).asInstanceOf[RexInputRef].getIndex
  }
}

/**
  * The information about aggregate function call
  * @param agg  calcite agg call
  * @param function AggregateFunction or DeclarativeAggregateFunction
  * @param aggIndex the index of the aggregate call in the aggregation list
  * @param argIndexes the aggregate arguments indexes in the input
  * @param externalAccTypes  accumulator types
  * @param viewSpecs  data view specs
  * @param externalResultType the result type of aggregate
  */
case class AggregateInfo(
  agg: AggregateCall,
  function: UserDefinedFunction,
  aggIndex: Int,
  argIndexes: Array[Int],
  externalAccTypes: Array[DataType],
  viewSpecs: Array[DataViewSpec],
  externalResultType: DataType)

/**
  * The information contains all aggregate infos, and including input count information.
  *
  * @param aggInfos the information about every aggregates
  * @param count1AggIndex  None if input count is not needed, otherwise is needed and the index
  *                        represents the count1 index
  * @param count1AggInserted  true when the count1 is inserted into agg list,
  *                           false when the count1 is already existent in agg list.
  */
case class AggregateInfoList(
  aggInfos: Array[AggregateInfo],
  count1AggIndex: Option[Int],
  count1AggInserted: Boolean) {

  def getAggNames: Array[String] = aggInfos.map(_.agg.getName)

  def getAccTypes: Array[DataType] = aggInfos.flatMap(_.externalAccTypes)
  
  def getActualAggregateCalls: Array[AggregateCall] = {
    getActualAggregateInfos.map(_.agg)
  }

  def getActualFunctions: Array[UserDefinedFunction] = {
    getActualAggregateInfos.map(_.function)
  }

  def getActualValueTypes: Array[DataType] = {
    getActualAggregateInfos.map(_.externalResultType)
  }

  def getCount1AccIndex: Option[Int] = {
    if (count1AggIndex.nonEmpty) {
      var accOffset = 0
      aggInfos.indices.foreach { i =>
        if (i < count1AggIndex.get) {
          accOffset += aggInfos(i).externalAccTypes.length
        }
      }
      Some(accOffset)
    } else {
      None
    }
  }
  
  def getActualAggregateInfos: Array[AggregateInfo] = {
    if (count1AggIndex.nonEmpty && count1AggInserted) {
      // need input count agg and the count1 is inserted,
      // which means the count1 shouldn't be calculated in value
      aggInfos.zipWithIndex
      .filter { case (_, index) => index != count1AggIndex.get}
      .map { case (aggInfo, _) => aggInfo}
    } else {
      aggInfos
    }
  }
}
