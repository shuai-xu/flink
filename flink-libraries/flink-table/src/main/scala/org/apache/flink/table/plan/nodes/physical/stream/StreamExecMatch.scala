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

import java.lang.{Long => JLong}
import java.util
import java.math.{BigDecimal => JBigDecimal}
import java.util.UUID

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel._
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlMatchRecognize.AfterOption
import org.apache.calcite.sql.SqlMatchRecognize.RowsPerMatchOption
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.cep.{EventComparator, PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.nfa.AfterMatchSkipStrategy
import org.apache.flink.cep.nfa.compiler.NFACompiler
import org.apache.flink.cep.operator.{FlatSelectCepOperator, FlatSelectTimeoutCepOperator, SelectCepOperator, SelectTimeoutCepOperator}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.triggers.{FixedIntervalTrigger, PeriodicIntervalTrigger}
import org.apache.flink.streaming.api.operators.{ChainingStrategy, ProcessOperator}
import org.apache.flink.streaming.api.operators.co.CoStreamMap
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, SideOutputTransformation, StreamTransformation, TwoInputTransformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.StreamExecUtil
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.BaseRowRowtimeProcessFunction
import org.apache.flink.table.runtime.`match`._
import org.apache.flink.table.runtime.aggregate.SortUtil
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.OutputTag

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Flink RelNode which matches along with LogicalMatch.
  */
class StreamExecMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    pattern: RexNode,
    strictStart: Boolean,
    strictEnd: Boolean,
    patternDefinitions: util.Map[String, RexNode],
    measures: util.Map[String, RexNode],
    after: RexNode,
    subsets: util.Map[String, _ <: util.SortedSet[String]],
    rowsPerMatch: RexNode,
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    interval: RexNode,
    emit: RexNode,
    outputSchema: BaseRowSchema,
    inputSchema: BaseRowSchema)
  extends SingleRel(cluster, traitSet, input)
  with StreamExecRel {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecMatch(
      cluster,
      traitSet,
      inputs.get(0),
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      rowsPerMatch,
      partitionKeys,
      orderKeys,
      interval,
      emit,
      outputSchema,
      inputSchema)
  }

  override def toString: String = {
    s"Match(${
      if (!partitionKeys.isEmpty) {
        s"PARTITION BY: ${partitionKeys.toArray.map(_.toString).mkString(", ")}, "
      } else {
        ""
      }
    }${
      if (!orderKeys.getFieldCollations.isEmpty) {
        s"ORDER BY: ${orderKeys.getFieldCollations.asScala.map {
          x => inputSchema.fieldNames(x.getFieldIndex)
        }.mkString(", ")}, "
      } else {
        ""
      }
    }${
      if (!measures.isEmpty) {
        s"MEASURES: ${measures.asScala.map {
          case (k, v) => s"${v.toString} AS $k"
        }.mkString(", ")}, "
      } else {
        ""
      }
    }${
      if (rowsPerMatch != null) {
        s"${rowsPerMatch.toString}, "
      }
    }${
      s"${after.toString}, "
    }${
      s"PATTERN: (${pattern.toString})"
    }${
      if (interval != null) {
        s"WITHIN INTERVAL: $interval, "
      } else {
        s", "
      }
    }${
      if (emit != null) {
        s"EMIT: $emit, "
      } else {
        s""
      }
    }${
      if (!subsets.isEmpty) {
        s"SUBSET: ${subsets.asScala.map {
          case (k, v) => s"$k = (${v.toArray.mkString(", ")})"
        }.mkString(", ")}, "
      } else {
        ""
      }
    }${
      s"DEFINE: ${patternDefinitions.asScala.map {
        case (k, v) => s"$k AS ${v.toString}"
      }.mkString(", ")}"
    })"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput())
      .itemIf("partitionBy",
        partitionKeys.toArray.map(_.toString).mkString(", "),
        !partitionKeys.isEmpty)
      .itemIf("orderBy",
        orderKeys.getFieldCollations.asScala.map {
          x => inputSchema.fieldNames(x.getFieldIndex)
        }.mkString(", "),
        !orderKeys.getFieldCollations.isEmpty)
      .itemIf("measures",
        measures.asScala.map { case (k, v) => s"${v.toString} AS $k"}.mkString(", "),
        !measures.isEmpty)
      .itemIf("rowsPerMatch",
        if (rowsPerMatch != null) {
          rowsPerMatch.toString
        } else {
          null
        },
        rowsPerMatch != null)
      .item("after", after.toString)
      .item("pattern", pattern.toString)
      .itemIf("within interval",
        if (interval != null) {
          interval.toString
        } else {
          null
        },
        interval != null)
      .itemIf("emit",
        if (emit != null) {
          emit.toString
        } else {
          null
        },
        emit != null)
      .itemIf("subset",
        subsets.asScala.map { case (k, v) => s"$k = (${v.toArray.mkString(", ")})"}.mkString(", "),
        !subsets.isEmpty)
      .item("define",
        patternDefinitions.asScala.map { case (k, v) => s"$k AS ${v.toString}"}.mkString(", "))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {

    val config = tableEnv.config
    val relBuilder = tableEnv.getRelBuilder
    val inputTypeInfo =
      inputSchema.typeInfo(classOf[BaseRow]).asInstanceOf[BaseRowTypeInfo[BaseRow]]

    val inputTransform =
      getInput.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)

    val rowtimeFields = inputSchema.relDataType
      .getFieldList.asScala
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    val timestampedInputTransform = if (rowtimeFields.nonEmpty) {
      // copy the rowtime field into the StreamRecord timestamp field
      val timeIdx = rowtimeFields.head.getIndex

      new OneInputTransformation(
        inputTransform,
        s"rowtime field: (${rowtimeFields.head})",
        new ProcessOperator(new BaseRowRowtimeProcessFunction(timeIdx, inputTypeInfo)),
        inputTypeInfo,
        inputTransform.getParallelism)
    } else {
      inputTransform
    }

    def translatePattern(
      relBuilder: RelBuilder,
      rexNode: RexNode,
      currentPattern: Pattern[BaseRow, BaseRow],
      patternNames: ListBuffer[String],
      strictContiguity: Option[Boolean]): Pattern[BaseRow, BaseRow] = rexNode match {
      case literal: RexLiteral =>
        val patternName = literal.getValue3.toString
        patternNames += patternName
        val newPattern = createPattern(currentPattern, patternName, strictContiguity)

        val patternDefinition = patternDefinitions.get(patternName)
        if (patternDefinition != null) {
          val condition = MatchUtil.generateIterativeCondition(
            config,
            relBuilder,
            patternName,
            patternNames,
            patternDefinition,
            inputTypeInfo)

          newPattern.where(condition)
        } else {
          newPattern
        }

      case call: RexCall =>

        call.getOperator match {
          case PATTERN_CONCAT =>
            val left = call.getOperands.get(0)
            val right = call.getOperands.get(1)
            translatePattern(
              relBuilder,
              right,
              translatePattern(relBuilder, left, currentPattern, patternNames, strictContiguity),
              patternNames,
              Some(true))

          case PATTERN_FOLLOWED_BY =>
            val left = call.getOperands.get(0)
            val right = call.getOperands.get(1)
            translatePattern(
              relBuilder,
              right,
              translatePattern(relBuilder, left, currentPattern, patternNames, strictContiguity),
              patternNames,
              Some(false))

          case PATTERN_QUANTIFIER =>
            val name = call.getOperands.get(0).asInstanceOf[RexLiteral]
            val newPattern = translatePattern(
              relBuilder,
              name,
              currentPattern,
              patternNames,
              strictContiguity)

            val startNum = call.getOperands.get(1).asInstanceOf[RexLiteral]
              .getValue3.asInstanceOf[JBigDecimal].intValue()
            val endNum = call.getOperands.get(2).asInstanceOf[RexLiteral]
              .getValue3.asInstanceOf[JBigDecimal].intValue()
            val greedy = !call.getOperands.get(3).asInstanceOf[RexLiteral]
              .getValue3.asInstanceOf[Boolean]

            if (startNum == 0 && endNum == -1) {
              // zero or more
              newPattern.oneOrMore().optional().consecutive()
            } else if (startNum == 1 && endNum == -1) {
              // one or more
              newPattern.oneOrMore().consecutive()
            } else if (startNum == 0 && endNum == 1) {
              // optional
              newPattern.optional()
            } else if (endNum != -1) {
              // times
              newPattern.times(startNum, endNum).consecutive()
            } else {
              // times or more
              newPattern.timesOrMore(startNum).consecutive()
            }

            if (greedy) {
              newPattern.greedy()
            } else {
              newPattern
            }

          case PATTERN_ALTER =>
            throw TableException("Currently, CEP doesn't support branching patterns.")

          case PATTERN_PERMUTE =>
            throw TableException("Currently, CEP doesn't support PERMUTE patterns.")

          case PATTERN_EXCLUDE =>
            throw TableException("Currently, CEP doesn't support '{-' '-}' patterns.")
        }

      case _ =>
        throw TableException("")
    }

    var comparator: EventComparator[BaseRow] = null
    if (orderKeys.getFieldCollations.size() > 0) {
      // need to identify time between others order fields. Time needs to be first sort element
      val timeType = SortUtil.getFirstSortField(orderKeys, inputSchema.relDataType).getType

      // time ordering needs to be ascending
      if (SortUtil.getFirstSortDirection(orderKeys) != Direction.ASCENDING) {
        throw new TableException("Primary sort order must be ascending on time.")
      }

      comparator = timeType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
          MatchUtil.createProcTimeSortFunction(orderKeys, inputSchema)
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
          MatchUtil.createRowTimeSortFunction(orderKeys, inputSchema)
        case _ =>
          throw new TableException("Primary sort order must be on time column.")
      }
    }

    val patternNames: ListBuffer[String] = ListBuffer()
    val cepPattern = translatePattern(relBuilder, pattern, null, patternNames, None)

    // set pattern interval
    interval match {
      case intervalLiteral: RexLiteral =>
        cepPattern.within(Time.milliseconds(convertToMs(intervalLiteral)))

      case intervalNode: RexNode =>
        val patternWindowTimeFunction =
          MatchUtil.generatePatternWindowTimeFunction(
            config,
            relBuilder,
            intervalNode,
            patternNames,
            inputTypeInfo)
        cepPattern.within(patternWindowTimeFunction)

      case _ =>
    }

    // set pattern emit strategy
    emit match {
      case emitCall: RexCall if emitCall.getKind == SqlKind.EMIT_TIMEOUT =>
        if (cepPattern.getWindowTimeFunction == null) {
          throw new TableException("EMIT TIMEOUT must be used with WITHIN clause.")
        }
        val emitIntervals = new util.ArrayList[JLong]()
        emitCall.getOperands.asScala.foreach {
          case emitIntervalLiteral: RexLiteral =>
            emitIntervals.add(convertToMs(emitIntervalLiteral))

          case _ =>
            throw new TableException("Currently, EMIT TIMEOUT only support literal interval.")
        }
        cepPattern.trigger(new FixedIntervalTrigger(emitIntervals))

      case emitEvery: RexCall if emitEvery.getKind == SqlKind.EMIT_TIMEOUT_EVERY =>
        if (cepPattern.getWindowTimeFunction == null) {
          throw new TableException("EMIT TIMEOUT must be used with WITHIN clause.")
        }

        val emitInterval = emitEvery.getOperands.get(0) match {
          case emitIntervalLiteral: RexLiteral =>
            convertToMs(emitIntervalLiteral)

          case _ =>
            throw new TableException("Currently, EMIT TIMEOUT only support literal interval.")
        }
        cepPattern.trigger(new PeriodicIntervalTrigger(emitInterval))

      case _ =>
    }

    val outputTypeInfo = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BaseRow])
    val isProcessingTime = rowtimeFields.isEmpty

    // define the select function to the detected pattern sequence
    val rowsPerMatchLiteral =
      if (rowsPerMatch == null) {
        RowsPerMatchOption.ONE_ROW
      } else {
        rowsPerMatch.asInstanceOf[RexLiteral].getValue
      }

    rowsPerMatchLiteral match {
      case RowsPerMatchOption.ONE_ROW =>
        val patternSelectFunction =
          MatchUtil.generatePatternSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            partitionKeys,
            measures,
            inputTypeInfo)

        generateSelectTransformation(
          timestampedInputTransform,
          cepPattern,
          comparator,
          patternSelectFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case RowsPerMatchOption.ALL_ROWS =>
        val patternFlatSelectFunction =
          MatchUtil.generatePatternFlatSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            partitionKeys,
            orderKeys,
            measures,
            inputTypeInfo)

        generateFlatSelectTransformation(
          timestampedInputTransform,
          cepPattern,
          comparator,
          patternFlatSelectFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case RowsPerMatchOption.ONE_ROW_WITH_TIMEOUT =>
        val patternSelectFunction =
          MatchUtil.generatePatternSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            partitionKeys,
            measures,
            inputTypeInfo)

        val patternTimeoutFunction =
          MatchUtil.generatePatternTimeoutFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            partitionKeys,
            measures,
            inputTypeInfo)

        generateSelectTimeoutTransformation(
          timestampedInputTransform,
          cepPattern,
          comparator,
          patternSelectFunction,
          patternTimeoutFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case RowsPerMatchOption.ALL_ROWS_WITH_TIMEOUT =>
        val patternFlatSelectFunction =
          MatchUtil.generatePatternFlatSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            partitionKeys,
            orderKeys,
            measures,
            inputTypeInfo)

        val patternFlatTimeoutFunction =
          MatchUtil.generatePatternFlatTimeoutFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            partitionKeys,
            orderKeys,
            measures,
            inputTypeInfo)

        generateFlatSelectTimeoutTransformation(
          timestampedInputTransform,
          cepPattern,
          comparator,
          patternFlatSelectFunction,
          patternFlatTimeoutFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case _ =>
        throw new TableException(s"Unsupported RowsPerMatchOption: $rowsPerMatchLiteral")
    }
  }

  private def setKeySelector(
      transform: OneInputTransformation[BaseRow, _],
      inputTypeInfo: BaseRowTypeInfo[_]): Unit = {
    val logicalKeys = partitionKeys.asScala.map {
      case inputRef: RexInputRef => inputRef.getIndex
    }.toArray

    val selector = StreamExecUtil.getKeySelector(logicalKeys, inputTypeInfo)
    transform.setStateKeySelector(selector)
    transform.setStateKeyType(selector.getProducedType)

    if (logicalKeys.isEmpty) {
      transform.forceNonParallel()
    }
  }

  private def createPattern(
      currentPattern: Pattern[BaseRow, BaseRow],
      patternName: String,
      strictContiguity: Option[Boolean]): Pattern[BaseRow, BaseRow] = {
    if (currentPattern == null) {
      after match {
        case afterLiteral: RexLiteral
          if afterLiteral.getValue2.asInstanceOf[AfterOption]
            == AfterOption.SKIP_TO_NEXT_ROW =>
          Pattern.begin(patternName, AfterMatchSkipStrategy.noSkip())
        case afterLiteral: RexLiteral
          if afterLiteral.getValue2.asInstanceOf[AfterOption]
            == AfterOption.SKIP_PAST_LAST_ROW =>
          Pattern.begin(patternName, AfterMatchSkipStrategy.skipPastLastEvent())
        case afterCall: RexCall if afterCall.getOperator.getKind == SqlKind.SKIP_TO_FIRST =>
          val symbol = afterCall.getOperands.get(0)
            .asInstanceOf[RexLiteral].getValue3.asInstanceOf[String]
          Pattern.begin(patternName, AfterMatchSkipStrategy.skipToFirst(symbol))
        case afterCall: RexCall if afterCall.getOperator.getKind == SqlKind.SKIP_TO_LAST =>
          val symbol = afterCall.getOperands.get(0)
            .asInstanceOf[RexLiteral].getValue3.asInstanceOf[String]
          Pattern.begin(patternName, AfterMatchSkipStrategy.skipToLast(symbol))
      }
    } else {
      if (strictContiguity.get) {
        currentPattern.next(patternName)
      } else {
        currentPattern.followedBy(patternName)
      }
    }
  }

  private def convertToMs(intervalLiteral: RexLiteral): JLong = {
    val intervalValue = intervalLiteral.asInstanceOf[RexLiteral].getValueAs(classOf[JLong])
    intervalLiteral.getTypeName match {
      case INTERVAL_YEAR | INTERVAL_YEAR_MONTH | INTERVAL_MONTH =>
        // convert from months to milliseconds, suppose 1 month = 30 days
        intervalValue * 30L * 24 * 3600 * 1000
      case _ => intervalValue
    }
  }

  private def generateSelectTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternSelectFunction: PatternSelectFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo[BaseRow],
      outputTypeInfo: BaseRowTypeInfo[BaseRow]): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, inputSerializer, true)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "SelectCepOperator",
      new SelectCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        cepPattern.getWindowTimeFunction,
        cepPattern.getTrigger,
        patternSelectFunction),
      outputTypeInfo,
      inputTransform.getParallelism)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    patternStreamTransform
  }

  private def generateFlatSelectTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternFlatSelectFunction: PatternFlatSelectFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo[BaseRow],
      outputTypeInfo: BaseRowTypeInfo[BaseRow]): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, inputSerializer, true)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "FlatSelectCepOperator",
      new FlatSelectCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        cepPattern.getWindowTimeFunction,
        cepPattern.getTrigger,
        patternFlatSelectFunction),
      outputTypeInfo,
      inputTransform.getParallelism)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    patternStreamTransform
  }

  private def generateSelectTimeoutTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternSelectFunction: PatternSelectFunction[BaseRow, BaseRow],
      patternTimeoutFunction: PatternTimeoutFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo[BaseRow],
      outputTypeInfo: BaseRowTypeInfo[BaseRow]): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, inputSerializer, true)
    val timeoutOutputTag = new OutputTag(UUID.randomUUID.toString, outputTypeInfo)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "SelectTimeoutCepOperator",
      new SelectTimeoutCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        cepPattern.getWindowTimeFunction,
        cepPattern.getTrigger,
        patternSelectFunction,
        patternTimeoutFunction,
        timeoutOutputTag),
      outputTypeInfo,
      inputTransform.getParallelism)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    val timeoutStreamTransform = new SideOutputTransformation(
      patternStreamTransform,
      timeoutOutputTag)

    new TwoInputTransformation(
      patternStreamTransform,
      timeoutStreamTransform,
      "CombineOutputCepOperator",
      new CoStreamMap(new CombineCepOutputCoMapFunction),
      outputTypeInfo,
      inputTransform.getParallelism)
  }

  private def generateFlatSelectTimeoutTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternFlatSelectFunction: PatternFlatSelectFunction[BaseRow, BaseRow],
      patternFlatTimeoutFunction: PatternFlatTimeoutFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo[BaseRow],
      outputTypeInfo: BaseRowTypeInfo[BaseRow]): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, inputSerializer, true)
    val timeoutOutputTag = new OutputTag(UUID.randomUUID.toString, outputTypeInfo)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "FlatSelectTimeoutCepOperator",
      new FlatSelectTimeoutCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        cepPattern.getWindowTimeFunction,
        cepPattern.getTrigger,
        patternFlatSelectFunction,
        patternFlatTimeoutFunction,
        timeoutOutputTag),
      outputTypeInfo,
      inputTransform.getParallelism)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    val timeoutStreamTransform = new SideOutputTransformation(
      patternStreamTransform,
      timeoutOutputTag)

    new TwoInputTransformation(
      patternStreamTransform,
      timeoutStreamTransform,
      "CombineOutputCepOperator",
      new CoStreamMap(new CombineCepOutputCoMapFunction),
      outputTypeInfo,
      inputTransform.getParallelism)
  }
}
