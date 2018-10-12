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

package org.apache.flink.table.codegen.agg

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.runtime.util.SingleElementIterator
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.STREAM_RECORD
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression, GeneratedOperator, _}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.TableValuedAggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{TableValuedAggregateFunction, DeclarativeAggregateFunction, UserDefinedFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.conversion.InternalTypeConverters._
import org.apache.flink.table.types._
import org.apache.flink.table.codegen.Indenter.toISC

import scala.collection.JavaConverters._

trait BatchExecAggregateCodeGen {

  private[flink] def genGroupKeyProjectionCode(
      prefix: String,
      ctx: CodeGeneratorContext,
      groupKeyType: BaseRowType,
      grouping: Array[Int],
      inputType: BaseRowType,
      inputTerm: String,
      currentKeyTerm: String,
      currentKeyWriterTerm: String): String = {
    ProjectionCodeGenerator.generateProjection(
      ctx,
      newName(prefix + "GroupingKeyProj"),
      inputType,
      groupKeyType,
      grouping,
      inputTerm = inputTerm,
      outRecordTerm = currentKeyTerm,
      outRecordWriterTerm = currentKeyWriterTerm).expr.code
  }

  /**
    * The generated codes only supports the comparison of the key terms
    * in the form of binary row with only one memory segment.
    */
  private[flink] def genGroupKeyChangedCheckCode(
      currentKeyTerm: String,
      lastKeyTerm: String): String = {
    s"""
       |$currentKeyTerm.getSizeInBytes() != $lastKeyTerm.getSizeInBytes() ||
       |  !(org.apache.flink.table.util.BinaryRowUtil.byteArrayEquals(
       |     $currentKeyTerm.getMemorySegment().getHeapMemory(),
       |     $lastKeyTerm.getMemorySegment().getHeapMemory(),
       |     $currentKeyTerm.getSizeInBytes()))
       """.stripMargin.trim
  }

  // ===============================================================================================

  /**
    * In the cases of sort aggregation or hash aggregation's fall back,
    * we store the aggregate buffer as class members directly.
    * We use an unique name to locate the aggregate buffer field.
    */
  private[flink] def bindReference(
      isMerge: Boolean,
      agg: DeclarativeAggregateFunction,
      aggIndex: Int,
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferTypes: Array[Array[InternalType]]): PartialFunction[Expression, Expression] = {
    case input: UnresolvedFieldReference =>
      // We always use UnresolvedFieldReference to represent reference of input field.
      // In non-merge case, the input is operand of the aggregate function. But in merge
      // case, the input is aggregate buffers which sent by local aggregate.
      val localIndex = if (isMerge) {
        agg.inputAggBufferAttributes.indexOf(input)
      } else {
        agg.operands.indexOf(input)
      }
      val (inputIndex, inputType) = argsMapping(aggIndex)(localIndex)
      ResolvedAggInputReference(input.name, inputIndex, inputType)
    case aggBuffAttr: UnresolvedAggBufferReference =>
      val variableName = s"agg${aggIndex}_${aggBuffAttr.name}"
      val localIndex = agg.aggBufferAttributes.indexOf(aggBuffAttr)
      ResolvedAggBufferReference(
        variableName,
        aggBufferTypes(aggIndex)(localIndex))
  }

  /**
    * Declare all aggregate buffer variables, store these variables in class members
    */
  private[flink] def genFlatAggBufferExprs(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]]): Seq[GeneratedExpression] = {
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)

    val accessAuxGroupingExprs = auxGrouping.indices.map {
      idx => ResolvedAggBufferReference(aggBufferNames(idx)(0), aggBufferTypes(idx)(0))
    }.map(_.toRexNode(builder)).map(exprCodegen.generateExpression)

    val aggCallExprs = aggregates.zipWithIndex.flatMap {
      case (agg: DeclarativeAggregateFunction, aggIndex: Int) =>
        val idx = auxGrouping.length + aggIndex
        agg.aggBufferAttributes.map(_.postOrderTransform(
          bindReference(isMerge, agg, idx, argsMapping, aggBufferTypes)))
      case (_: UserDefinedAggregateFunction[_], aggIndex: Int) =>
        val idx = auxGrouping.length + aggIndex
        val variableName = aggBufferNames(idx)(0)
        Some(ResolvedAggBufferReference(
          variableName,
          aggBufferTypes(idx)(0)))
    }.map(_.toRexNode(builder)).map(exprCodegen.generateExpression)

    accessAuxGroupingExprs ++ aggCallExprs
  }

  private[flink] def genAggregateByFlatAggregateBuffer(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputType: BaseRowType,
      inputTerm: String,
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      aggBufferExprs: Seq[GeneratedExpression]): String = {
    if (isMerge) {
      genMergeFlatAggregateBuffer(ctx, config, builder, inputTerm, inputType, auxGrouping,
        aggregates, udaggs, argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs)
    } else {
      genAccumulateFlatAggregateBuffer(
        ctx, config, builder, inputTerm, inputType, auxGrouping, aggCallToAggFunction, udaggs,
        argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs)
    }
  }

  /**
    * Generate expressions which will get final aggregate value from aggregate buffers.
    */
  private[flink] def genGetValueFromFlatAggregateBuffer(
      isMerge: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      outputType: BaseRowType): Seq[GeneratedExpression] = {

    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)

    val auxGroupingExprs = auxGrouping.indices.map { idx =>
      val resultTerm = aggBufferNames(idx)(0)
      val nullTerm = s"${resultTerm}IsNull"
      GeneratedExpression(resultTerm, nullTerm, "", aggBufferTypes(idx)(0))
    }

    val aggExprs = aggregates.zipWithIndex.map {
      case (agg: DeclarativeAggregateFunction, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        agg.getValueExpression.postOrderTransform(
          bindReference(isMerge, agg, idx, argsMapping, aggBufferTypes))
      case (agg: UserDefinedAggregateFunction[_], aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        (agg, idx)
    }.map {
      case (expr: Expression) => expr.toRexNode(builder)
      case t@_ => t
    }.map {
      case (rex: RexNode) => exprCodegen.generateExpression(rex)
      case (agg: UserDefinedAggregateFunction[_], aggIndex: Int) =>
        val resultType = getResultTypeOfAggregateFunction(agg)
        val accType = getAccumulatorTypeOfAggregateFunction(agg)
        val resultTerm = genToInternal(ctx, resultType,
          s"${udaggs(agg)}.getValue(${genToExternal(ctx, accType, aggBufferNames(aggIndex)(0))})")
        val nullTerm = s"${aggBufferNames(aggIndex)(0)}IsNull"
        GeneratedExpression(resultTerm, nullTerm, "", DataTypes.internal(resultType))
    }

    auxGroupingExprs ++ aggExprs
  }

  /**
    * Generate codes which will init the aggregate buffer.
    */
  private[flink] def genInitFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputType: BaseRowType,
      inputTerm: String,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      aggBufferExprs: Seq[GeneratedExpression],
      forHashAgg: Boolean = false): String = {
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(inputType, inputTerm = inputTerm, inputFieldMapping = Some(auxGrouping))

    val initAuxGroupingExprs = {
      if (forHashAgg) {
        // access fallbackInput
        auxGrouping.indices.map(idx => idx + grouping.length).toArray
      } else {
        // access input
        auxGrouping
      }
    }.map { idx =>
      CodeGenUtils.generateFieldAccess(
        ctx, DataTypes.internal(inputType), inputTerm, idx, nullCheck = true)
    }

    val initAggCallBufferExprs = aggregates.flatMap {
      case (agg: DeclarativeAggregateFunction) =>
        agg.initialValuesExpressions
      case (agg: UserDefinedAggregateFunction[_]) =>
        Some(agg)
    }.map {
      case (expr: Expression) => expr.toRexNode(builder)
      case t@_ => t
    }.map {
      case (rex: RexNode) => exprCodegen.generateExpression(rex)
      case (agg: UserDefinedAggregateFunction[_]) =>
        val resultTerm = s"${udaggs(agg)}.createAccumulator()"
        val nullTerm = "false"
        val resultType = getAccumulatorTypeOfAggregateFunction(agg)
        GeneratedExpression(
          genToInternal(ctx, resultType, resultTerm), nullTerm, "", DataTypes.internal(resultType))
    }

    val initAggBufferExprs = initAuxGroupingExprs ++ initAggCallBufferExprs
    require(aggBufferExprs.length == initAggBufferExprs.length)

    aggBufferExprs.zip(initAggBufferExprs).map {
      case (aggBufVar, initExpr) =>
        val needCopy = aggBufVar.resultType match {
          case _: StringType | _: ByteArrayType | _: ArrayType | _: MapType =>
            true
          case _ => false
        }
        val copyCode = if (needCopy) ".copy()" else ""
        s"""
           |${initExpr.code}
           |${aggBufVar.nullTerm} = ${initExpr.nullTerm};
           |${aggBufVar.resultTerm} = (${initExpr.resultTerm})$copyCode;
         """.stripMargin.trim
    } mkString "\n"
  }

  /**
    * Generate codes which will read input and accumulating aggregate buffers.
    */
  private[flink] def genAccumulateFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputTerm: String,
      inputType: BaseRowType,
      auxGrouping: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      aggBufferExprs: Seq[GeneratedExpression]): String = {
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(DataTypes.internal(inputType), inputTerm = inputTerm)

    // flat map to get flat agg buffers.
    aggCallToAggFunction.zipWithIndex.flatMap {
      case (aggCallToAggFun, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        val aggCall = aggCallToAggFun._1
        aggCallToAggFun._2 match {
          case agg: DeclarativeAggregateFunction =>
            agg.accumulateExpressions.map(_.postOrderTransform(
              bindReference(isMerge = false, agg, idx, argsMapping, aggBufferTypes)))
                .map(e => (e, aggCall))
          case agg: UserDefinedAggregateFunction[_] =>
            val idx = auxGrouping.length + aggIndex
            Some(agg, idx, aggCall)
        }
    }.zip(aggBufferExprs.slice(auxGrouping.length, aggBufferExprs.size)).map {
      // DeclarativeAggregateFunction
      case ((expr: Expression, aggCall: AggregateCall), aggBufVar) =>
        val accExpr = exprCodegen.generateExpression(expr.toRexNode(builder))
        (s"""
           |${accExpr.code}
           |${aggBufVar.nullTerm} = ${accExpr.nullTerm};
           |if (!${accExpr.nullTerm}) {
           |  ${accExpr.copyResultTermToTargetIfChanged(aggBufVar.resultTerm)}
           |}
           """.stripMargin, aggCall.filterArg)
      // UserDefinedAggregateFunction
      case ((agg: UserDefinedAggregateFunction[_], aggIndex: Int, aggCall: AggregateCall),
          aggBufVar) =>
        val inFields = argsMapping(aggIndex)
        val externalAccType = getAccumulatorTypeOfAggregateFunction(agg)

        val inputExprs = inFields.map {
          f =>
            val inputRef = ResolvedAggInputReference(inputTerm, f._1, f._2)
            exprCodegen.generateExpression(inputRef.toRexNode(builder))
        }

        val externalUDITypes = getAggUserDefinedInputTypes(
          agg, externalAccType, inputExprs.map(_.resultType))
        val parameters = inputExprs.zipWithIndex.map {
          case (expr, i) =>
            s"${expr.nullTerm} ? null : " +
                s"${genToExternal(ctx, externalUDITypes(i), expr.resultTerm)}"
        }

        val javaTerm = externalBoxedTermForType(externalAccType)
        val tmpAcc = newName("tmpAcc")
        val innerCode =
          s"""
             |  $javaTerm $tmpAcc = ${
            genToExternal(ctx, externalAccType, aggBufferNames(aggIndex)(0))};
             |  ${udaggs(agg)}.accumulate($tmpAcc, ${parameters.mkString(", ")});
             |  ${aggBufferNames(aggIndex)(0)} = ${genToInternal(ctx, externalAccType, tmpAcc)};
             |  ${aggBufVar.nullTerm} = false;
           """.stripMargin
        (innerCode, aggCall.filterArg)
    }.map({
      case (innerCode, filterArg) =>
        if (filterArg >= 0) {
          s"""
             |if ($inputTerm.getBoolean($filterArg)) {
             | $innerCode
             |}
          """.stripMargin
        } else {
          innerCode
        }
    }) mkString "\n"
  }

  /**
    * Generate codes which will read input and merge the aggregate buffers.
    */
  private[flink] def genMergeFlatAggregateBuffer(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      inputTerm: String,
      inputType: BaseRowType,
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      aggBufferExprs: Seq[GeneratedExpression]): String = {

    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(DataTypes.internal(inputType), inputTerm = inputTerm)

    // flat map to get flat agg buffers.
    aggregates.zipWithIndex.flatMap {
      case (agg: DeclarativeAggregateFunction, aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        agg.mergeExpressions.map(
          _.postOrderTransform(
            bindReference(isMerge = true, agg, idx, argsMapping, aggBufferTypes)))
      case (agg: UserDefinedAggregateFunction[_], aggIndex) =>
        val idx = auxGrouping.length + aggIndex
        Some(agg, idx)
    }.zip(aggBufferExprs.slice(auxGrouping.length, aggBufferExprs.size)).map {
      // DeclarativeAggregateFunction
      case ((expr: Expression), aggBufVar) =>
        val mergeExpr = exprCodegen.generateExpression(expr.toRexNode(builder))
        s"""
           |${mergeExpr.code}
           |${aggBufVar.nullTerm} = ${mergeExpr.nullTerm};
           |if (!${mergeExpr.nullTerm}) {
           |  ${mergeExpr.copyResultTermToTargetIfChanged(aggBufVar.resultTerm)}
           |}
           """.stripMargin.trim
      // UserDefinedAggregateFunction
      case ((agg: UserDefinedAggregateFunction[_], aggIndex: Int), aggBufVar) =>
        val (inputIndex, inputType) = argsMapping(aggIndex)(0)
        val inputRef = ResolvedAggInputReference(inputTerm, inputIndex, inputType)
        val inputExpr = exprCodegen.generateExpression(inputRef.toRexNode(builder))
        val singleIterableClass = classOf[SingleElementIterator[_]].getCanonicalName

        val externalAccT = getAccumulatorTypeOfAggregateFunction(agg)
        val javaField = externalBoxedTermForType(externalAccT)
        val tmpAcc = newName("tmpAcc")
        s"""
           |final $singleIterableClass accIt$aggIndex = new  $singleIterableClass();
           |accIt$aggIndex.set(${genToExternal(ctx, externalAccT, inputExpr.resultTerm)});
           |$javaField $tmpAcc = ${genToExternal(ctx, externalAccT, aggBufferNames(aggIndex)(0))};
           |${udaggs(agg)}.merge($tmpAcc, accIt$aggIndex);
           |${aggBufferNames(aggIndex)(0)} = ${genToInternal(ctx, externalAccT, tmpAcc)};
           |${aggBufVar.nullTerm} = ${aggBufferNames(aggIndex)(0)}IsNull || ${inputExpr.nullTerm};
         """.stripMargin
    } mkString "\n"
  }

  /**
    * Build an arg mapping for reference binding. The mapping will be a 2-dimension array.
    * The first dimension represents the aggregate index, the order is same with agg calls in plan.
    * The second dimension information represents input count of the aggregate. The meaning will
    * be different depends on whether we should do merge.
    *
    * In non-merge case, aggregate functions will treat inputs as operands. In merge case, the
    * input is local aggregation's buffer, we need to merge with our local aggregate buffers.
    */
  private[flink] def buildAggregateArgsMapping(
      isMerge: Boolean,
      aggBufferOffset: Int,
      inputRelDataType: RelDataType,
      auxGrouping: Array[Int],
      aggregateCalls: Seq[AggregateCall],
      aggBufferTypes: Array[Array[InternalType]]): Array[Array[(Int, InternalType)]] = {

    val auxGroupingMapping = auxGrouping.indices.map {
      i => Array[(Int, InternalType)]((i, aggBufferTypes(i)(0)))
    }.toArray

    val aggCallMapping = if (isMerge) {
      var offset = aggBufferOffset + auxGrouping.length
      aggBufferTypes.slice(auxGrouping.length, aggBufferTypes.length).map { types =>
        val baseOffset = offset
        offset = offset + types.length
        types.indices.map(index => (baseOffset + index, types(index))).toArray
      }
    } else {
      val mappingInputType = (index: Int) => FlinkTypeFactory.toInternalType(
        inputRelDataType.getFieldList.get(index).getType)
      aggregateCalls.map { call =>
        call.getArgList.asScala.map(i =>
          (i.intValue(), mappingInputType(i))).toArray
      }.toArray
    }

    auxGroupingMapping ++ aggCallMapping
  }

  private[flink] def buildAggregateAggBuffMapping(
      aggBufferTypes: Array[Array[InternalType]]): Array[Array[(Int, InternalType)]] = {
    var aggBuffOffset = 0
    val mapping = for (aggIndex <- aggBufferTypes.indices) yield {
      val types = aggBufferTypes(aggIndex)
      val indexes = (aggBuffOffset until aggBuffOffset + types.length).toArray
      aggBuffOffset += types.length
      indexes.zip(types)
    }
    mapping.toArray
  }

  private[flink] def registerUDAGGs(
      ctx: CodeGeneratorContext,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)]): Unit = {
    aggCallToAggFunction
        .map(_._2).filter(a => a.isInstanceOf[UserDefinedAggregateFunction[_]])
        .map(a => ctx.addReusableFunction(a))
  }

  // ===============================================================================================

  def addReusableConvertCollector(
      ctx: CodeGeneratorContext,
      resultType: DataType,
      withoutKey: Boolean = true,
      joinedRowTerm: String = "",
      lastKeyTerm:String = ""): String = {
    val COLLECTOR = AggsHandlerCodeGenerator.COLLECTOR
    val CONVERT_COLLECTOR_TYPE_TERM = AggsHandlerCodeGenerator.CONVERT_COLLECTOR_TYPE_TERM
    val CONVERTER_RESULT_TERM = AggsHandlerCodeGenerator.CONVERTER_RESULT_TERM
    val COLLECTOR_TERM = AggsHandlerCodeGenerator.COLLECTOR_TERM
    val STREAM_RECORD = OperatorCodeGenerator.STREAM_RECORD
    val OUT_ELEMENT = OperatorCodeGenerator.OUT_ELEMENT

    val preUnboxExprs = ctx.getReusableInputUnboxingExprs.clone()

    val baseRowConverterCode = CodeGenUtils.convertToBaseRow(
      ctx,
      CONVERTER_RESULT_TERM,
      "record",
      resultType,
      true,
      true)

    val collectCode = if (withoutKey) {
      s"""
         |this.out.collect($OUT_ELEMENT.replace($CONVERTER_RESULT_TERM));
          """
    } else {
      s"""
         |this.out.collect(
         |    $OUT_ELEMENT.replace(
         |        $joinedRowTerm.replace($lastKeyTerm, $CONVERTER_RESULT_TERM)));
       """.stripMargin
    }

    val code =
      j"""
          public class $CONVERT_COLLECTOR_TYPE_TERM implements $COLLECTOR {

              public $COLLECTOR<$STREAM_RECORD> out;

              @Override
              public void collect(Object record) throws Exception {
                    $baseRowConverterCode
                    $collectCode
              }

              @Override
              public void close() {
                   this.out.close();
              }
          }
      """

    ctx.setReusableInputUnboxingExprs(preUnboxExprs)

    ctx.addReusableInnerClass(CONVERT_COLLECTOR_TYPE_TERM, code)
    ctx.addReusableMember(
      s"public $CONVERT_COLLECTOR_TYPE_TERM $COLLECTOR_TERM;")
    ctx.addReusableOpenStatement(s"$COLLECTOR_TERM = new $CONVERT_COLLECTOR_TYPE_TERM();")
    ctx.addReusableOpenStatement(s"$COLLECTOR_TERM.out = output;")

    COLLECTOR_TERM
  }

  def genSortAggOutputExpr(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      aggBufferExprs: Seq[GeneratedExpression],
      outputType: BaseRowType,
      resultType: DataType = null,
      accType: DataType = null): GeneratedExpression = {
    val valueRow = CodeGenUtils.newName("valueRow")
    val resultCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
    if (isFinal) {
      if (isTableValuedAgg(aggregates)) {
        val otherFieldCode = ctx.reuseFieldCode()
        val collector = if (grouping.length == 0) {
          addReusableConvertCollector(ctx, resultType)
        } else {
          addReusableConvertCollector(ctx, resultType, false, joinedRow, lastKeyTerm)
        }
        val aggterm = udaggs(aggregates(0).asInstanceOf[UserDefinedAggregateFunction[_]])
        val code =
          s"$aggterm.emitValue(${genToExternal(ctx, accType, aggBufferNames(0)(0))}, $collector);"
        ctx.startNewFieldStatements("processElementAndEndInput")
        ctx.addAllReusableFields(otherFieldCode.split("\n").toSet)
        GeneratedExpression("", "", code, null)
      } else {
        val getValueExprs = genGetValueFromFlatAggregateBuffer(
          isMerge, ctx, config, builder, auxGrouping, aggregates, udaggs, argsMapping,
          aggBufferNames, aggBufferTypes, outputType)
        val valueRowType =
          new BaseRowType(classOf[GenericRow], getValueExprs.map(_.resultType): _*)
        resultCodegen.generateResultExpression(getValueExprs, valueRowType, valueRow)
      }
    } else {
      val valueRowType = new BaseRowType(classOf[GenericRow], aggBufferExprs.map(_.resultType): _*)
      resultCodegen.generateResultExpression(aggBufferExprs, valueRowType, valueRow)
    }
  }

  def genSortAggCodes(
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      builder: RelBuilder,
      grouping: Array[Int],
      auxGrouping: Array[Int],
      inputRelDataType: RelDataType,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggregates: Seq[UserDefinedFunction],
      udaggs: Map[UserDefinedAggregateFunction[_], String],
      inputTerm: String,
      inputType: BaseRowType,
      aggBufferNames: Array[Array[String]],
      aggBufferTypes: Array[Array[InternalType]],
      outputType: BaseRowType,
      forHashAgg: Boolean = false): (String, String, GeneratedExpression) = {
    // gen code to apply aggregate functions to grouping elements
    val argsMapping = buildAggregateArgsMapping(isMerge, grouping.length, inputRelDataType,
      auxGrouping, aggCallToAggFunction.map(_._1), aggBufferTypes)
    val aggBufferExprs = genFlatAggBufferExprs(isMerge, ctx, config, builder, auxGrouping,
      aggregates, argsMapping, aggBufferNames, aggBufferTypes)
    val initAggBufferCode = genInitFlatAggregateBuffer(ctx, config, builder, inputType, inputTerm,
      grouping, auxGrouping, aggregates, udaggs, aggBufferExprs, forHashAgg)
    val doAggregateCode = genAggregateByFlatAggregateBuffer(
      isMerge, ctx, config, builder, inputType, inputTerm, auxGrouping, aggCallToAggFunction,
      aggregates, udaggs, argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs)
    val aggOutputExpr = if (isTableValuedAgg(aggregates)) {
      val sqlFunction = aggCallToAggFunction.head._1.
        getAggregation.asInstanceOf[TableValuedAggSqlFunction]
      val resultType = sqlFunction.externalResultType
      val accType = sqlFunction.externalAccType
      genSortAggOutputExpr(
        isMerge, isFinal, ctx, config, builder, grouping, auxGrouping, aggregates, udaggs,
        argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs,
        outputType, resultType, accType)
    } else {
      genSortAggOutputExpr(
        isMerge, isFinal, ctx, config, builder, grouping, auxGrouping, aggregates, udaggs,
        argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs, outputType)
    }

    (initAggBufferCode, doAggregateCode, aggOutputExpr)
  }

  // ===============================================================================================

  private[flink] def generateOperator(
      ctx: CodeGeneratorContext,
      name: String,
      operatorBaseClass: String,
      processCode: String,
      endInputCode: String,
      inputRelDataType: RelDataType,
      config: TableConfig): GeneratedOperator = {
    ctx.addReusableMember("private boolean hasInput = false;")
    ctx.addReusableMember(s"$STREAM_RECORD element = new $STREAM_RECORD((Object)null);")
    OperatorCodeGenerator.generateOneInputStreamOperator(
      ctx,
      name,
      processCode,
      endInputCode,
      FlinkTypeFactory.toInternalBaseRowType(inputRelDataType, classOf[BaseRow]),
      config,
      lazyInputUnboxingCode = true)
  }

  val lastKeyTerm = "lastKey"

  val joinedRow = "joinedRow"

  private[flink] def isTableValuedAgg(aggregates: Seq[UserDefinedFunction]) =
    aggregates.size == 1 &&
      aggregates.head.isInstanceOf[TableValuedAggregateFunction[_, _]]
}
