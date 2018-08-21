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

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.TableException
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{AggregateFunction, DeclarativeAggregateFunction}
import org.apache.flink.table.plan.util.{AggregateInfo, AggregateInfoList}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext, SubKeyedAggsHandleFunction}
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

/**
  * A code generator for generating [[AggsHandleFunction]].
  */
class AggsHandlerCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    inputFieldTypes: Seq[InternalType],
    needRetract: Boolean,
    needMerge: Boolean,
    nullCheck: Boolean) {

  private val INPUT_NOT_NULL = false

  private val inputType = new BaseRowType(classOf[BaseRow], inputFieldTypes: _*)

  /** constant expressions that act like a second input in the parameter indices. */
  private var constantExprs: Seq[GeneratedExpression] = Seq()

  /** window properties like window_start and window_end, only used in window aggregates */
  private var namespaceClassName: String = _
  private var windowProperties: Seq[WindowProperty] = Seq()
  private var hasNamespace: Boolean = false

  /** Aggregates informations */
  private var accTypeInfo: BaseRowType = _
  private var aggBufferSize: Int = _
  private var aggCodeGens: Array[AggCodeGen] = _

  private var mergedAccExternalTypes: Array[DataType] = _
  private var mergedAccOffset: Int = 0
  private var mergedAccOnHeap: Boolean = false

  private var ignoreAggValue: Option[Int] = None

  /**
    * Adds constant expressions that act like a second input in the parameter indices.
    */
  def withConstants(literals: Seq[RexLiteral]): AggsHandlerCodeGenerator = {
    // create constants
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
    val exprs = literals.map(exprGenerator.generateExpression)
    this.constantExprs = exprs.map(ctx.addReusableConstant(_, nullCheck))
    this
  }

  /**
    * Sets merged accumulator information.
    *
    * @param mergedAccOffset the mergedAcc may come from local aggregate,
    *                         this is the first buffer offset in the row
    * @param mergedAccOnHeap true if the mergedAcc is on heap, otherwise
    * @param mergedAccExternalTypes the merged acc types
    */
  def withMerging(
      mergedAccOffset: Int,
      mergedAccOnHeap: Boolean,
      mergedAccExternalTypes: Array[DataType]): AggsHandlerCodeGenerator = {
    this.mergedAccOffset = mergedAccOffset
    this.mergedAccOnHeap = mergedAccOnHeap
    this.mergedAccExternalTypes = mergedAccExternalTypes
    this
  }

  /**
    * Adds window properties such as window_start, window_end
    */
  private def initialWindowProperties(
      windowProperties: Seq[WindowProperty],
      windowClass: Class[_]): Unit = {
    this.windowProperties = windowProperties
    this.namespaceClassName = windowClass.getCanonicalName
    this.hasNamespace = true
  }

  /**
    * Adds aggregate infos into context
    */
  private def initialAggregateInformation(aggInfoList: AggregateInfoList): Unit = {

    this.accTypeInfo = new BaseRowType(
      classOf[GenericRow],
      aggInfoList.getAccTypes.map(DataTypes.internal): _*)
    this.aggBufferSize = accTypeInfo.getArity
    var aggBufferOffset: Int = 0

    if (mergedAccExternalTypes == null) {
      mergedAccExternalTypes = aggInfoList.getAccTypes
    }

    this.aggCodeGens = aggInfoList.aggInfos.map { aggInfo =>
      val codegen = aggInfo.function match {
        case _: DeclarativeAggregateFunction =>
          new DeclarativeAggCodeGen(
            ctx,
            aggInfo,
            createFilterExpression(aggInfo),
            mergedAccOffset,
            aggBufferOffset,
            aggBufferSize,
            inputFieldTypes,
            constantExprs,
            relBuilder)
        case _: AggregateFunction[_, _] =>
          new ImperativeAggCodeGen(
            ctx,
            aggInfo,
            createFilterExpression(aggInfo),
            mergedAccOffset,
            aggBufferOffset,
            aggBufferSize,
            inputFieldTypes,
            constantExprs,
            relBuilder,
            hasNamespace,
            mergedAccOnHeap,
            mergedAccExternalTypes(aggBufferOffset))
      }
      aggBufferOffset = aggBufferOffset + aggInfo.externalAccTypes.length
      codegen
    }

    // when input contains retractions, we inserted a count1 agg in the agg list
    // the count1 agg value shouldn't be in the aggregate result
    if (aggInfoList.count1AggIndex.nonEmpty && aggInfoList.count1AggInserted) {
      ignoreAggValue = Some(aggInfoList.count1AggIndex.get)
    }
  }

  /**
    * Creates filter argument access expression, none if no filter
    */
  private def createFilterExpression(aggInfo: AggregateInfo): Option[Expression] = {
    val filterArg = aggInfo.agg.filterArg
    if (filterArg > 0) {
      val name = s"agg_${aggInfo.aggIndex}_filter"
      val filterType = inputFieldTypes(filterArg)
      if (filterType != DataTypes.BOOLEAN) {
        throw new TableException(s"filter arg must be boolean, but is $filterArg, " +
                                   s"the aggregate is ${aggInfo.agg.toString}.")
      }
      Some(ResolvedAggInputReference(name, filterArg, inputFieldTypes(filterArg)))
    } else {
      None
    }
  }

  /**
    * Generate [[GeneratedAggsHandleFunction]] with the given function name and aggregate infos.
    */
  def generateAggsHandler(
      name: String,
      aggInfoList: AggregateInfoList): GeneratedAggsHandleFunction = {

    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val getValueCode = genGetValue()

    val functionName = newName(name)

    val functionCode =
      j"""
        public final class $functionName implements $AGGS_HANDLER_FUNCTION {

          private $EXECUTION_CONTEXT $CONTEXT_TERM;
          ${ctx.reuseMemberCode()}

          public $functionName(java.lang.Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open($EXECUTION_CONTEXT ctx) throws Exception {
            this.$CONTEXT_TERM = ctx;
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($BASE_ROW $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($BASE_ROW $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge($BASE_ROW $MERGED_ACC_TERM) throws Exception {
            $mergeCode
          }

          @Override
          public void setAccumulators($BASE_ROW $ACC_TERM) throws Exception {
            $setAccumulatorsCode
          }

          @Override
          public $BASE_ROW getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $BASE_ROW createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public $BASE_ROW getValue() throws Exception {
            $getValueCode
          }

          @Override
          public void cleanup() throws Exception {
            $BASE_ROW $CURRENT_KEY = ctx.currentKey();
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin

    GeneratedAggsHandleFunction(functionName, functionCode, ctx.references.toArray)
  }

  /**
    * Generate [[GeneratedAggsHandleFunction]] with the given function name and aggregate infos
    * and window properties.
    */
  def generateSubKeyedAggsHandler[N](
      name: String,
      aggInfoList: AggregateInfoList,
      windowProperties: Seq[WindowProperty],
      windowClass: Class[N]): GeneratedSubKeyedAggsHandleFunction[N] = {

    initialWindowProperties(windowProperties, windowClass)
    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val getValueCode = genGetValue()

    val functionName = newName(name)

    val functionCode =
      j"""
        public final class $functionName
          implements $SUB_KEYED_AGGS_HANDLER_FUNCTION<$namespaceClassName> {

          private $EXECUTION_CONTEXT $CONTEXT_TERM;
          ${ctx.reuseMemberCode()}

          public $functionName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open($EXECUTION_CONTEXT ctx) throws Exception {
            this.$CONTEXT_TERM = ctx;
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($BASE_ROW $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($BASE_ROW $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge(Object ns, $BASE_ROW $MERGED_ACC_TERM) throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $mergeCode
          }

          @Override
          public void setAccumulators(Object ns, $BASE_ROW $ACC_TERM)
          throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $setAccumulatorsCode
          }

          @Override
          public $BASE_ROW getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $BASE_ROW createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public $BASE_ROW getValue(Object ns) throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $getValueCode
          }

          @Override
          public void cleanup(Object ns) throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $BASE_ROW $CURRENT_KEY = ctx.currentKey();
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin

    GeneratedSubKeyedAggsHandleFunction(functionName, functionCode, ctx.references.toArray)
  }

  private def genCreateAccumulators(): String = {
    val methodName = "createAccumulators"
    ctx.startNewFieldStatements(methodName)

    // not need to bind input for ExprCodeGenerator
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
    val initAccExprs = aggCodeGens.flatMap(_.createAccumulator(exprGenerator))
    val accTerm = newName("acc")
    val resultExpr = exprGenerator.generateResultExpression(
      initAccExprs,
      DataTypes.internal(accTypeInfo).asInstanceOf[BaseRowType],
      outRow = accTerm,
      reusedOutRow = false)

    s"""
      |${ctx.reuseFieldCode(methodName)}
      |${resultExpr.code}
      |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def genGetAccumulators(): String = {
    val methodName = "getAccumulators"
    ctx.startNewFieldStatements(methodName)

    // no need to bind input
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
    val accExprs = aggCodeGens.flatMap(_.getAccumulator(exprGenerator))
    val accTerm = newName("acc")
    // always create a new accumulator row
    val resultExpr = exprGenerator.generateResultExpression(
      accExprs,
      DataTypes.internal(accTypeInfo).asInstanceOf[BaseRowType],
      outRow = accTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseFieldCode(methodName)}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def genSetAccumulators(): String = {
    val methodName = "setAccumulators"
    ctx.startNewFieldStatements(methodName)

    // bind input1 as accumulators
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
      .bindInput(accTypeInfo, inputTerm = ACC_TERM)
    val body = aggCodeGens.map(_.setAccumulator(exprGenerator)).mkString("\n")

    s"""
      |${ctx.reuseFieldCode(methodName)}
      |${ctx.reuseInputUnboxingCode(Set(ACC_TERM))}
      |$body
    """.stripMargin
  }

  private def genAccumulate(): String = {
    // validation check
    checkNeededMethods(needAccumulate = true)

    val methodName = "accumulate"
    ctx.startNewFieldStatements(methodName)

    // bind input1 as inputRow
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
      .bindInput(inputType, inputTerm = ACCUMULATE_INPUT_TERM)
    val body = aggCodeGens.map(_.accumulate(exprGenerator)).mkString("\n")
    s"""
       |${ctx.reuseFieldCode(methodName)}
       |${ctx.reuseInputUnboxingCode(Set(ACCUMULATE_INPUT_TERM))}
       |$body
    """.stripMargin
  }

  private def genRetract(): String = {
    if (needRetract) {
      // validation check
      checkNeededMethods(needRetract = true)

      val methodName = "retract"
      ctx.startNewFieldStatements(methodName)

      // bind input1 as inputRow
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
        .bindInput(inputType, inputTerm = RETRACT_INPUT_TERM)
      val body = aggCodeGens.map(_.retract(exprGenerator)).mkString("\n")
      s"""
         |${ctx.reuseFieldCode(methodName)}
         |${ctx.reuseInputUnboxingCode(Set(RETRACT_INPUT_TERM))}
         |$body
      """.stripMargin
    } else {
      genThrowException(
        "This function not require retract method, but the retract method is called.")
    }
  }

  private def genMerge(): String = {
    if (needMerge) {
      // validation check
      checkNeededMethods(needMerge = true)

      val methodName = "merge"
      ctx.startNewFieldStatements(methodName)

      // the mergedAcc is partial of mergedInput, such as <key, acc> in local-global, ignore keys
      val mergedAccType = if (mergedAccOffset > 0) {
        // concat padding types and acc types, use int type as padding
        // the padding types will be ignored
        val padding = Array.range(0, mergedAccOffset).map(_ => DataTypes.INT)
        val typeInfo = padding ++ mergedAccExternalTypes
        new BaseRowType(classOf[GenericRow], typeInfo.map(DataTypes.internal): _*)
      } else {
        new BaseRowType(classOf[GenericRow], mergedAccExternalTypes.map(DataTypes.internal): _*)
      }

      // bind input1 as otherAcc
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
        .bindInput(mergedAccType, inputTerm = MERGED_ACC_TERM)
      val body = aggCodeGens.map(_.merge(exprGenerator)).mkString("\n")
      s"""
         |${ctx.reuseFieldCode(methodName)}
         |${ctx.reuseInputUnboxingCode(Set(MERGED_ACC_TERM))}
         |$body
      """.stripMargin
    } else {
      genThrowException(
        "This function not require merge method, but the merge method is called.")
    }
  }


  private def genGetValue(): String = {
    val methodName = "getValue"
    ctx.startNewFieldStatements(methodName)

    // no need to bind input
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)

    var valueExprs = aggCodeGens.zipWithIndex.filter { case (_, index) =>
      // filter the count1 agg codegen
      ignoreAggValue.isEmpty || ignoreAggValue.get != index
    }.map { case (codegen, _) =>
      codegen.getValue(exprGenerator)
    }

    if (hasNamespace) {
      // append window property results
      val windowExprs = windowProperties.map {
        case w: WindowStart =>
          // return a Timestamp(Internal is long)
          GeneratedExpression(
            s"$NAMESPACE_TERM.getStart()", "false", "", DataTypes.internal(w.resultType))
        case w: WindowEnd =>
          // return a Timestamp(Internal is long)
          GeneratedExpression(
            s"$NAMESPACE_TERM.getEnd()", "false", "", DataTypes.internal(w.resultType))
        case r: RowtimeAttribute =>
          // return a rowtime, use long as internal type
          GeneratedExpression(
            s"$NAMESPACE_TERM.getEnd() - 1", "false", "", DataTypes.internal(r.resultType))
        case p: ProctimeAttribute =>
          // ignore this property, it will be null at the position later
          GeneratedExpression("-1L", "true", "", DataTypes.internal(p.resultType))
      }
      valueExprs = valueExprs ++ windowExprs
    }

    val aggValueTerm = newName("aggValue")
    val valueType = new BaseRowTypeInfo(classOf[GenericRow],
      valueExprs.map(_.resultType).map(DataTypes.toTypeInfo): _*)

    // always create a new result row
    val resultExpr = exprGenerator.generateResultExpression(
      valueExprs,
      DataTypes.internal(valueType).asInstanceOf[BaseRowType],
      outRow = aggValueTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseFieldCode(methodName)}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false): Unit = {
    // check and validate the needed methods
    aggCodeGens.foreach(_.checkNeededMethods(needAccumulate, needRetract, needMerge, needReset))
  }

  private def genThrowException(msg: String): String = {
    s"""
       |throw new java.lang.RuntimeException("$msg");
     """.stripMargin
  }
}

object AggsHandlerCodeGenerator {
  /** static class names */
  val BASE_ROW: String = className[BaseRow]
  val GENERIC_ROW: String = className[GenericRow]
  val AGGS_HANDLER_FUNCTION: String = className[AggsHandleFunction]
  val SUB_KEYED_AGGS_HANDLER_FUNCTION: String = className[SubKeyedAggsHandleFunction[_]]
  val EXECUTION_CONTEXT:String = className[ExecutionContext]

  /** static terms **/
  val ACC_TERM = "acc"
  val MERGED_ACC_TERM = "otherAcc"
  val ACCUMULATE_INPUT_TERM = "accInput"
  val RETRACT_INPUT_TERM = "retractInput"

  val NAMESPACE_TERM = "namespace"
  val CONTEXT_TERM = "ctx"
  val CURRENT_KEY = "currentKey"
}
