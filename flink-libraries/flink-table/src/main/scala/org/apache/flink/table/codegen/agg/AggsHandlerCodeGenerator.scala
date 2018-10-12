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
import org.apache.flink.table.api.dataview.{ListView, MapView}
import org.apache.flink.table.codegen.CodeGenUtils.{className, newName}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{DeclarativeAggregateFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.plan.util.AggregateInfoList
import org.apache.flink.table.runtime.functions._
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

/**
  * A code generator for generating [[AggsHandleFunction]].
  */
class AggsHandlerCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    needRetract: Boolean,
    needMerge: Boolean,
    nullCheck: Boolean) {

  /** co table aggregate function contains two inputs **/
  private var input1Type: BaseRowType = _
  private var input1FieldTypes: Seq[InternalType] = _
  private var input2Type: BaseRowType = _
  private var input2FieldTypes: Seq[InternalType] = _

  /** constant expressions that act like a second input in the parameter indices. */
  private var constantExprs: Seq[GeneratedExpression] = Seq()

  /** window properties like window_start and window_end, only used in window aggregates */
  private var namespaceClassName: String = _
  private var windowProperties: Seq[WindowProperty] = Seq()
  private var hasNamespace: Boolean = false

  /** Aggregates informations */
  private var accTypeInfo: BaseRowType = _
  private var aggBufferSize: Int = _

  private var mergedAccExternalTypes: Array[DataType] = _
  private var mergedAccOffset: Int = 0
  private var mergedAccOnHeap: Boolean = false

  private var ignoreAggValues: Array[Int] = Array()

  /**
    * The [[aggBufferCodeGens]] and [[aggActionCodeGens]] will be both created when code generate
    * an [[AggsHandleFunction]] or [[SubKeyedAggsHandleFunction]]. They both contain all the
    * same AggCodeGens, but are different in the organizational form. The [[aggBufferCodeGens]]
    * flatten all the AggCodeGens in a flat format. The [[aggActionCodeGens]] organize all the
    * AggCodeGens in a tree format. If there is no distinct aggregate, the [[aggBufferCodeGens]]
    * and [[aggActionCodeGens]] are totally the same.
    */

  /**
    * The aggBufferCodeGens is organized according to the agg buffer order, which is in a flat
    * format, and is only used to generate the methods relative to accumulators, Such as
    * [[genCreateAccumulators()]], [[genGetAccumulators()]], [[genSetAccumulators()]].
    *
    * For example if we have :
    * count(*), count(distinct a), sum(a), sum(distinct a), max(distinct c)
    *
    * then the members of aggBufferCodeGens are organized looks like this:
    * +----------+-----------+--------+---------+---------+----------------+----------------+
    * | count(*) | count(a') | sum(a) | sum(a') | max(c') | distinct(a) a' | distinct(c) c' |
    * +----------+-----------+--------+---------+---------+----------------+----------------+
    * */
  private var aggBufferCodeGens: Array[AggCodeGen] = _

  /**
    * The aggActionCodeGens is organized according to the aggregate calling order, which is in
    * a tree format. Such as the aggregates distinct on the same fields should be accumulated
    * together when distinct is satisfied. And this is only used to generate the methods relative
    * to aggregate action. Such as [[genAccumulate()]], [[genRetract()]], [[genMerge()]].
    *
    * For example if we have :
    * count(*), count(distinct a), sum(a), sum(distinct a), max(distinct c)
    *
    * then the members of aggActionCodeGens are organized looks like this:
    *
    * +-------------------------------------------------------+
    * | count(*) | sum(a) | distinct(a) a'  | distinct(c) c'  |
    * |          |        |   |-- count(a') |   |-- max(c')   |
    * |          |        |   |-- sum(a')   |                 |
    * +-------------------------------------------------------+
    */
  private var aggActionCodeGens: Array[AggCodeGen] = _

  /**
    * Bind the input information, should be called before generating expression.
    */
  def bindInput(inputType: Seq[InternalType]): AggsHandlerCodeGenerator = {
    input1FieldTypes = inputType
    input1Type = new BaseRowType(classOf[BaseRow], inputType: _*)
    this
  }

  /**
    * In some cases, the expression will have two inputs (e.g. co table valued function). We should
    * bind second input information before use.
    */
  def bindSecondInput(inputType: Seq[InternalType]): AggsHandlerCodeGenerator = {
    input2FieldTypes = inputType
    input2Type = new BaseRowType(classOf[BaseRow], inputType: _*)
    this
  }

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

    val aggCodeGens = aggInfoList.aggInfos.map { aggInfo =>
      val filterExpr = if (null == aggInfo.agg) {
        None
      } else {
        createFilterExpression(
          aggInfo.agg.filterArg,
          aggInfo.aggIndex,
          aggInfo.agg.name)
      }

      val codegen = aggInfo.function match {
        case _: DeclarativeAggregateFunction =>
          new DeclarativeAggCodeGen(
            ctx,
            aggInfo,
            filterExpr,
            mergedAccOffset,
            aggBufferOffset,
            aggBufferSize,
            input1FieldTypes,
            constantExprs,
            relBuilder)
        case _: UserDefinedAggregateFunction[_] =>
          new ImperativeAggCodeGen(
            ctx,
            aggInfo,
            filterExpr,
            mergedAccOffset,
            aggBufferOffset,
            aggBufferSize,
            input1FieldTypes,
            Some(input2FieldTypes),
            constantExprs,
            relBuilder,
            hasNamespace,
            mergedAccOnHeap,
            mergedAccExternalTypes(aggBufferOffset))
      }
      aggBufferOffset = aggBufferOffset + aggInfo.externalAccTypes.length
      codegen
    }

    val distinctCodeGens = aggInfoList.distinctInfos.zipWithIndex.map {
      case (distinctInfo, index) =>
        val innerCodeGens =
          distinctInfo.aggIndexes.map(aggCodeGens(_)).toArray
        val filterExpr = createFilterExpression(
          distinctInfo.filterArg,
          index + aggCodeGens.length,
          "distinct aggregate")
        val codegen = new DistinctAggCodeGen(
          ctx,
          distinctInfo,
          index,
          innerCodeGens,
          filterExpr,
          mergedAccOffset,
          aggBufferOffset,
          aggBufferSize,
          hasNamespace,
          mergedAccOnHeap,
          distinctInfo.consumeRetraction,
          relBuilder)
        // distinct agg buffer occupies only one field
        aggBufferOffset += 1
        codegen
    }

    val distinctAggIndexes = aggInfoList.distinctInfos.flatMap(_.aggIndexes)
    val nonDistinctAggIndexes = aggCodeGens.indices.filter(!distinctAggIndexes.contains(_)).toArray

    this.aggBufferCodeGens = aggCodeGens ++ distinctCodeGens
    this.aggActionCodeGens = nonDistinctAggIndexes.map(aggCodeGens(_)) ++ distinctCodeGens

    // when input contains retractions, we inserted a count1 agg in the agg list
    // the count1 agg value shouldn't be in the aggregate result
    if (aggInfoList.count1AggIndex.nonEmpty && aggInfoList.count1AggInserted) {
      ignoreAggValues ++= Array(aggInfoList.count1AggIndex.get)
    }

    // the distinct value shouldn't be in the aggregate result
    if (aggInfoList.distinctInfos.nonEmpty) {
      ignoreAggValues ++= distinctCodeGens.indices.map(_ + aggCodeGens.length)
    }
  }

  /**
    * Creates filter argument access expression, none if no filter
    */
  private  def createFilterExpression(
    filterArg: Int,
    aggIndex: Int,
    aggName: String): Option[Expression] = {

    if (filterArg > 0) {
      val name = s"agg_${aggIndex}_filter"
      val filterType = input1FieldTypes(filterArg)
      if (filterType != DataTypes.BOOLEAN) {
        throw new TableException(s"filter arg must be boolean, but is $filterType, " +
                                   s"the aggregate is $aggName.")
      }
      Some(ResolvedAggInputReference(name, filterArg, input1FieldTypes(filterArg)))
    } else {
      None
    }
  }

  private def genCreateAccumulators(): String = {
    val methodName = "createAccumulators"
    ctx.startNewFieldStatements(methodName)

    // not need to bind input for ExprCodeGenerator
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
    val initAccExprs = aggBufferCodeGens.flatMap(_.createAccumulator(exprGenerator))
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
    val accExprs = aggBufferCodeGens.flatMap(_.getAccumulator(exprGenerator))
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
    val body = aggBufferCodeGens.map(_.setAccumulator(exprGenerator)).mkString("\n")

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
      .bindInput(input1Type, inputTerm = ACCUMULATE_INPUT_TERM)
    val body = aggActionCodeGens.map(_.accumulate(exprGenerator)).mkString("\n")
    s"""
       |${ctx.reuseFieldCode(methodName)}
       |${ctx.reuseInputUnboxingCode(Set(ACCUMULATE_INPUT_TERM))}
       |$body
    """.stripMargin
  }

  private def genCoAccumulate(): (String, String) = {
    // validation check
    checkNeededMethods(needAccumulate = true)

    val methodName1 = "accumulateLeft"
    ctx.startNewFieldStatements(methodName1)

    // bind input1 as inputRow
    val exprGenerator1 = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
      .bindInput(input1Type, inputTerm = ACCUMULATE_LEFT_INPUT_TERM)
    val body1 =
      aggActionCodeGens.map(_.accumulate(exprGenerator1)).mkString("\n")
    val ret1 = s"""
       |${ctx.reuseFieldCode(methodName1)}
       |${ctx.reuseInputUnboxingCode(Set(ACCUMULATE_LEFT_INPUT_TERM))}
       |$body1
    """.stripMargin

    val methodName2 = "accumulateRight"
    ctx.startNewFieldStatements(methodName2)

    // bind input2 as inputRow
    val exprGenerator2 = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
      .bindInput(input2Type, inputTerm = ACCUMULATE_RIGHT_INPUT_TERM)
    val body2 =
      aggActionCodeGens.map(_.accumulate(exprGenerator2)).mkString("\n")
    val ret2 = s"""
      |${ctx.reuseFieldCode(methodName2)}
      |${ctx.reuseInputUnboxingCode(Set(ACCUMULATE_RIGHT_INPUT_TERM))}
      |$body2
    """.stripMargin

    (ret1, ret2)
  }

  private def genRetract(): String = {
    if (needRetract) {
      // validation check
      checkNeededMethods(needRetract = true)

      val methodName = "retract"
      ctx.startNewFieldStatements(methodName)

      // bind input1 as inputRow
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
        .bindInput(input1Type, inputTerm = RETRACT_INPUT_TERM)
      val body = aggActionCodeGens.map(_.retract(exprGenerator)).mkString("\n")
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

  private def genCoRetract(): (String, String) = {
    if (needRetract) {
      // validation check
      checkNeededMethods(needRetract = true)

      val methodName1 = "retractLeft"
      ctx.startNewFieldStatements(methodName1)

      // bind input1 as inputRow
      val exprGenerator1 = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
        .bindInput(input1Type, inputTerm = RETRACT_LEFT_INPUT_TERM)
      val body1 = aggActionCodeGens.map(_.retract(exprGenerator1)).mkString("\n")
      val ret1 = s"""
         |${ctx.reuseFieldCode(methodName1)}
         |${ctx.reuseInputUnboxingCode(Set(RETRACT_LEFT_INPUT_TERM))}
         |$body1
      """.stripMargin

      val methodName2 = "retractRight"
      ctx.startNewFieldStatements(methodName2)

      // bind input2 as inputRow
      val exprGenerator2 = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck)
        .bindInput(input2Type, inputTerm = RETRACT_RIGHT_INPUT_TERM)
      val body2 = aggActionCodeGens.map(_.retract(exprGenerator2)).mkString("\n")
      val ret2 = s"""
         |${ctx.reuseFieldCode(methodName2)}
         |${ctx.reuseInputUnboxingCode(Set(RETRACT_RIGHT_INPUT_TERM))}
         |$body2
      """.stripMargin
      (ret1, ret2)
    } else {
      val errorMessage = genThrowException(
        "This function not require retract method, but the retract method is called.")
      (errorMessage, errorMessage)
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
      val body = aggActionCodeGens.map(_.merge(exprGenerator)).mkString("\n")
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

    var valueExprs = aggBufferCodeGens.zipWithIndex.filter { case (_, index) =>
      // ignore the count1 agg codegen and distinct agg codegen
      ignoreAggValues.isEmpty || !ignoreAggValues.contains(index)
    }.map {
      case (codegen, _) => codegen.getValue(exprGenerator)
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

  private def genEmitValue(): String = {
    val methodName = "EmitValue"
    ctx.startNewFieldStatements(methodName)

    // no need to bind input
    val exprGenerator =
      new ExprCodeGenerator(ctx,  INPUT_NOT_NULL, nullCheck)

    val code = aggBufferCodeGens.zipWithIndex.filter { case (_, index) =>
      // ignore the count1 agg codegen and distinct agg codegen
      ignoreAggValues.isEmpty || !ignoreAggValues.contains(index)
    }.map {
      case (codegen, _) => codegen.emitValue(exprGenerator)
    }.apply(0)

    s"""
       |${ctx.reuseFieldCode(methodName)}
       |$code
    """.stripMargin
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

          private  $EXECUTION_CONTEXT $CONTEXT_TERM;
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

  def generateTableValuedAggHandler(
    name: String,
    aggInfoList: AggregateInfoList): GeneratedTableValuedAggHandleFunction = {

    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val emitValueCode = genEmitValue()

    val functionName = newName(name)

    val baseRowConverterCode = CodeGenUtils.convertToBaseRow(
      ctx,
      CONVERTER_RESULT_TERM,
      "record",
      aggInfoList.aggInfos.head.externalResultType,
      true,
      true)

    val code =
      j"""
       public final class $functionName implements ${TABLEVALUED_AGG_HANDLER_FUNCTION} {

         private ${EXECUTION_CONTEXT} ${CONTEXT_TERM};
         private ${CONVERT_COLLECTOR_TYPE_TERM} ${MEMBER_COLLECTOR_TERM};
        ${ctx.reuseMemberCode()}

        public $functionName(java.lang.Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
          ${MEMBER_COLLECTOR_TERM} = new ${CONVERT_COLLECTOR_TYPE_TERM}();
        }

        @Override
        public void open(${EXECUTION_CONTEXT} ctx) throws Exception {
          this.${CONTEXT_TERM} = ctx;
          ${ctx.reuseOpenCode()}
        }

         @Override
         public void accumulate(${BASE_ROW} ${ACCUMULATE_INPUT_TERM}) throws Exception {
          $accumulateCode
         }

          @Override
          public void retract(${BASE_ROW} ${RETRACT_INPUT_TERM}) throws Exception {
            $retractCode
          }

           @Override
           public void merge(${BASE_ROW} ${MERGED_ACC_TERM}) throws Exception {
             $mergeCode
           }

           @Override
           public void setAccumulators(${BASE_ROW} ${ACC_TERM}) throws Exception {
             $setAccumulatorsCode
           }

           @Override
           public ${BASE_ROW} getAccumulators() throws Exception {
             $getAccumulatorsCode
           }

           @Override
           public ${BASE_ROW} createAccumulators() throws Exception {
             $createAccumulatorsCode
           }

           @Override
           public void emitValue(${COLLECTOR}<${BASE_ROW}> ${COLLECTOR_TERM}) throws Exception {
             ${MEMBER_COLLECTOR_TERM}.out = ${COLLECTOR_TERM};
             $emitValueCode
           }

           @Override
           public void cleanup() throws Exception {
             ${BASE_ROW} ${CURRENT_KEY} = ctx.currentKey();
             ${ctx.reuseCleanupCode()}
           }

           @Override
           public void close() throws Exception {
             ${ctx.reuseCloseCode()}
           }

           public class ${CONVERT_COLLECTOR_TYPE_TERM} implements ${COLLECTOR} {

              public ${COLLECTOR}<${BASE_ROW}> out;

              @Override
              public void collect(Object record) throws Exception {
                    ${baseRowConverterCode}
                    out.collect($CONVERTER_RESULT_TERM);
                }

               @Override
               public void close() {
                out.close();
               }
        }
     }
     """.stripMargin

    GeneratedTableValuedAggHandleFunction(functionName, code, ctx.references.toArray)
  }

  /**
    * Generate [[GeneratedCoTableValuedAggHandleFunction]] with the given function name and
    * aggregate infos.
    */
  def generateCoTableValuedAggHandler(
    name: String,
    aggInfoList: AggregateInfoList): GeneratedCoTableValuedAggHandleFunction = {

    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val (accumulateLeftCode, accumulateRightCode) = genCoAccumulate()
    val (retractLeftCode, retractRightCode) = genCoRetract()
    val mergeCode = genMerge()
    val emitValueCode = genEmitValue()

    val functionName = newName(name)

    val baseRowConverterCode = CodeGenUtils.convertToBaseRow(
      ctx,
      CONVERTER_RESULT_TERM,
      "record",
      aggInfoList.aggInfos.head.externalResultType,
      true,
      true)

    val code =
      j"""
       public final class $functionName implements $COTABLEVALUED_AGG_HANDLER_FUNCTION {

         private $EXECUTION_CONTEXT $CONTEXT_TERM;
         private $CONVERT_COLLECTOR_TYPE_TERM $MEMBER_COLLECTOR_TERM;
        ${ctx.reuseMemberCode()}

        public $functionName(java.lang.Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
          $MEMBER_COLLECTOR_TERM = new $CONVERT_COLLECTOR_TYPE_TERM(references);
        }

        @Override
        public void open($EXECUTION_CONTEXT ctx) throws Exception {
          this.$CONTEXT_TERM = ctx;
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void accumulateLeft($BASE_ROW $ACCUMULATE_LEFT_INPUT_TERM) throws Exception {
         $accumulateLeftCode
        }

        @Override
        public void accumulateRight($BASE_ROW $ACCUMULATE_RIGHT_INPUT_TERM) throws Exception {
         $accumulateRightCode
        }

        @Override
        public void retractLeft($BASE_ROW $RETRACT_LEFT_INPUT_TERM) throws Exception {
          $retractLeftCode
        }

        @Override
        public void retractRight($BASE_ROW $RETRACT_RIGHT_INPUT_TERM) throws Exception {
          $retractRightCode
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
        public void emitValue($COLLECTOR<$BASE_ROW> $COLLECTOR_TERM) throws Exception {
          $MEMBER_COLLECTOR_TERM.out = $COLLECTOR_TERM;
          $emitValueCode
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

        public static class $CONVERT_COLLECTOR_TYPE_TERM implements $COLLECTOR {

           public $COLLECTOR<$BASE_ROW> out;
           ${ctx.reuseMemberCode()}

           public $CONVERT_COLLECTOR_TYPE_TERM (java.lang.Object[] references)
           throws Exception {
               ${ctx.reuseInitCode()}
           }

           @Override
           public void collect(Object record) throws Exception {
                 ${baseRowConverterCode}
                 out.collect($CONVERTER_RESULT_TERM);
           }

           @Override
           public void close() {
            out.close();
           }
        }
     }
     """.stripMargin

    GeneratedCoTableValuedAggHandleFunction(functionName, code, ctx.references.toArray)
  }

  private def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false): Unit = {
    // check and validate the needed methods
    aggActionCodeGens.foreach(
      _.checkNeededMethods(needAccumulate, needRetract, needMerge, needReset))
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
  val TABLEVALUED_AGG_HANDLER_FUNCTION: String = className[TableValuedAggHandleFunction]
  val COTABLEVALUED_AGG_HANDLER_FUNCTION: String = className[CoTableValuedAggHandleFunction]
  val SUB_KEYED_AGGS_HANDLER_FUNCTION: String = className[SubKeyedAggsHandleFunction[_]]
  val EXECUTION_CONTEXT:String = className[ExecutionContext]

  // for table-valued
  val COLLECTOR:String = className[Collector[_]]

  /** static terms **/
  val ACC_TERM = "acc"
  val MERGED_ACC_TERM = "otherAcc"
  val ACCUMULATE_INPUT_TERM = "accInput"
  val ACCUMULATE_LEFT_INPUT_TERM = "accLeftInput"
  val ACCUMULATE_RIGHT_INPUT_TERM = "accRightInput"
  val RETRACT_INPUT_TERM = "retractInput"
  val RETRACT_LEFT_INPUT_TERM = "retractLeftInput"
  val RETRACT_RIGHT_INPUT_TERM = "retractRightInput"
  val DISTINCT_KEY_TERM = "distinctKey"

  val NAMESPACE_TERM = "namespace"
  val CONTEXT_TERM = "ctx"
  val CURRENT_KEY = "currentKey"

  val INPUT_NOT_NULL = false

  // for table-valued
  val COLLECTOR_TERM = "out"
  val MEMBER_COLLECTOR_TERM = "externalCollector"
  val CONVERT_COLLECTOR_TYPE_TERM = "ConvertCollector"
  val RESULT_NULL_CHECK = true
  val CONVERTER_RESULT_TERM = "baseRowTrem"

  /**
    * Create DataView term, for example, acc1_map_dataview.
    *
    * @return term to access [[MapView]] or [[ListView]]
    */
  def createDataViewTerm(spec: DataViewSpec): String = {
    s"${spec.stateId}_dataview"
  }

  /**
    * Create DataView backup term, for example, acc1_map_dataview_backup.
    * The backup dataview term is used for merging two statebackend
    * dataviews, e.g. session window.
    *
    * @return term to access backup [[MapView]] or [[ListView]]
    */
  def createDataViewBackupTerm(spec: DataViewSpec): String = {
    s"${spec.stateId}_dataview_backup"
  }

  def addReusableStateDataViews(
    ctx: CodeGeneratorContext,
    hasNamespace: Boolean,
    viewSpecs: Array[DataViewSpec]): Unit = {
    // add reusable dataviews to context
    viewSpecs.foreach { spec =>
      val viewFieldTerm = createDataViewTerm(spec)
      val backupViewTerm = createDataViewBackupTerm(spec)
      val viewTypeTerm = spec.getStateDataViewClass(hasNamespace).getCanonicalName
      ctx.addReusableMember(s"private $viewTypeTerm $viewFieldTerm;")
      // always create backup dataview
      ctx.addReusableMember(s"private $viewTypeTerm $backupViewTerm;")

      val descTerm = ctx.addReusableObject(spec.toStateDescriptor, "desc")
      val createStateCall = spec.getCreateStateCall(hasNamespace)

      val openCode =
        s"""
           |$viewFieldTerm = new $viewTypeTerm($CONTEXT_TERM.$createStateCall($descTerm));
           |$backupViewTerm = new $viewTypeTerm($CONTEXT_TERM.$createStateCall($descTerm));
         """.stripMargin
      ctx.addReusableOpenStatement(openCode)

      // only cleanup dataview term, do not cleanup backup
      val cleanupCode = if (hasNamespace) {
        s"""
           |$viewFieldTerm.setKeyNamespace($CURRENT_KEY, $NAMESPACE_TERM);
           |$viewFieldTerm.clear();
        """.stripMargin
      } else {
        s"""
           |$viewFieldTerm.setKey($CURRENT_KEY);
           |$viewFieldTerm.clear();
        """.stripMargin
      }
      ctx.addReusableCleanupStatement(cleanupCode)
    }
  }

  def getMethodName(inputTerm: String, defaultName: String): String = {
    inputTerm match {
      case AggsHandlerCodeGenerator.ACCUMULATE_INPUT_TERM => "accumulate"
      case AggsHandlerCodeGenerator.ACCUMULATE_LEFT_INPUT_TERM => "accumulateLeft"
      case AggsHandlerCodeGenerator.ACCUMULATE_RIGHT_INPUT_TERM => "accumulateRight"
      case AggsHandlerCodeGenerator.RETRACT_INPUT_TERM => "retract"
      case AggsHandlerCodeGenerator.RETRACT_LEFT_INPUT_TERM => "retractLeft"
      case AggsHandlerCodeGenerator.RETRACT_RIGHT_INPUT_TERM => "retractRight"
      case _ => defaultName
    }
  }

  def isRightMethod(inputTerm: String): Boolean = {
    inputTerm match {
      case AggsHandlerCodeGenerator.ACCUMULATE_RIGHT_INPUT_TERM => true
      case AggsHandlerCodeGenerator.RETRACT_RIGHT_INPUT_TERM => true
      case _ => false
    }
  }
}
