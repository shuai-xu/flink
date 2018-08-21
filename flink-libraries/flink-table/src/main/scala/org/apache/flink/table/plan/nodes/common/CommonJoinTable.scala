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
package org.apache.flink.table.plan.nodes.common

import java.util
import java.util.concurrent.TimeUnit

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.operators.ProcessOperator
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen._
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.runtime.join._
import org.apache.flink.table.sources.{DimensionTableSource, IndexKey}
import org.apache.flink.table.types.{BaseRowType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConverters._

/**
 * Flink RelNode which matches along with stream joins a dimension table
 */
abstract class CommonJoinTable(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputSchema: BaseRowSchema,
    input: RelNode,
    val tableSource: DimensionTableSource[_],
    tableSourceSchema: BaseRowSchema,
    calcProgram: Option[RexProgram],
    period: Option[RexNode],
    lookupKeyPairs: util.List[IntPair],
    constantLookupKeys: util.Map[Int, (InternalType, Object)],
    joinCondition: Option[RexNode],
    checkedIndex: IndexKey,
    schema: BaseRowSchema,
    val joinInfo: JoinInfo,
    val joinType: JoinRelType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with CommonCalc
  with FlinkRelNode {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def toString: String = {
    joinToString(schema.relDataType, joinCondition, getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    joinExplainTerms(
      super.explainTerms(pw),
      schema.relDataType,
      joinCondition,
      getExpressionString)
  }

  protected def translateToPlanInternal(
      inputTransformation: StreamTransformation[BaseRow],
      env: StreamExecutionEnvironment,
      config: TableConfig): StreamTransformation[BaseRow] = {

    // we do not need to specify input type
    val leftOuterJoin = if (joinType == JoinRelType.LEFT) true else false
    val operatorName = joinToString(schema.relDataType, joinCondition, getExpressionString)

    val inputBaseRowType = inputSchema.internalType(classOf[BaseRow])
    val tableBaseRowType = tableSourceSchema.internalType(classOf[GenericRow])
    val resultBaseRowType = schema.internalType(classOf[JoinedRow])
    val resultBaseRowTypeInfo = schema.typeInfo(classOf[JoinedRow])

    val indexColumnInDefineOrder: List[Int] = checkedIndex.getDefinedColumns.asScala
      .map(f => f: Int).toList
    val leftKeyIdx2KeyRowIdx: List[(Int, Int)] = lookupKeyPairs.asScala
      .map{k => (k.source, indexColumnInDefineOrder.indexOf(k.target))}.toList

    val operator = if (tableSource.isAsync) {
      val asyncFetcher = tableSource
        .getAsyncLookupFunction(checkedIndex)
        .asInstanceOf[AsyncFunction[BaseRow, BaseRow]]
      val asyncConfig = tableSource.getAsyncConfig
      val asyncFunc = if (calcProgram.isDefined) {
        // a projection or filter after table source scan
        val calcSchema = new BaseRowSchema(calcProgram.get.getOutputRowType)
        val rightTypeInfo = calcSchema.internalType(classOf[GenericRow])
        val collector = generateAsyncCollector(
          config,
          inputBaseRowType,
          rightTypeInfo)
        val calcMap = generateCalcMap(config)
        new JoinTableWithCalcAsyncRunner(
          asyncFetcher,
          calcMap.name,
          calcMap.code,
          collector.name,
          collector.code,
          env.getConfig.isObjectReuseEnabled,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          indexColumnInDefineOrder,
          leftKeyIdx2KeyRowIdx,
          constantLookupKeys,
          resultBaseRowTypeInfo)
      } else {
        val collector = generateAsyncCollector(
          config,
          inputBaseRowType,
          tableBaseRowType)
        new JoinTableAsyncRunner(
          asyncFetcher,
          collector.name,
          collector.code,
          env.getConfig.isObjectReuseEnabled,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          indexColumnInDefineOrder,
          leftKeyIdx2KeyRowIdx,
          constantLookupKeys,
          resultBaseRowTypeInfo)
      }

      val timeout = TimeUnit.MILLISECONDS.toMillis(asyncConfig.timeoutMs)
      val mode = if (asyncConfig.orderedMode) OutputMode.ORDERED else OutputMode.UNORDERED

      new AsyncWaitOperator(asyncFunc, timeout, asyncConfig.bufferCapacity, mode)
    } else {
      // sync join
      val fetcher = tableSource.getLookupFunction(checkedIndex)
        .asInstanceOf[FlatMapFunction[BaseRow, BaseRow]]

      val ctx = CodeGeneratorContext(config)
      val processFunc = if (calcProgram.isDefined) {
        // a projection or filter after table source scan
        val calcSchema = new BaseRowSchema(calcProgram.get.getOutputRowType)
        val rightTypeInfo = calcSchema.internalType(classOf[GenericRow])
        val collector = generateCollector(
          ctx,
          config,
          inputBaseRowType,
          rightTypeInfo,
          resultBaseRowType,
          joinCondition,
          None)
        val calcMap = generateCalcMap(config)
        new JoinTableWithCalcProcessRunner(
          fetcher,
          calcMap.name,
          calcMap.code,
          collector.name,
          collector.code,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          indexColumnInDefineOrder,
          leftKeyIdx2KeyRowIdx,
          constantLookupKeys,
          resultBaseRowTypeInfo)
      } else {
        val collector = generateCollector(
          ctx,
          config,
          inputBaseRowType,
          tableBaseRowType,
          resultBaseRowType,
          joinCondition,
          None)
        new JoinTableProcessRunner(
          fetcher,
          collector.name,
          collector.code,
          leftOuterJoin,
          inputSchema.fieldTypes.toArray,
          indexColumnInDefineOrder,
          leftKeyIdx2KeyRowIdx,
          constantLookupKeys,
          resultBaseRowTypeInfo)
      }
      new ProcessOperator(processFunc)
    }

    new OneInputTransformation(
      inputTransformation,
      operatorName,
      operator,
      DataTypes.toTypeInfo(resultBaseRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      inputTransformation.getParallelism)
  }

  private def generateAsyncCollector(
      config: TableConfig,
      inputType: BaseRowType,
      tableType: BaseRowType): GeneratedCollector = {

    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val tableInputTerm = CodeGeneratorContext.DEFAULT_INPUT2_TERM

    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(tableType, inputTerm = tableInputTerm)

    val tableResultExpr = exprGenerator.generateConverterResultExpression(tableType)

    val body =
      s"""
         |${tableResultExpr.code}
         |getCollector().complete(java.util.Collections.singleton(${tableResultExpr.resultTerm}));
      """.stripMargin

    val collectorCode = if (joinCondition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(inputType, inputTerm)
        .bindSecondInput(tableType, tableInputTerm)
      val filterCondition = filterGenerator.generateExpression(joinCondition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |} else {
         |  getCollector().complete(java.util.Collections.emptyList());
         |}
         |""".stripMargin
    }

    CollectorCodeGenerator.generateTableAsyncCollector(
      ctx,
      "TableAsyncCollector",
      collectorCode,
      inputType,
      tableType,
      config)
  }

  /**
    * Differs from CommonCorrelate.generateCollector which has no real condition because of
    * FLINK-7865, here we should deal with outer join type when real conditions filtered result.
    */
  private def generateCollector(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputType: BaseRowType,
      udtfTypeInfo: BaseRowType,
      resultType: BaseRowType,
      condition: Option[RexNode],
      pojoFieldMapping: Option[Array[Int]],
      retainHeader: Boolean = true): GeneratedCollector = {
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val udtfInputTerm = CodeGeneratorContext.DEFAULT_INPUT2_TERM

    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(udtfTypeInfo, inputTerm = udtfInputTerm, inputFieldMapping = pojoFieldMapping)

    val udtfResultExpr = exprGenerator.generateConverterResultExpression(udtfTypeInfo)

    val joinedRowTerm = CodeGenUtils.newName("joinedRow")
    ctx.addOutputRecord(resultType, joinedRowTerm)

    val header = if (retainHeader) {
      s"$joinedRowTerm.setHeader($inputTerm.getHeader());"
    } else {
      ""
    }

    val body =
      s"""
         |${udtfResultExpr.code}
         |$joinedRowTerm.replace($inputTerm, ${udtfResultExpr.resultTerm});
         |$header
         |getCollector().collect($joinedRowTerm);
         |super.collect(record);
      """.stripMargin

    val collectorCode = if (condition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(inputType, inputTerm)
        .bindSecondInput(udtfTypeInfo, udtfInputTerm, pojoFieldMapping)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |}
         |""".stripMargin
    }

    generateTableFunctionCollectorForJoinTable(
      ctx,
      "JoinTableFuncCollector",
      collectorCode,
      inputType,
      udtfTypeInfo,
      config,
      inputTerm = inputTerm,
      collectedTerm = udtfInputTerm)
  }

  /**
    * The only differences against CollectorCodeGenerator.generateTableFunctionCollector is
    * "super.collect" call is binding with collect join row in "body" code
    */
  private def generateTableFunctionCollectorForJoinTable(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputType: BaseRowType,
      collectedType: BaseRowType,
      config: TableConfig,
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      collectedTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM)
  : GeneratedCollector = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForType(DataTypes.internal(inputType))
    val input2TypeClass = boxedTypeTermForType(DataTypes.internal(collectedType))

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$input1TypeClass $inputTerm, $input2TypeClass $collectedTerm",
      callingParams = s"$inputTerm, $collectedTerm"
    )


    val funcCode = if (unboxingCodeSplit.isSplit) {
      s"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) record;
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
        case (define, body) =>
          s"""
             |$define throws Exception {
             |  $body
             |}
          """.stripMargin
      } mkString "\n"
      }

        @Override
        public void close() {
        }
      }
    """.stripMargin
    } else {
      s"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) record;
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() {
        }
      }
    """.stripMargin
    }

    GeneratedCollector(className, funcCode)
  }

  private def generateCalcMap(config: TableConfig)
    : GeneratedFunction[FlatMapFunction[BaseRow, BaseRow], BaseRow] = {

    val program = calcProgram.get
    val condition = if (program.getCondition != null) {
      Some(program.expandLocalRef(program.getCondition))
    } else {
      None
    }
    generateFunction(
      tableSourceSchema.internalType(classOf[BaseRow]),
      "TableCalcMapFunction",
      FlinkTypeFactory.toInternalBaseRowType(program.getOutputRowType, classOf[GenericRow]),
      program,
      condition,
      config,
      classOf[FlatMapFunction[BaseRow, BaseRow]])
  }

  private def joinSelectionToString(inputType: RelDataType): String = {
    inputType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString(
    inputType: RelDataType,
    joinCondition: RexNode,
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    val inFields = inputType.getFieldNames.asScala.toList
    if (joinCondition != null) {
      expression(joinCondition, inFields, None)
    } else {
      null
    }
  }

  private def joinTypeToString = joinType match {
    case JoinRelType.INNER => "InnerJoin"
    case JoinRelType.LEFT => "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }

  private def joinToString(
    inputType: RelDataType,
    joinCondition: Option[RexNode],
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    val prefix = if (tableSource.isAsync) {
      "AsyncJoinTable"
    } else {
      "JoinTable"
    }
    var str = s"$prefix(table: (${tableSource.explainSource()}), joinType: $joinTypeToString, "
    str += s"join: (${joinSelectionToString(inputType)}), "
    val inputFieldNames = inputSchema.fieldNames
    val tableFieldNames = tableSourceSchema.fieldNames
    val keyPairNames = lookupKeyPairs.asScala.map { p =>
      s"${inputFieldNames(p.source)}=${
        if (p.target > -1) tableFieldNames(p.target) else -1
      }"
    }
    str += s" on: (${keyPairNames.mkString(", ")}"
    str +=
      s"${constantLookupKeys.asScala.map(k => tableFieldNames(k._1) + " = " + k._2)
        .mkString(", ")})"
    if (joinCondition.isDefined) {
      str += s", where: (${joinConditionToString(inputType, joinCondition.get, expression)})"
    }
    str += ")"
    str
  }

  private def joinExplainTerms(
    pw: RelWriter,
    inputType: RelDataType,
    joinCondition: Option[RexNode],
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): RelWriter = {

    val condition: String = if (calcProgram.isDefined) {
      conditionToString(calcProgram.get, getExpressionString)
    } else {
      ""
    }

    pw
        .item("join", joinSelectionToString(inputType))
        .item("on", lookupKeyPairs)
        .item("joinType", joinTypeToString)
        .itemIf("where", condition, !condition.isEmpty)
        .itemIf("joinCondition",
          joinConditionToString(inputType, joinCondition.orNull, expression),
          joinCondition.isDefined)
        .itemIf("period", period.orNull, period.isDefined)

    pw
  }
}
