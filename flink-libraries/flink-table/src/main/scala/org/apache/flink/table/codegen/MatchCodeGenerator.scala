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

package org.apache.flink.table.codegen

import java.math.{BigDecimal => JBigDecimal}
import java.util

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.{SqlAggFunction, SqlOperator}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.cep.pattern.interval.PatternWindowTimeFunction
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.AggregateUtil
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.conversion.InternalTypeConverters._
import org.apache.flink.table.runtime.functions.AggsHandleFunction
import org.apache.flink.table.types.{BaseRowType, DataTypes, GenericType, InternalType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

sealed trait GenMode
case object GenConditionFunction extends GenMode
case object GenSelectFunction extends GenMode
case object GenTimeWindowFunction extends GenMode

/**
  * A code generator for generating CEP related functions.
  *
  * @param ctx the cotext of the code generator
  * @param nullableInput input(s) can be null.
  * @param nullCheck whether to do null check
  * @param patternNames the names of patterns
  * @param genMode what the code generator is generating for: [[IterativeCondition]],
  *                [[PatternSelectFunction]] or [[PatternWindowTimeFunction]]
  * @param patternName the name of current pattern
  */
class MatchCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    nullableInput: Boolean,
    nullCheck: Boolean,
    patternNames: Seq[String],
    genMode: GenMode,
    patternName: Option[String] = None,
    collectorTerm: String = CodeGeneratorContext.DEFAULT_COLLECTOR_TERM)
  extends ExprCodeGenerator(ctx, nullableInput, nullCheck) {

  private val AGGS_HANDLE_FUNCTION = className[AggsHandleFunction]

  private val GENERIC_ROW = className[GenericRow]

  private val CURRENT_CLASS_LOADER = "Thread.currentThread().getContextClassLoader()"

  /**
    * @return term of pattern names
    */
  private val patternNameListTerm = newName("patternNameList")

  /**
    * @return term of current pattern which is processing
    */
  private val currPatternTerm = newName("currPattern")

  /**
    * @return term of current event which is processing
    */
  private val currEventTerm = newName("currEvent")

  /**
    * @return term of whether RUNNING semantics is used
    */
  private val runningTerm = newName("running")

  private lazy val inputFieldTypeInfos =
    input1Type.asInstanceOf[BaseRowType].getFieldTypes

  private val buildPatternNameList: String = {
    for (patternName <- patternNames) yield
      s"""
        |$patternNameListTerm.add("$patternName");
        |""".stripMargin
  }.mkString("\n")

  def addReusableStatements(): Unit = {
    val eventTypeTerm = boxedTypeTermForType(DataTypes.internal(input1Type))
    val memberStatement =
      s"""
        |$eventTypeTerm $currEventTerm = null;
        |String $currPatternTerm = null;
        |boolean $runningTerm;
        |java.util.List<String> $patternNameListTerm = new java.util.ArrayList();
        |""".stripMargin

    ctx.addReusableMember(memberStatement, buildPatternNameList)
  }

  /**
    * Generates a [[IterativeCondition]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedIterativeCondition
    */
  def generateIterativeCondition(
      name: String,
      bodyCode: String,
      config: TableConfig)
    : GeneratedIterativeCondition = {

    val funcName = newName(name)
    val inputTypeTerm = boxedTypeTermForType(DataTypes.internal(input1Type))

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $input1Term, " +
        s"${classOf[IterativeCondition.Context[_]].getCanonicalName} $contextTerm",
      callingParams = s"$input1Term, $contextTerm"
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName
          extends ${classOf[RichIterativeCondition[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public boolean filter(
          Object _in1, ${classOf[IterativeCondition.Context[_]].getCanonicalName} $contextTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                """.stripMargin
            }
          } mkString("\n")
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          extends ${classOf[RichIterativeCondition[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public boolean filter(
          Object _in1, ${classOf[IterativeCondition.Context[_]].getCanonicalName} $contextTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin
    }

    GeneratedIterativeCondition(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates a [[PatternSelectFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternSelectFunction
    */
  def generatePatternSelectFunction(
      name: String,
      bodyCode: String,
      config: TableConfig)
    : GeneratedPatternSelectFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[BaseRow]]].getCanonicalName

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $input1Term",
      callingParams = input1Term
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName
          implements ${classOf[PatternSelectFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public Object select(java.util.Map<String, java.util.List<Object>> _in1)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          implements ${classOf[PatternSelectFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public Object select(java.util.Map<String, java.util.List<Object>> _in1)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin
    }

    GeneratedPatternSelectFunction(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates a [[PatternTimeoutFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternSelectFunction
    */
  def generatePatternTimeoutFunction(
      name: String,
      bodyCode: String,
      config: TableConfig)
    : GeneratedPatternTimeoutFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[BaseRow]]].getCanonicalName

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $input1Term, long timeoutTimestamp",
      callingParams = s"$input1Term, timeoutTimestamp"
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName
          implements ${classOf[PatternTimeoutFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public Object timeout(java.util.Map<String, java.util.List<Object>> _in1,
          long timeoutTimestamp) throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          implements ${classOf[PatternTimeoutFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public Object timeout(java.util.Map<String, java.util.List<Object>> _in1,
          long timeoutTimestamp) throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin
    }

    GeneratedPatternTimeoutFunction(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates a [[PatternFlatSelectFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternFlatSelectFunction
    */
  def generatePatternFlatSelectFunction(
      name: String,
      bodyCode: String,
      config: TableConfig)
    : GeneratedPatternFlatSelectFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[BaseRow]]].getCanonicalName

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $input1Term, org.apache.flink.util.Collector $collectorTerm",
      callingParams = s"$input1Term, $collectorTerm"
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName
          implements ${classOf[PatternFlatSelectFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void flatSelect(java.util.Map<String, java.util.List<Object>> _in1,
            org.apache.flink.util.Collector $collectorTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 | ${ctx.reusePerRecordCode()}
                 | $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          implements ${classOf[PatternFlatSelectFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void flatSelect(java.util.Map<String, java.util.List<Object>> _in1,
            org.apache.flink.util.Collector $collectorTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin
    }

    GeneratedPatternFlatSelectFunction(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates a [[PatternFlatTimeoutFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternFlatTimeoutFunction
    */
  def generatePatternFlatTimeoutFunction(
      name: String,
      bodyCode: String,
      config: TableConfig)
    : GeneratedPatternFlatTimeoutFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[BaseRow]]].getCanonicalName

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $input1Term, org.apache.flink.util.Collector $collectorTerm",
      callingParams = s"$input1Term, $collectorTerm"
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName
          implements ${classOf[PatternFlatTimeoutFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void timeout(java.util.Map<String, java.util.List<Object>> _in1,
            long timeoutTimestamp,
            org.apache.flink.util.Collector $collectorTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          implements ${classOf[PatternFlatTimeoutFunction[_, _]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void timeout(java.util.Map<String, java.util.List<Object>> _in1,
            long timeoutTimestamp,
            org.apache.flink.util.Collector $collectorTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin
    }

    GeneratedPatternFlatTimeoutFunction(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates a [[PatternWindowTimeFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternFlatTimeoutFunction
    */
  def generatePatternWindowTimeFunction(
      name: String,
      bodyCode: String,
      config: TableConfig)
    : GeneratedPatternWindowTimeFunction = {

    val funcName = newName(name)
    val inputTypeTerm = classOf[BaseRow].getCanonicalName

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getMaxGeneratedCodeLength,
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$inputTypeTerm $input1Term",
      callingParams = input1Term
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName
          implements ${classOf[PatternWindowTimeFunction[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public long getWindowTime(Object _in1) throws Exception {
          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          implements ${classOf[PatternWindowTimeFunction[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public long getWindowTime(Object _in1) throws Exception {
          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin
    }

    GeneratedPatternWindowTimeFunction(funcName, funcCode, ctx.references.toArray)
  }

  def generateSelectOutputExpression(
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    returnSchema: BaseRowSchema
  ): GeneratedExpression = {

    val eventNameTerm = newName("event")
    val eventTypeTerm = boxedTypeTermForType(DataTypes.internal(input1Type))

    // For "ONE ROW PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the columns defined in the measures clause.
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        generateFieldAccess(
          ctx, DataTypes.internal(input1Type), eventNameTerm, inputRef.getIndex, nullCheck)
      } ++ returnSchema.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val resultCodeGenerator = new ExprCodeGenerator(ctx, nullableInput, nullCheck)
        .bindInput(input1Type, inputTerm = input1Term)
    val resultExpression = resultCodeGenerator.generateResultExpression(
      resultExprs,
      new BaseRowType(
        classOf[GenericRow],
        returnSchema.fieldTypeInfos.map(DataTypes.internal).toArray,
        returnSchema.fieldNames.toArray))

    val resultCode =
      s"""
        |$eventTypeTerm $eventNameTerm = null;
        |if (${partitionKeys.size()} > 0) {
        |  for (java.util.Map.Entry entry : $input1Term.entrySet()) {
        |    java.util.List<BaseRow> value = (java.util.List<BaseRow>) entry.getValue();
        |    if (value != null && value.size() > 0) {
        |      $eventNameTerm = ($eventTypeTerm) value.get(0);
        |      break;
        |    }
        |  }
        |}
        |
        |${resultExpression.code}
        |""".stripMargin

    resultExpression.copy(code = resultCode)
  }

  def generateFlatSelectOutputExpression(
      partitionKeys: util.List[RexNode],
      orderKeys: RelCollation,
      measures: util.Map[String, RexNode],
      returnSchema: BaseRowSchema)
    : GeneratedExpression = {

    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val eventNameListTerm = newName("eventList")
    val input1T = DataTypes.internal(input1Type)
    val eventTypeTerm = boxedTypeTermForType(input1T)
    val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

    // For "ALL ROWS PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the ordering columns;
    // 3) the columns defined in the measures clause;
    // 4) any remaining columns defined of the input.
    val fieldsAccessed = mutable.Set[Int]()
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        fieldsAccessed += inputRef.getIndex
        generateFieldAccess(ctx, input1T, eventNameTerm, inputRef.getIndex, nullCheck)
      } ++ orderKeys.getFieldCollations.asScala.map { fieldCollation =>
        fieldsAccessed += fieldCollation.getFieldIndex
        generateFieldAccess(ctx, input1T, eventNameTerm, fieldCollation.getFieldIndex, nullCheck)
      } ++ (0 until DataTypes.getArity(input1Type)).filterNot(fieldsAccessed.contains).map { idx =>
        generateFieldAccess(ctx, input1T, eventNameTerm, idx, nullCheck)
      } ++ returnSchema.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val resultCodeGenerator = new ExprCodeGenerator(ctx, nullableInput, nullCheck)
          .bindInput(input1Type, inputTerm = input1Term)
    val resultExpression = resultCodeGenerator.generateResultExpression(
      resultExprs,
      new BaseRowType(
        classOf[GenericRow],
        returnSchema.fieldTypeInfos.map(DataTypes.internal).toArray,
        returnSchema.fieldNames.toArray))

    val resultCode =
      s"""
        |for (String $patternNameTerm : $patternNameListTerm) {
        |  $currPatternTerm = $patternNameTerm;
        |  $listTypeTerm $eventNameListTerm = ($listTypeTerm) $input1Term.get($patternNameTerm);
        |  if ($eventNameListTerm != null) {
        |    for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
        |      $currEventTerm = $eventNameTerm;
        |      ${resultExpression.code}
        |      $collectorTerm.collect(${resultExpression.resultTerm});
        |    }
        |  }
        |}
        |$currPatternTerm = null;
        |$currEventTerm = null;
        |""".stripMargin

    GeneratedExpression("", "false", resultCode, null)
  }

  override def visitCall(call: RexCall): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toInternalType(call.getType)
    call.getOperator match {
      case PREV =>
        val countLiteral = call.getOperands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generatePrev(
          call.getOperands.get(0),
          count,
          resultType)

      case NEXT | MATCH_NUMBER =>
        throw new CodeGenException(s"Unsupported call: $call")

      case FIRST | LAST =>
        val countLiteral = call.getOperands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generateFirstLast(
          call.getOperands.get(0),
          count,
          resultType,
          running = true,
          call.getOperator == FIRST)

      case RUNNING | FINAL =>
        generateRunningFinal(
          call.getOperands.get(0),
          resultType,
          call.getOperator == RUNNING)

      case sqlAggFunction: SqlAggFunction =>
        // convert operands and help giving untyped NULL literals a type
        val operands = call.getOperands.zipWithIndex.map {

          // this helps e.g. for AS(null)
          // we might need to extend this logic in case some rules do not create typed NULLs
          case (operandLiteral: RexLiteral, 0) if
          operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
              call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
            generateNullLiteral(DataTypes.internal(resultType), nullCheck)

          case (o@_, _) =>
            o.accept(this)
        }
        operands.foreach(requireList(_, call.getOperator.getName))
        val elementType = DataTypes.internal(operands.head.resultType
            .asInstanceOf[GenericType[_]]
            .getTypeInfo
            .asInstanceOf[ListTypeInfo[_]]
            .getElementTypeInfo)
        generateAggFunction(sqlAggFunction, operands, elementType, resultType)

      case _ => super.visitCall(call)
    }
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val eventNameListTerm = newName("eventNameList")
    val eventTypeTerm = boxedTypeTermForType(DataTypes.internal(input1Type))
    val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

    var resultType: InternalType = null

    val findEventsByPatternName = genMode match {
      case GenConditionFunction =>
        resultType = new GenericType(
          new ListTypeInfo(FlinkTypeFactory.toTypeInfo(fieldRef.getType)))

        s"""
          |java.util.List $resultTerm = new java.util.ArrayList();
          |boolean $nullTerm = false;
          |for ($eventTypeTerm $eventNameTerm : $contextTerm
          |    .getEventsForPattern("${fieldRef.getAlpha}")) {
          |  $resultTerm.add(${CodeGenUtils.baseRowFieldReadAccess(
                ctx, fieldRef.getIndex, eventNameTerm,
                inputFieldTypeInfos(fieldRef.getIndex))});
          |}
          |""".stripMargin

      case GenSelectFunction =>
        resultType = new GenericType(
          new ListTypeInfo(FlinkTypeFactory.toTypeInfo(fieldRef.getType)))

        s"""
          |java.util.List $resultTerm = new java.util.ArrayList();
          |boolean $nullTerm = false;
          |for (String $patternNameTerm : $patternNameListTerm) {
          |  if ($patternNameTerm.equals("${fieldRef.getAlpha}") ||
          |      ${fieldRef.getAlpha.equals("*")}) {
          |    boolean skipLoop = false;
          |    $listTypeTerm $eventNameListTerm =
          |      ($listTypeTerm) $input1Term.get($patternNameTerm);
          |    if ($eventNameListTerm != null) {
          |      for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
          |        $resultTerm.add(${CodeGenUtils.baseRowFieldReadAccess(
                      ctx,
                      fieldRef.getIndex,
                      eventNameTerm,
                      inputFieldTypeInfos(fieldRef.getIndex))});
          |        if ($runningTerm && $eventNameTerm == $currEventTerm) {
          |          skipLoop = true;
          |          break;
          |        }
          |      }
          |    }
          |
          |    if (skipLoop) {
          |      break;
          |    }
          |  }
          |
          |  if ($runningTerm && $patternNameTerm.equals($currPatternTerm)) {
          |    break;
          |  }
          |}
          |""".stripMargin

      case GenTimeWindowFunction =>
        resultType = FlinkTypeFactory.toInternalType(fieldRef.getType)
        val resultTypeTerm = boxedTypeTermForType(resultType)

        s"""
           |boolean $nullTerm = false;
           |$resultTypeTerm $resultTerm = ($resultTypeTerm)
           |  ${CodeGenUtils.baseRowFieldReadAccess(
                ctx,
                fieldRef.getIndex,
                input1Term,
                inputFieldTypeInfos(fieldRef.getIndex))};
           |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, findEventsByPatternName, resultType)
  }

  def generateCallExpression(
      operator: SqlOperator,
      operands: Seq[GeneratedExpression],
      resultType: InternalType): GeneratedExpression = {
    operator match {
      case sqlAggFunction: SqlAggFunction =>
        operands.foreach(requireList(_, operator.getName))
        val elementType = DataTypes.internal(operands.head.resultType
            .asInstanceOf[GenericType[_]]
            .getTypeInfo
            .asInstanceOf[ListTypeInfo[_]]
            .getElementTypeInfo)
        generateAggFunction(sqlAggFunction, operands, elementType, resultType)

      case _ => CodeGenUtils.generateCallExpression(
        ctx, operator, operands, resultType, nullCheck)
    }
  }

  private def generateAggFunction(
      sqlAggFunction: SqlAggFunction,
      operands: Seq[GeneratedExpression],
      elementType: InternalType,
      returnType: InternalType)
    : GeneratedExpression = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    // assume the aggregate function only accept one parameter
    val argList = new util.ArrayList[Integer]()
    argList.add(0)
    val resultRelType = typeFactory.buildRelDataType(Seq("TMP0"), Seq(returnType))
    val aggCall = AggregateCall.create(
      sqlAggFunction,
      false,
      false,
      argList,
      -1,
      resultRelType,
      sqlAggFunction.getName)

    val inputRelType = typeFactory.buildRelDataType(Seq("TMP0"), Seq(elementType))
    val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      Seq(aggCall),
      inputRelType,
      Array(false),
      needInputCount = false,
      isStateBackendDataViews = false)
    val aggsHandlerCodeGenerator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(new TableConfig, supportReference = true),
      relBuilder,
      Seq(elementType),
      needRetract = false,
      needMerge = false,
      nullCheck = true)
    val generatedAggsHandler = aggsHandlerCodeGenerator.generateAggsHandler(
      "MatchAggregateHandler",
      aggInfoList)

    val generatedTerm = ctx.addReusableObject(generatedAggsHandler, "generatedAggHandler")
    val functionTerm = newName("aggsHandler")

    val declareCode = s"private $AGGS_HANDLE_FUNCTION $functionTerm;"
    val initCode =
      s"$functionTerm = ($AGGS_HANDLE_FUNCTION) $generatedTerm.newInstance($CURRENT_CLASS_LOADER);"
    ctx.addReusableMember(declareCode, initCode)

    val elementTypeTerm = boxedTypeTermForType(elementType)
    val resultTypeTerm = boxedTypeTermForType(returnType)
    val resultTerm = newName("result")
    val nullTerm = newName("null")
    val elementTerm = newName("element")
    val inputTerm = newName("input")

    val operand = operands.head
    val functionCallCode =
      s"""
        |${operand.code}
        |boolean $nullTerm = false;
        |$functionTerm.setAccumulators($functionTerm.createAccumulators());
        |$GENERIC_ROW $inputTerm = new $GENERIC_ROW(1);
        |for ($elementTypeTerm $elementTerm : (java.util.List) ${operand.resultTerm}) {
        |  $inputTerm.update(0, $elementTerm);
        |  $functionTerm.accumulate($inputTerm);
        |}
        |$resultTypeTerm $resultTerm = ($resultTypeTerm)
        |  (($GENERIC_ROW) $functionTerm.getValue()).getField(0);
        |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, functionCallCode, returnType)
  }

  private def generatePrev(
      rexNode: RexNode,
      count: Int,
      resultType: InternalType)
    : GeneratedExpression = {
    rexNode match {
      case patternFieldRef: RexPatternFieldRef =>
        if (count == 0 && patternFieldRef.getAlpha == patternName.get) {
          // return current one
          return visitInputRef(patternFieldRef)
        }

        val listName = newName("patternEvents")
        val resultTerm = newName("result")
        val resultEventTerm = newName("resultEvent")
        val nullTerm = newName("isNull")
        val indexTerm = newName("eventIndex")
        val visitedEventNumberTerm = newName("visitedEventNumber")
        val eventTerm = newName("event")
        val resultTypeTerm = primitiveTypeTermForType(DataTypes.internal(resultType))
        val defaultValue = primitiveDefaultValue(DataTypes.internal(resultType))

        val eventTypeTerm = boxedTypeTermForType(DataTypes.internal(input1Type))

        val patternNamesToVisit = patternNames
          .take(patternNames.indexOf(patternFieldRef.getAlpha) + 1)
          .reverse
        def findEventByPhysicalPosition: String = {
          val init: String =
            s"""
              |java.util.List $listName = new java.util.ArrayList();
              |$eventTypeTerm $resultEventTerm = null;
              |""".stripMargin

          val fieldAccessExpr = CodeGenUtils.generateFieldAccess(
            ctx, DataTypes.internal(input1Type),
            resultEventTerm, patternFieldRef.getIndex, nullCheck)

          val getResultEvent: String = {
            for (tmpPatternName <- patternNamesToVisit) yield
              s"""
                |for ($eventTypeTerm $eventTerm : $contextTerm
                |    .getEventsForPattern("$tmpPatternName")) {
                |  $listName.add($eventTerm);
                |}
                |
                |$indexTerm = $listName.size() - ($count - $visitedEventNumberTerm);
                |if ($indexTerm >= 0) {
                |  $resultEventTerm = ($eventTypeTerm) $listName.get($indexTerm);
                |
                |  ${fieldAccessExpr.code}
                |  $resultTerm = ${fieldAccessExpr.resultTerm};
                |  $nullTerm = ${fieldAccessExpr.nullTerm};
                |
                |  break;
                |}
                |
                |$visitedEventNumberTerm += $listName.size();
                |$listName.clear();
                |""".stripMargin
          }.mkString("\n")

          s"""
            |$init
            |$getResultEvent
            |""".stripMargin
        }

        val resultCode =
          s"""
            |int $visitedEventNumberTerm = 0;
            |int $indexTerm;
            |$resultTypeTerm $resultTerm = $defaultValue;
            |boolean $nullTerm = true;
            |do {
            |  $findEventByPhysicalPosition
            |} while (false);
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, DataTypes.internal(resultType))

      case rexCall: RexCall if rexCall.getOperator == CLASSIFIER =>
        val classifierList = newName("classifiers")
        val resultTerm = newName("result")
        val nullTerm = newName("isNull")
        val indexTerm = newName("eventIndex")
        val eventTerm = newName("event")
        val resultTypeTerm = primitiveTypeTermForType(DataTypes.internal(resultType))
        val defaultValue = primitiveDefaultValue(DataTypes.internal(resultType))

        val eventTypeTerm = boxedTypeTermForType(DataTypes.internal(input1Type))

        def findClassifierByPhysicalPosition: String = {
          var init =
            s"""
              |java.util.List $classifierList = new java.util.ArrayList();
              |""".stripMargin

          val buildClassifierList: String = {
            val patternNamesToVisit = patternNames
              .take(patternNames.indexOf(patternName.get) + 1)
            for (patternName1 <- patternNamesToVisit) yield
              s"""
                |for ($eventTypeTerm $eventTerm : $contextTerm
                |  .getEventsForPattern("$patternName1")) {
                |  $classifierList.add("$patternName1");
                |}
                |""".stripMargin
          }.mkString("\n")

          val resultInternal = genToInternal(ctx, resultType, s"$classifierList.get($indexTerm)")
          val getResult =
            s"""
              |$classifierList.add("${patternName.get}");
              |int $indexTerm = $classifierList.size() - $count - 1;
              |if ($indexTerm >= 0) {
              |  $resultTerm = ($resultTypeTerm) $resultInternal;
              |  $nullTerm = false;
              |}
              |$classifierList.clear();
              |""".stripMargin

          s"""
            |$init
            |$buildClassifierList
            |$getResult
            |""".stripMargin
        }

        val resultCode =
          s"""
            |$resultTypeTerm $resultTerm = $defaultValue;
            |boolean $nullTerm = true;
            |$findClassifierByPhysicalPosition
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, DataTypes.internal(resultType))

      case rexCall: RexCall =>
        val operands = rexCall.getOperands.asScala.map {
          operand => generatePrev(
            operand,
            count,
            FlinkTypeFactory.toInternalType(operand.getType))
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }

  private def generateFirstLast(
      rexNode: RexNode,
      count: Int,
      resultType: InternalType,
      running: Boolean,
      first: Boolean)
    : GeneratedExpression = {
    rexNode match {
      case patternFieldRef: RexPatternFieldRef =>

        val eventNameTerm = newName("event")
        val resultEventTerm = newName("resultEvent")
        val resultTerm = newName("result")
        val listName = newName("patternEvents")
        val nullTerm = newName("isNull")
        val patternNameTerm = newName("patternName")
        val eventNameListTerm = newName("eventNameList")
        val resultTypeTerm = primitiveTypeTermForType(resultType)
        val defaultValue = primitiveDefaultValue(resultType)

        val eventTypeTerm = boxedTypeTermForType(input1Type)
        val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

        def findEventByLogicalPosition: String = {
          val init =
            s"""
              |java.util.List $listName = new java.util.ArrayList();
              |$eventTypeTerm $resultEventTerm = null;
              |""".stripMargin

          val fieldAccessExpr = CodeGenUtils.generateFieldAccess(
            ctx, input1Type, resultEventTerm, patternFieldRef.getIndex, nullCheck)

          val findEventsByPatternName = genMode match {
            case GenConditionFunction =>
              s"""
                |for ($eventTypeTerm $eventNameTerm : $contextTerm
                |    .getEventsForPattern("${patternFieldRef.getAlpha}")) {
                |  $listName.add($eventNameTerm);
                |}
                |""".stripMargin

            case GenSelectFunction =>
              s"""
                |for (String $patternNameTerm : $patternNameListTerm) {
                |  if ($patternNameTerm.equals("${patternFieldRef.getAlpha}") ||
                |      ${patternFieldRef.getAlpha.equals("*")}) {
                |    boolean skipLoop = false;
                |    $listTypeTerm $eventNameListTerm =
                |      ($listTypeTerm) $input1Term.get($patternNameTerm);
                |    if ($eventNameListTerm != null) {
                |      for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
                |        $listName.add($eventNameTerm);
                |        if ($running && $eventNameTerm == $currEventTerm) {
                |          skipLoop = true;
                |          break;
                |        }
                |      }
                |    }
                |
                |    if (skipLoop) {
                |      break;
                |    }
                |  }
                |
                |  if ($running && $patternNameTerm.equals($currPatternTerm)) {
                |    break;
                |  }
                |}
                |""".stripMargin
          }

          val getResultEvent =
            s"""
              |if ($listName.size() > $count) {
              |  $eventTypeTerm $eventNameTerm;
              |  if ($first) {
              |    $resultEventTerm = ($eventTypeTerm)
              |      $listName.get($count);
              |  } else {
              |    $resultEventTerm = ($eventTypeTerm)
              |      $listName.get($listName.size() - $count - 1);
              |  }
              |  $nullTerm = false;
              |}
              |""".stripMargin

          s"""
            |$init
            |$findEventsByPatternName
            |$getResultEvent
            |if (!$nullTerm) {
            |  ${fieldAccessExpr.code}
            |  $resultTerm = ${fieldAccessExpr.resultTerm};
            |  $nullTerm = ${fieldAccessExpr.nullTerm};
            |}
            |""".stripMargin
        }

        val resultCode =
          s"""
            |$resultTypeTerm $resultTerm = $defaultValue;
            |boolean $nullTerm = true;
            |$findEventByLogicalPosition
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)

      case rexCall: RexCall if rexCall.getOperator.isInstanceOf[SqlAggFunction] =>
        val operandsRaw = rexCall.getOperands.asScala.map(generateExpression)
        val operands = operandsRaw.map {
          operand =>
            val code =
              s"""
                |$runningTerm = $running;
                |${operand.code}
                |""".stripMargin
            operand.copy(code = code)
        }
        generateCallExpression(rexCall.getOperator, operands, resultType)

      case rexCall: RexCall if rexCall.getOperator == CLASSIFIER =>
        val resultTerm = newName("result")
        val nullTerm = newName("isNull")
        val resultTypeTerm = primitiveTypeTermForType(resultType)
        val defaultValue = primitiveDefaultValue(resultType)

        val classifierList = newName("classifiers")
        val patternNameTerm = newName("patternName")
        val eventNameTerm = newName("event")
        val eventNameListTerm = newName("eventList")

        val eventTypeTerm = boxedTypeTermForType(input1Type)
        val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

        def findClassifierByLogicalPosition: String = {
          val init =
            s"""
              |java.util.List $classifierList = new java.util.ArrayList();
              |""".stripMargin

          val findClassifiers =
            s"""
              |for (String $patternNameTerm : $patternNameListTerm) {
              |  boolean skipLoop = false;
              |  $listTypeTerm $eventNameListTerm =
              |    ($listTypeTerm) $input1Term.get($patternNameTerm);
              |  if ($eventNameListTerm != null) {
              |    for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
              |      $classifierList.add($patternNameTerm);
              |      if ($running && $eventNameTerm == $currEventTerm) {
              |        skipLoop = true;
              |        break;
              |      }
              |    }
              |  }
              |  if (skipLoop) {
              |    break;
              |  }
              |
              |  if ($running && $patternNameTerm.equals($currPatternTerm)) {
              |    break;
              |  }
              |}
              |""".stripMargin

          val getResult =
            s"""
              |if ($classifierList.size() > $count) {
              |  if ($first) {
              |    $resultTerm = ($resultTypeTerm)
              |      ${genToInternal(ctx, resultType, s"$classifierList.get($count)")};
              |  } else {
              |    $resultTerm = ($resultTypeTerm)
              |      ${genToInternal(ctx, resultType,
                        s"$classifierList.get($classifierList.size() - $count - 1)")};
              |  }
              |  $nullTerm = false;
              |} else {
              |  $resultTerm = $defaultValue;
              |  $nullTerm = true;
              |}
              |""".stripMargin

          s"""
            |$init
            |$findClassifiers
            |$getResult
            |""".stripMargin
        }

        val resultCode =
          s"""
            |$resultTypeTerm $resultTerm;
            |boolean $nullTerm;
            |$findClassifierByLogicalPosition
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)

      case rexCall: RexCall =>
        val operands = rexCall.getOperands.asScala.map {
          operand => generateFirstLast(
            operand,
            count,
            FlinkTypeFactory.toInternalType(operand.getType),
            running,
            first)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }

  private def generateRunningFinal(
      rexNode: RexNode,
      resultType: InternalType,
      running: Boolean)
    : GeneratedExpression = {
    rexNode match {
      case _: RexPatternFieldRef =>
        generateFirstLast(rexNode, 0, resultType, running, first = false)

      case rexCall: RexCall if rexCall.getOperator == CLASSIFIER =>
        generateFirstLast(rexNode, 0, resultType, running, first = false)

      case rexCall: RexCall if rexCall.getOperator == FIRST || rexCall.getOperator == LAST =>
        val countLiteral = rexCall.getOperands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generateFirstLast(
          rexCall.getOperands.get(0),
          count,
          resultType,
          running,
          rexCall.getOperator == FIRST)

      case rexCall: RexCall if rexCall.getOperator.isInstanceOf[SqlAggFunction] =>
        val operandsRaw = rexCall.getOperands.asScala.map(generateExpression)
        val operands = operandsRaw.map {
          operand =>
            val code =
              s"""
                |$runningTerm = $running;
                |${operand.code}
                |""".stripMargin
            operand.copy(code = code)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)


      case rexCall: RexCall =>
        val operands = rexCall.getOperands.asScala.map {
          operand => generateRunningFinal(
            operand,
            FlinkTypeFactory.toInternalType(operand.getType),
            running)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }
}
