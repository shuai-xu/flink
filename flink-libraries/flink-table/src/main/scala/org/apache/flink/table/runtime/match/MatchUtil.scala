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

package org.apache.flink.table.runtime.`match`

import java.util
import java.util.Comparator

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.{EventComparator, PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{CodeGeneratorContext, Compiler, GenConditionFunction, GenSelectFunction, GeneratedSorter, MatchCodeGenerator}
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.aggregate.{CollectionBaseRowComparator, SortUtil}
import org.apache.flink.table.runtime.sort.RecordComparator
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.util.Logging

import scala.collection.JavaConverters._

/**
  * An util class to generate match functions.
  */
object MatchUtil {

  private[flink] def generateIterativeCondition(
    config: TableConfig,
    relBuilder: RelBuilder,
    patternName: String,
    patternNames: Seq[String],
    patternDefinition: RexNode,
    inputTypeInfo: TypeInformation[_]): IterativeCondition[BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames,
      GenConditionFunction,
      Some(patternName))
        .bindInput(DataTypes.internal(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]
    val condition = generator.generateExpression(patternDefinition)
    val body =
      s"""
        |${condition.code}
        |return ${condition.resultTerm};
        |""".stripMargin

    val genCondition = generator
      .generateIterativeCondition("MatchRecognizeCondition", body, config)
    new IterativeConditionRunner(genCondition)
  }

  private[flink] def generatePatternSelectFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternSelectFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames,
      GenSelectFunction)
        .bindInput(DataTypes.internal(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateSelectOutputExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
        |${resultExpression.code}
        |return ${resultExpression.resultTerm};
        |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternSelectFunction(
      "MatchRecognizePatternSelectFunction",
      body, config)
    new PatternSelectFunctionRunner(genFunction)
  }

  private[flink] def generatePatternTimeoutFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternTimeoutFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames,
      GenSelectFunction)
        .bindInput(DataTypes.internal(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateSelectOutputExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |return ${resultExpression.resultTerm};
         |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternTimeoutFunction(
      "MatchRecognizePatternTimeoutFunction",
      body, config)
    new PatternTimeoutFunctionRunner(genFunction)
  }

  private[flink] def generatePatternFlatSelectFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternFlatSelectFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames,
      GenSelectFunction)
        .bindInput(DataTypes.internal(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateFlatSelectOutputExpression(
      partitionKeys,
      orderKeys,
      measures,
      returnType)
    val body =
      s"""
        |${resultExpression.code}
        |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternFlatSelectFunction(
      "MatchRecognizePatternFlatSelectFunction",
      body, config)
    new PatternFlatSelectFunctionRunner(genFunction)
  }

  private[flink] def generatePatternFlatTimeoutFunction(
    config: TableConfig,
    relBuilder: RelBuilder,
    returnType: BaseRowSchema,
    patternNames: Seq[String],
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_]): PatternFlatTimeoutFunction[BaseRow, BaseRow] = {

    val ctx = CodeGeneratorContext(config, supportReference = true)
    val generator = new MatchCodeGenerator(
      ctx,
      relBuilder,
      false,
      config.getNullCheck,
      patternNames,
      GenSelectFunction)
        .bindInput(DataTypes.internal(inputTypeInfo))
        .asInstanceOf[MatchCodeGenerator]

    val resultExpression = generator.generateFlatSelectOutputExpression(
      partitionKeys,
      orderKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |""".stripMargin

    generator.addReusableStatements()
    val genFunction = generator.generatePatternFlatTimeoutFunction(
      "MatchRecognizePatternFlatTimeoutFunction",
      body, config)
    new PatternFlatTimeoutFunctionRunner(genFunction)
  }

  private[flink] def createRowTimeSortFunction(
      orderKeys: RelCollation,
      inputSchema: BaseRowSchema): EventComparator[BaseRow] = {
    if (orderKeys.getFieldCollations.size() > 1) {
      val sorter = SortUtil.createSorter(
        inputSchema.internalType(classOf[BaseRow]),
        orderKeys.getFieldCollations.asScala.tail) // strip off time collation

      new CustomEventComparator(sorter)
    } else {
      null
    }
  }

  private[flink] def createProcTimeSortFunction(
      orderKeys: RelCollation,
      inputSchema: BaseRowSchema): EventComparator[BaseRow] = {
    if (orderKeys.getFieldCollations.size() > 1) {
      val sorter = SortUtil.createSorter(
        inputSchema.internalType(classOf[BaseRow]),
        orderKeys.getFieldCollations.asScala.tail) // strip off time collation

      new CustomEventComparator(sorter)
    } else {
      null
    }
  }

  /**
    * Custom EventComparator.
    */
  class CustomEventComparator(private val gSorter: GeneratedSorter)
    extends EventComparator[BaseRow]
    with Compiler[RecordComparator]
    with Logging {

    private var rowComp: Comparator[BaseRow] = _

    override def compare(arg0: BaseRow, arg1: BaseRow):Int = {
      if (rowComp == null) {
        val name = gSorter.comparator.name
        val code = gSorter.comparator.code
        LOG.debug(s"Compiling Sorter: $name \n\n Code:\n$code")
        val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
        gSorter.comparator.code = null
        LOG.debug("Instantiating Sorter.")
        val comparator = clazz.newInstance()
        comparator.init(gSorter.serializers, gSorter.comparators)
        rowComp = new CollectionBaseRowComparator(comparator)
      }

      rowComp.compare(arg0, arg1)
    }
  }
}
