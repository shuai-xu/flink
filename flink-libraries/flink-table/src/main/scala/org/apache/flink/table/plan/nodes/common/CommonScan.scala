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

import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.java.typeutils.TypeExtractor

import java.util.{List => JList}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.types._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD, generatorCollect}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.OneInputSubstituteStreamOperator
import org.apache.flink.table.runtime.conversion.DataStructureConverters.genToInternal
import org.apache.flink.table.sources.{BatchTableSource, LookupableTableSource, StreamTableSource, TableSource}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}

import scala.collection.JavaConversions._

/**
  * Common class for batch and stream scans.
  */
trait CommonScan[T] {

  private[flink] def needsConversion(dataType: DataType, clz: Class[_]): Boolean = dataType match {
    case bt: RowType => !bt.isUseBaseRow
//    case r: RowType => !CodeGenUtils.isInternalClass(clz, r)
    case t: TypeInfoWrappedDataType if t.getTypeInfo.isInstanceOf[BaseRowTypeInfo[_]] => false
    case _ => true
  }

  private[flink] def extractTableSourceTypeClass(source: TableSource): Class[_] = {
    try {
      source match {
        case s: BatchTableSource[_] =>
          TypeExtractor.createTypeInfo(source, classOf[BatchTableSource[_]], source.getClass, 0)
              .getTypeClass.asInstanceOf[Class[_]]
        case s: StreamTableSource[_] =>
          TypeExtractor.createTypeInfo(source, classOf[StreamTableSource[_]], source.getClass, 0)
              .getTypeClass.asInstanceOf[Class[_]]
        case s: LookupableTableSource[_] =>
          TypeExtractor.createTypeInfo(
            source, classOf[LookupableTableSource[_]], source.getClass, 0)
              .getTypeClass.asInstanceOf[Class[_]]
      }
    } catch {
      case _: InvalidTypesException =>
        classOf[Object]
    }
  }

  /**
    * @param qualifiedName qualified name for table
    */
  private[flink] def getOperatorName(qualifiedName: JList[String], rowType: RelDataType): String = {
    val s = s"table:$qualifiedName, fields:(${rowType.getFieldNames.mkString(", ")})"
    s"SourceConversion($s)"
  }

  private[flink] def hasTimeAttributeField(indexes: Array[Int]) =
    indexes.contains(DataTypes.ROWTIME_STREAM_MARKER)||
      indexes.contains(DataTypes.ROWTIME_BATCH_MARKER)||
      indexes.contains(DataTypes.PROCTIME_STREAM_MARKER)||
      indexes.contains(DataTypes.PROCTIME_BATCH_MARKER)

  def needInternalConversion: Boolean

  private[flink] def convertToInternalRow(
      ctx: CodeGeneratorContext,
      input: StreamTransformation[Any],
      fieldIndexes: Array[Int],
      inputType: DataType,
      outRowType: RelDataType,
      qualifiedName: JList[String],
      config: TableConfig,
      rowtimeExpr: Option[RexNode] = None): StreamTransformation[BaseRow] = {

    val outputRowType = FlinkTypeFactory.toInternalRowType(outRowType, classOf[GenericRow])

    // conversion
    val convertName = "SourceConversion"
    // type convert
    val inputTerm = DEFAULT_INPUT1_TERM
    val (inputTermConverter, internalInputType: InternalType) =
      if (!inputType.isInstanceOf[RowType] ||
          !inputType.asInstanceOf[RowType].isUseBaseRow) {
        val convertFunc = genToInternal(ctx, inputType)
        if (inputType.toInternalType.isInstanceOf[RowType]) {
          (convertFunc, inputType.toInternalType)
        } else {
          (
              (record: String) =>
                s"${classOf[GenericRow].getCanonicalName}.wrap(${convertFunc(record)})",
              new RowType(inputType.toInternalType)
          )
        }
      } else {
        ((record: String) => record, inputType)
      }

    var codeSplit = GeneratedSplittableExpression.UNSPLIT_EXPRESSION
    val (inputTypes, inputNames) = inputType.toInternalType match {
      case rowType: RowType => (rowType.getFieldInternalTypes, rowType.getFieldNames)
      case t => (Array(t), Array("f0"))
    }

    val processCode =
      if ((inputTypes sameElements outputRowType.getFieldInternalTypes) &&
          (inputNames sameElements outputRowType.getFieldNames) &&
          !hasTimeAttributeField(fieldIndexes)) {
        s"${generatorCollect(inputTerm)}"
      } else {

        // field index change (pojo)
        val resultGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
            .bindInput(
              internalInputType, // this must be a RowType
              inputTerm = inputTerm,
              inputFieldMapping = Some(fieldIndexes))

        val inputTypeTerm = boxedTypeTermForType(internalInputType)

        val conversion = resultGenerator.generateConverterResultExpression(
          outputRowType, rowtimeExpression = rowtimeExpr)

        codeSplit = CodeGenUtils.generateSplitFunctionCalls(
          conversion.codeBuffer,
          config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
          "SourceConversionApply",
          "private final void",
          ctx.reuseFieldCode().length,
          defineParams = s"$inputTypeTerm $inputTerm, $STREAM_RECORD $ELEMENT",
          callingParams = s"$inputTerm, $ELEMENT"
        )

        val conversionCode = if (codeSplit.isSplit) {
          codeSplit.callings.mkString("\n")
        } else {
          conversion.code
        }

        // extract time if the index is -1 or -2.
        val (extractElement, resetElement) =
          if (hasTimeAttributeField(fieldIndexes)) {
            (s"ctx.$ELEMENT = $ELEMENT;", s"ctx.$ELEMENT = null;")
          }
          else {
            ("", "")
          }

        s"""
           |$extractElement
           |$conversionCode
           |${generatorCollect(conversion.resultTerm)}
           |$resetElement
           |""".stripMargin
      }

    val generatedOperator = OperatorCodeGenerator.generateOneInputStreamOperator[Any, BaseRow](
      ctx,
      convertName,
      processCode,
      "",
      outputRowType,
      config,
      splitFunc = codeSplit,
      converter = inputTermConverter)

    val substituteStreamOperator = new OneInputSubstituteStreamOperator[Any, BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)

    new OneInputTransformation(
      input,
      getOperatorName(qualifiedName, outRowType),
      substituteStreamOperator,
      TypeConverters.toBaseRowTypeInfo(outputRowType),
      input.getParallelism)
  }
}
