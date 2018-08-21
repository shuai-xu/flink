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

import java.util.{List => JList}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.{ELEMENT, STREAM_RECORD, generatorCollect}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.conversion.InternalTypeConverters.genToInternal
import org.apache.flink.table.runtime.operator.SubstituteStreamOperator
import org.apache.flink.table.types._
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._

/**
  * Common class for batch and stream scans.
  */
trait CommonScan[T] {

  private[flink] def needsConversion(dataType: DataType): Boolean = dataType match {
    case _: BaseRowType => false
    case t: TypeInfoWrappedType if t.getTypeInfo.isInstanceOf[BaseRowTypeInfo[_]] => false
    case _ => true
  }

  /**
    * @param qualifiedName qualified name for table
    */
  private[flink] def getOperatorName(qualifiedName: JList[String], rowType: RelDataType): String = {
    val s = s"table:$qualifiedName, fields:(${rowType.getFieldNames.mkString(", ")})"
    s"SourceConversion($s)"
  }

  private def hasEventTimeField(indexes: Array[Int]) =
    indexes.contains(DataTypes.ROWTIME_MARKER)

  private[flink] def convertToInternalRow(
      ctx: CodeGeneratorContext,
      input: StreamTransformation[Any],
      fieldIndexes: Array[Int],
      inputType: DataType,
      outRowType: RelDataType,
      qualifiedName: JList[String],
      config: TableConfig): StreamTransformation[BaseRow] = {

    val outputRowType = FlinkTypeFactory.toInternalBaseRowType(outRowType, classOf[GenericRow])

    // conversion
    val convertName = "SourceConversion"
    // type convert
    val inputTerm = DEFAULT_INPUT1_TERM
    val (inputTermConverter, internalInputType: InternalType) =
      if (!inputType.isInstanceOf[BaseRowType]) {
        val convertFunc = genToInternal(ctx, inputType)
        if (DataTypes.internal(inputType).isInstanceOf[BaseRowType]) {
          (convertFunc, DataTypes.internal(inputType))
        } else {
          (
              (record: String) =>
                s"${classOf[GenericRow].getCanonicalName}.wrap(${convertFunc(record)})",
              new BaseRowType(DataTypes.internal(inputType))
          )
        }
      } else {
        ((record: String) => record, inputType)
      }

    var codeSplit = GeneratedSplittableExpression.UNSPLIT_EXPRESSION
    val inputFieldTypes = DataTypes.internal(inputType) match {
      case rowType: BaseRowType =>
        rowType.getFieldTypes
      case t => Array(t)
    }

    val processCode =
      if ((inputFieldTypes sameElements
          outputRowType.getFieldTypes) && !hasEventTimeField(fieldIndexes)) {
        s"${generatorCollect(inputTerm)}"
      } else {

        // field index change (pojo)
        val resultGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
            .bindInput(
              internalInputType, // this must be a BaseRowType
              inputTerm = inputTerm,
              inputFieldMapping = Some(fieldIndexes))

        val inputTypeTerm = boxedTypeTermForType(DataTypes.internal(internalInputType))

        val conversion = resultGenerator.generateConverterResultExpression(outputRowType)

        codeSplit = CodeGenUtils.generateSplitFunctionCalls(
          conversion.codeBuffer,
          config.getMaxGeneratedCodeLength,
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
          if (hasEventTimeField(fieldIndexes)) {
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

    val substituteStreamOperator = new SubstituteStreamOperator[BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)

    new OneInputTransformation(
      input,
      getOperatorName(qualifiedName, outRowType),
      substituteStreamOperator,
      DataTypes.toTypeInfo(outputRowType).asInstanceOf[BaseRowTypeInfo[BaseRow]],
      input.getParallelism)
  }
}
