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

import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.dataformat._
import org.apache.flink.table.types.{DataTypes, InternalType, RowType}

import scala.collection.mutable

/**
  * CodeGenerator for projection.
  */
object ProjectionCodeGenerator {

  // for loop optimization will only be enabled
  // if the number of fields to be project exceeds the limit
  //
  // this value is tuned by hand according to projecting
  // some string type fields with randomized order
  val FOR_LOOP_FIELD_LIMIT: Int = 25

  def generateProjectionExpression(
      ctx: CodeGeneratorContext,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: BaseRow] = classOf[BinaryRow],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      nullCheck: Boolean = true): GeneratedExpression = {

    val (setFieldGenerator, expressionGenerator) = CodeGenUtils.getSetFieldCodeGenerator(
      ctx, outType, outClass, outRecordTerm, Option(outRecordWriterTerm),
      nullCheck, reusedOutRecord, outRowAlreadyExists = false)

    // we use a for loop to do all the projections for the same field type
    // instead of generating separated code for each field.
    // when the number of fields of the same type is large, this can improve performance.
    def generateLoop(
        fieldType: InternalType,
        inIdxs: mutable.ArrayBuffer[Int],
        outIdxs: mutable.ArrayBuffer[Int]): String = {
      // this array contains the indices of the fields
      // whose type equals to `fieldType` in the input row
      val inIdxArr = newName("inIdx")
      ctx.addReusableMember(
        s"int[] $inIdxArr = null;",
        s"$inIdxArr = new int[] {${inIdxs.mkString(", ")}};")

      // this array contains the indices of the fields
      // whose type equals to `fieldType` in the output row
      val outIdxArr = newName("outIdx")
      ctx.addReusableMember(
        s"int[] $outIdxArr = null;",
        s"$outIdxArr = new int[] {${outIdxs.mkString(", ")}};")

      val loopIdx = newName("i")

      val (fieldVal, strIdxInitCode, strIdxIncCode) = if (fieldType == DataTypes.STRING) {
        // binary strings are reused, so we need to prepare an array
        val reuseBStrings = newName("reuseBStrings")
        val initIdx = newName("i")
        ctx.addReusableMember(
          s"$BINARY_STRING[] $reuseBStrings = new $BINARY_STRING[${inIdxs.length}];",
          s"""
             |for (int $initIdx = 0; $initIdx < $reuseBStrings.length; $initIdx++) {
             |  $reuseBStrings[$initIdx] = new $BINARY_STRING();
             |}
            """.stripMargin)

        val strIdx = newName("strIdx")
        (CodeGenUtils.baseRowFieldReadAccess(
          ctx, s"$inIdxArr[$loopIdx]", inputTerm,
          fieldType, Option(s"$reuseBStrings[$strIdx]")),
          s"int $strIdx = 0;", s"$strIdx++;")
      } else {
        (CodeGenUtils.baseRowFieldReadAccess(
          ctx, s"$inIdxArr[$loopIdx]", inputTerm, fieldType), "", "")
      }

      val inIdx = s"$inIdxArr[$loopIdx]"
      val outIdx = s"$outIdxArr[$loopIdx]"
      val nullTerm = s"$inputTerm.isNullAt($inIdx)"
      s"""
         |$strIdxInitCode
         |
         |for (int $loopIdx = 0; $loopIdx < $inIdxArr.length; $loopIdx++) {
         |  ${setFieldGenerator(outIdx, fieldType, "",  nullTerm, fieldVal)}
         |  $strIdxIncCode
         |}
       """.stripMargin
    }

    val outFieldTypes = outType.getFieldInternalTypes
    val typeIdxs = new mutable.HashMap[
      InternalType,
      (mutable.ArrayBuffer[Int], mutable.ArrayBuffer[Int])]()

    for (i <- outFieldTypes.indices) {
      val (inIdxs, outIdxs) = typeIdxs.getOrElseUpdate(
        outFieldTypes(i), (mutable.ArrayBuffer.empty[Int], mutable.ArrayBuffer.empty[Int]))
      inIdxs.append(inputMapping(i))
      outIdxs.append(i)
    }

    val codeBuffer = mutable.ArrayBuffer.empty[String]
    for ((fieldType, (inIdxs, outIdxs)) <- typeIdxs) {
      if (inIdxs.length >= FOR_LOOP_FIELD_LIMIT) {
        // for loop optimization will only be enabled
        // if the number of fields to be project exceeds the limit
        codeBuffer.append(generateLoop(fieldType, inIdxs, outIdxs))
      } else {
        // otherwise we do not use for loop
        for (i <- inIdxs.indices) {
          val nullTerm = s"$inputTerm.isNullAt(${inIdxs(i)})"
          codeBuffer.append(setFieldGenerator(
            outIdxs(i).toString, fieldType, "", nullTerm,
            CodeGenUtils.baseRowFieldReadAccess(ctx, inIdxs(i), inputTerm, fieldType)))
        }
      }
    }

    expressionGenerator(codeBuffer)
  }

  /**
    * CodeGenerator for projection.
    * @param reusedOutRecord If objects or variables can be reused, they will be added a reusable
    * output record to the member area of the generated class. If not they will be as temp
    * variables.
    * @return
    */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: BaseRow] = classOf[BinaryRow],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      nullCheck: Boolean = true): GeneratedProjection = {
    val className = newName(name)
    val baseClass = classOf[Projection[_, _]]

    val expression = generateProjectionExpression(
      ctx, inType, outType, inputMapping, outClass,
      inputTerm, outRecordTerm, outRecordWriterTerm, reusedOutRecord, nullCheck)

    val code =
      s"""
         |public class $className extends ${
            baseClass.getCanonicalName}<$BASE_ROW, ${outClass.getCanonicalName}> {
         |
         |  ${ctx.reuseMemberCode()}
         |
         |  public $className() throws Exception {
         |    ${ctx.reuseInitCode()}
         |  }
         |
         |  @Override
         |  public ${outClass.getCanonicalName} apply($BASE_ROW $inputTerm) {
         |    ${ctx.reuseFieldCode()}
         |    ${expression.code}
         |    return ${expression.resultTerm};
         |  }
         |}
        """.stripMargin

    GeneratedProjection(className, code, expression, inputMapping)
  }

  /**
    * For java invoke.
    */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int]): GeneratedProjection =
    generateProjection(
      ctx, name, inputType, outputType, inputMapping, inputTerm = DEFAULT_INPUT1_TERM)
}

abstract class Projection[IN <: BaseRow, OUT <: BaseRow] {
  def apply(row: IN): OUT
}
