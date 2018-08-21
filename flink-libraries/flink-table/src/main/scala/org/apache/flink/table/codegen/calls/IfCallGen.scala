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

package org.apache.flink.table.codegen.calls

import org.apache.flink.table.codegen.CodeGenUtils.{newName, primitiveTypeTermForType}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.InternalType

/**
  * Generates IF function call.
  */
class IfCallGen() extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean)
    : GeneratedExpression = {

    // Inferred return type is ARG1. Must be the same as ARG2
    if (operands(1).resultType != operands(2).resultType) {
      throw new Exception(String.format("Unsupported operand types: IF(boolean, %s, %s)",
        operands(1).resultType, operands(2).resultType))
    }
    val returnType = operands(1).resultType

    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForType(returnType)

    val resultCode =
      s"""
         |${operands.head.code}
         |$resultTypeTerm $resultTerm;
         |boolean $nullTerm;
         |if (${operands.head.resultTerm}) {
         |  ${operands(1).code}
         |  $resultTerm = ${operands(1).resultTerm};
         |  $nullTerm = ${operands(1).nullTerm};
         |} else {
         |  ${operands(2).code}
         |  $resultTerm = ${operands(2).resultTerm};
         |  $nullTerm = ${operands(2).nullTerm};
         |}
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }
}
