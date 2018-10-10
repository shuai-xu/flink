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

import java.lang.reflect.Method

import org.apache.flink.table.codegen.CodeGenUtils.qualifyMethod
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.codegen.calls.CallGenerator.{generateCallIfArgsNotNull, generateCallIfArgsNullable}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.{DataTypes, InternalType}

/**
  * Generates a function call by using a [[java.lang.reflect.Method]].
  */
class MethodCallGen(
    method: Method,
    argNotNull: Boolean = true) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {

    val call = (origTerms: Seq[String]) => {
      val terms = origTerms.zipWithIndex.map { case (term, index) =>
        if (operands(index).resultType == DataTypes.STRING) {
          s"$term.toString()"
        } else {
          term
        }
      }

      if (FunctionGenerator.isFunctionWithTimeZone(method)) {
        val timeZone = ctx.addReusableTimeZone()
        // insert the zoneID parameters for timestamp functions
        s"""
           |${qualifyMethod(method)}(${terms.mkString(", ")}, $timeZone)
           |""".stripMargin

      } else {
        s"""
           |${qualifyMethod(method)}(${terms.mkString(", ")})
           |""".stripMargin
      }
    }

    val resultCall = if (returnType == DataTypes.STRING ) {
      (args: Seq[String]) => {
        s"$BINARY_STRING.fromString(${call(args)})"
      }
    } else {
      call
    }

    if (argNotNull) {
      generateCallIfArgsNotNull(ctx, nullCheck, returnType, operands)(resultCall)
    } else {
      generateCallIfArgsNullable(ctx, nullCheck, returnType, operands)(resultCall)
    }
  }
}
