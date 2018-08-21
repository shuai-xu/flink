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

import java.util.regex.Pattern

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.{DataTypes, InternalType}
import org.apache.flink.table.util.{FlinkLike, StringLikeChainChecker}

/**
  * Generates Like function call.
  */
class LikeCallGen extends CallGenerator {

  /**
    * Accepts simple LIKE patterns like "abc%".
    */
  private val BEGIN_PATTERN = Pattern.compile("([^_%]+)%")

  /**
    * Accepts simple LIKE patterns like "%abc".
    */
  private val END_PATTERN = Pattern.compile("%([^_%]+)")

  /**
    * Accepts simple LIKE patterns like "%abc%".
    */
  private val MIDDLE_PATTERN = Pattern.compile("%([^_%]+)%")

  /**
    * Accepts simple LIKE patterns like "abc".
    */
  private val NONE_PATTERN = Pattern.compile("[^%_]+")

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {
    if (operands.size == 2 && operands(1).literal) {
      generateCallIfArgsNotNull(nullCheck, returnType, operands) {
        (terms) =>
          val pattern = operands(1).literalValue.toString
          val noneMatcher = NONE_PATTERN.matcher(pattern)
          val beginMatcher = BEGIN_PATTERN.matcher(pattern)
          val endMatcher = END_PATTERN.matcher(pattern)
          val middleMatcher = MIDDLE_PATTERN.matcher(pattern)

          if (noneMatcher.matches()) {
            s"${terms.head}.equals(${operands(1).resultTerm})"
          } else if (beginMatcher.matches()) {
            val field = ctx.addReusableStringConstants(beginMatcher.group(1))
            s"${terms.head}.startsWith($field)"
          } else if (endMatcher.matches()) {
            val field = ctx.addReusableStringConstants(endMatcher.group(1))
            s"${terms.head}.endsWith($field)"
          } else if (middleMatcher.matches()) {
            val field = ctx.addReusableStringConstants(middleMatcher.group(1))
            s"${terms.head}.contains($field)"
          }
          // '_' is a special char for like, so we skip it.
          // Accepts chained LIKE patterns without escaping like "abc%def%ghi%".
          else if (!pattern.contains("_") && pattern.contains('%')) {
            val field = classOf[StringLikeChainChecker].getCanonicalName
            val checker = newName("likeChainChecker")
            ctx.addReusableMember(s"$field $checker = new $field(${"\""}$pattern${"\""});")
            s"$checker.check(${terms.head})"
          } else {
            // Complex
            val patternClass = classOf[Pattern].getName
            val likeClass = classOf[FlinkLike].getName
            val patternName = newName("pattern")
            ctx.addReusableMember(
              s"""
                 |$patternClass $patternName =
                 |  $patternClass.compile(
                 |    $likeClass.sqlToRegexLike(${operands(1).resultTerm}.toString(), null));
                 |""".stripMargin)
            s"$patternName.matcher(${terms.head}.toString()).matches()"
          }
      }
    } else {
      generateDynamicLike(ctx, operands, nullCheck)
    }
  }

  def generateDynamicLike(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      nullCheck: Boolean): GeneratedExpression = {
    generateCallIfArgsNotNull(nullCheck, DataTypes.BOOLEAN, operands) {
      (terms) =>
        val str1 = s"${terms.head}.toString()"
        val str2 = s"${terms(1)}.toString()"
        val clsName = classOf[SqlFunctions].getName
        if (terms.length == 2) {
          s"$clsName.like($str1, $str2)"
        } else {
          val str3 = s"${terms(2)}.toString()"
          s"$clsName.like($str1, $str2, $str3)"
        }
    }
  }
}
