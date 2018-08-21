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

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.types._

class EqualiserCodeGenerator(fieldTypes: Seq[InternalType]) {

  private val BASE_ROW = className[BaseRow]
  private val RECORD_EQUALISER = className[RecordEqualiser]
  private val LEFT_INPUT = "left"
  private val RIGHT_INPUT = "right"

  def generateRecordEqualiser(name: String): GeneratedRecordEqualiser = {
    // ignore time zone
    val ctx = CodeGeneratorContext(new TableConfig, supportReference = true)
    val className = newName(name)
    val codes = for (i <- fieldTypes.indices) yield {
      val fieldType = fieldTypes(i)
      val fieldTypeTerm = primitiveTypeTermForType(fieldType)
      val result = s"cmp$i"
      val leftNullTerm = "leftIsNull$" + i
      val rightNullTerm = "rightIsNull$" + i
      val leftFieldTerm = "leftField$" + i
      val rightFieldTerm = "rightField$" + i
      val equalsCode = if (isInternalPrimitive(fieldType)) {
        s"$leftFieldTerm == $rightFieldTerm"
      } else {
        s"$leftFieldTerm.equals($rightFieldTerm)"
      }
      val leftReadCode = baseRowFieldReadAccess(ctx, i, LEFT_INPUT, fieldType)
      val rightReadCode = baseRowFieldReadAccess(ctx, i, RIGHT_INPUT, fieldType)
      s"""
        |boolean $leftNullTerm = $LEFT_INPUT.isNullAt($i);
        |boolean $rightNullTerm = $RIGHT_INPUT.isNullAt($i);
        |boolean $result;
        |if ($leftNullTerm && $rightNullTerm) {
        |  $result = true;
        |} else if ($leftNullTerm || $rightNullTerm) {
        |  $result = false;
        |} else {
        |  $fieldTypeTerm $leftFieldTerm = $leftReadCode;
        |  $fieldTypeTerm $rightFieldTerm = $rightReadCode;
        |  $result = $equalsCode;
        |}
        |if (!$result) {
        |  return false;
        |}
      """.stripMargin
    }

    val functionCode =
      j"""
        public final class $className implements $RECORD_EQUALISER {

          ${ctx.reuseMemberCode()}

          public $className(Object[] references) {
            ${ctx.reuseInitCode()}
          }

          @Override
          public boolean equals($BASE_ROW $LEFT_INPUT, $BASE_ROW $RIGHT_INPUT) {
            ${ctx.reuseFieldCode()}
            ${codes.mkString("\n")}
            return true;
          }
        }
      """.stripMargin

    GeneratedRecordEqualiser(className, functionCode, ctx.references.toArray)
  }

  private def isInternalPrimitive(t: InternalType): Boolean = t match {
    case _: PrimitiveType => true

    case _: DateType => true
    case DataTypes.TIME => true
    case _: TimestampType => true

    case _ => false
  }

}
