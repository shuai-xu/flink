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

import org.apache.flink.api.common.functions
import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.pattern.interval.PatternWindowTimeFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, SubKeyedAggsHandleFunction}
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.types.InternalType

/**
  * Describes a generated expression.
  *
  * @param resultTerm term to access the result of the expression
  * @param nullTerm boolean term that indicates if expression is null
  * @param code code necessary to produce resultTerm and nullTerm
  * @param resultType type of the resultTerm
  * @param literal flag to indicate a constant expression do not reference input and can thus
  *                 be used in the member area (e.g. as constructor parameter of a reusable
  *                 instance)
  * @param literalValue if this expression is literal, literalValue is non-null.
  */
case class GeneratedExpression(
  resultTerm: String,
  nullTerm: String,
  code: String,
  resultType: InternalType,
  literal: Boolean = false,
  literalValue: Any = null,
  codeBuffer: Seq[String] = Seq(),
  preceding: String = "",
  flowing: String = "") {

  /**
    * Copy result term to target term if the reference is changed.
    * Note: We must ensure that the target can only be copied out, so that its object is definitely
    * a brand new reference, not the object being re-used.
    * @param target the target term that cannot be assigned a reusable reference.
    * @return code.
    */
  def copyResultTermToTargetIfChanged(target: String): String = {
    if (CodeGenUtils.needCopyForType(resultType)) {
      s"""
         |if ($target != $resultTerm) {
         |  $target = ($resultTerm).copy();
         |}
       """.stripMargin
    } else {
      s"$target = $resultTerm;"
    }
  }

  def cloneRefTermIfNeeded(): String = {
    if (CodeGenUtils.needCloneRefForType(resultType)) {
      s"$resultTerm.cloneReference()"
    } else {
     resultTerm
    }
  }
}

object GeneratedExpression {
  val ALWAYS_NULL = "true"
  val NEVER_NULL = "false"
  val NO_CODE = ""
}

/**
  *
  */
case class GeneratedAggExpressions(
    genInitialValues: Seq[Seq[GeneratedExpression]],
    genAccumulate: Option[Seq[Seq[GeneratedExpression]]],
    genMerge: Option[Seq[Seq[GeneratedExpression]]],
    genGetValue: Seq[GeneratedExpression])

/**
  * Describes a generated [[functions.Function]]
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  * @tparam F type of function
  * @tparam T type of function
  */
case class GeneratedFunction[F <: Function, T <: Any](
  name: String,
  code: String)

/**
  * Describes a generated [[InputFormat]].
  *
  * @param name class name of the generated input function.
  * @param code code of the generated Function.
  * @tparam F type of function
  * @tparam T type of function
  */
case class GeneratedInput[F <: InputFormat[_, _], T <: Any](
    name: String,
    code: String)

/**
  * Describes a generated [[org.apache.flink.util.Collector]].
  *
  * @param name class name of the generated Collector.
  * @param code code of the generated Collector.
  */
case class GeneratedCollector(name: String, code: String)

/**
  * Describes a generated [[org.apache.flink.cep.pattern.conditions.IterativeCondition]].
  *
  * @param name class name of the generated IterativeCondition.
  * @param code code of the generated IterativeCondition.
  */
case class GeneratedIterativeCondition(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[IterativeCondition[BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.PatternSelectFunction]].
  *
  * @param name class name of the generated PatternSelectFunction.
  * @param code code of the generated PatternSelectFunction.
  */
case class GeneratedPatternSelectFunction(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[PatternSelectFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.PatternTimeoutFunction]].
  *
  * @param name class name of the generated PatternTimeoutFunction.
  * @param code code of the generated PatternTimeoutFunction.
  */
case class GeneratedPatternTimeoutFunction(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[PatternTimeoutFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.PatternFlatSelectFunction]].
  *
  * @param name class name of the generated PatternFlatSelectFunction.
  * @param code code of the generated PatternFlatSelectFunction.
  */
case class GeneratedPatternFlatSelectFunction(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[PatternFlatSelectFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.PatternFlatTimeoutFunction]].
  *
  * @param name class name of the generated PatternFlatTimeoutFunction.
  * @param code code of the generated PatternFlatTimeoutFunction.
  */
case class GeneratedPatternFlatTimeoutFunction(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[PatternFlatTimeoutFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.streaming.api.operators.StreamOperator]].
  *
  * @param name class name of the generated StreamOperator.
  * @param code code of the generated StreamOperator.
  */
case class GeneratedOperator(name: String, code: String)

/**
  * Describes a generated [[org.apache.flink.table.runtime.sort.NormalizedKeyComputer]].
  *
  * @param name class name of the generated NormalizedKeyComputer.
  * @param code code of the generated NormalizedKeyComputer.
  */
case class GeneratedNormalizedKeyComputer(name: String, code: String)

/**
  * Describes a generated [[org.apache.flink.table.runtime.sort.RecordComparator]].
  *
  * @param name class name of the generated RecordComparator.
  * @param code code of the generated RecordComparator.
  */
case class GeneratedRecordComparator(name: String, code: String)

/**
  * Describes a generated [[org.apache.flink.table.codegen.Projection]].
  *
  * @param name class name of the generated Projection.
  * @param code code of the generated Projection.
  * @param expr projection code and result term.
  */
case class GeneratedProjection(name: String, code: String, expr: GeneratedExpression)

/**
  * Describes the members of generated sort.
  *
  * @param computer NormalizedKeyComputer.
  * @param comparator RecordComparator.
  * @param serializers serializers to init computer and comparator.
  * @param comparators comparators to init computer and comparator.
  */
case class GeneratedSorter(
    computer: GeneratedNormalizedKeyComputer, comparator: GeneratedRecordComparator,
    serializers: Array[TypeSerializer[_]], comparators: Array[TypeComparator[_]])

/**
  * Describes a generated [[org.apache.flink.table.codegen.JoinConditionFunction]].
  *
  * @param name class name of the generated JoinConditionFunction.
  * @param code code of the generated JoinConditionFunction.
  */
case class GeneratedJoinConditionFunction(name: String, code: String)

/**
  * Describes a generated [[org.apache.flink.table.codegen.HashFunc]].
  *
  * @param name class name of the generated HashFunc.
  * @param code code of the generated HashFunc.
  */
case class GeneratedHashFunc(name: String, code: String)

/**
  * Describes a generated [[org.apache.flink.table.runtime.functions.ProcessFunction]]
  * or [[org.apache.flink.table.runtime.functions.ProcessFunction]].
  *
  * @param name class name of the generated ProcessFunction.
  * @param code code of the generated ProcessFunction.
  */
case class GeneratedProcessFunction(name: String, code: String)

/**
  * default implementation of GeneratedSplittable
  * @param definitions split function definitions like 'void check(Object in)'
  * @param bodies the split function body
  * @param callings the split function call
  */
case class GeneratedSplittableExpression(
  definitions: Seq[String],
  bodies: Seq[String],
  callings: Seq[String],
  isSplit: Boolean)

/**
 * Describes a generated [[org.apache.flink.table.runtime.operator.overagg.BoundComparator]].
 *
 * @param name class name of the generated BoundComparator.
 * @param code code of the generated BoundComparator.
 */
case class GeneratedBoundComparator(name: String, code: String)

/**
  * Describes a generated aggregate helper function
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedAggsHandleFunction(name: String, code: String, references: Array[AnyRef])
  extends GeneratedClass[AggsHandleFunction]

/**
  * Describes a generated subkeyed aggregate helper function
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedSubKeyedAggsHandleFunction[N](
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[SubKeyedAggsHandleFunction[N]]

/**
  * Describes a generated [[RecordEqualiser]]
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedRecordEqualiser(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RecordEqualiser]

/**
  * Describes a generated [[FieldAccess]]
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedFieldExtractor(
    name: String,
    code: String,
    references: Array[AnyRef])
  extends GeneratedClass[FieldAccess[_,_]]

object GeneratedSplittableExpression {
  /**
    * default implementation of GeneratedSplittable, which can't be split
    */
  val UNSPLIT_EXPRESSION = GeneratedSplittableExpression(Seq(), Seq(), Seq(), isSplit = false)
}
