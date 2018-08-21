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
package org.apache.flink.table.codegen.agg

import java.lang.reflect.ParameterizedType
import java.lang.{Iterable => JIterable}

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.runtime.util.SingleElementIterator
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator.{BASE_ROW, CONTEXT_TERM, CURRENT_KEY, GENERIC_ROW, NAMESPACE_TERM}
import org.apache.flink.table.codegen.{CodeGenException, CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.expressions.{Expression, ResolvedAggInputReference}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.util.AggregateInfo
import org.apache.flink.table.dataformat.{GenericRow, UpdatableRow}
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.runtime.conversion.InternalTypeConverters.{genToExternal, genToInternal}
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.TypeUtils


/**
  * It is for code generate aggregation functions that are specified in terms of
  * accumulate(), retract() and merge() functions. The aggregate accumulator is
  * embedded inside of a larger shared aggregation buffer.
  *
  * @param ctx the code gen context
  * @param aggInfo  the aggregate information
  * @param filterExpression filter argument access expression, none if no filter
  * @param mergedAccOffset the mergedAcc may come from local aggregate,
  *                        this is the first buffer offset in the row
  * @param aggBufferOffset  the offset in the buffers of this aggregate
  * @param aggBufferSize  the total size of aggregate buffers
  * @param inputTypes   the input field type infos
  * @param constantExprs  the constant expressions
  * @param relBuilder  the rel builder to translate expressions to calcite rex nodes
  * @param hasNamespace  whether the accumulators state has namespace
  */
class ImperativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    mergedAccOffset: Int,
    aggBufferOffset: Int,
    aggBufferSize: Int,
    inputTypes: Seq[InternalType],
    constantExprs: Seq[GeneratedExpression],
    relBuilder: RelBuilder,
    hasNamespace: Boolean,
    mergedAccOnHeap: Boolean,
    mergedAccExternalType: DataType)
  extends AggCodeGen {

  private val SINGLE_ITERABLE = className[SingleElementIterator[_]]
  private val UPDATABLE_ROW = className[UpdatableRow]

  val function: AggregateFunction[_, _] = aggInfo.function.asInstanceOf[AggregateFunction[_, _]]
  val functionTerm: String = ctx.addReusableFunction(
    function,
    constructorTerm = s"$CONTEXT_TERM.getRuntimeContext()")
  val aggIndex: Int = aggInfo.aggIndex

  val externalAccType = aggInfo.externalAccTypes(0)

  /** whether the acc type is an internal type.
    * Currently we only support GenericRow as internal acc type */
  val isAccTypeInternal: Boolean = externalAccType match {
    // current we only support GenericRow as internal ACC type
    case t: BaseRowType if t.getTypeClass == classOf[GenericRow] => true
    case _ => false
  }

  val accInternalTerm: String = s"agg${aggIndex}_acc_internal"
  val accExternalTerm: String = s"agg${aggIndex}_acc_external"
  val accTypeInternalTerm: String = if (isAccTypeInternal) {
    GENERIC_ROW
  } else {
    boxedTypeTermForType(DataTypes.internal(externalAccType))
  }
  val accTypeExternalTerm: String = externalBoxedTermForType(externalAccType)

  val argTypes: Array[InternalType] = {
    val types = inputTypes ++ constantExprs.map(_.resultType)
    aggInfo.argIndexes.map(types(_))
  }

  val resultType: DataType = aggInfo.externalResultType

  val viewSpecs: Array[DataViewSpec] = aggInfo.viewSpecs
  // add reusable dataviews to context
  addReusableDataViews(viewSpecs)

  /**
    * Create DataView term, for example, acc1_map_dataview.
    *
    * @return term to access [[MapView]] or [[ListView]]
    */
  private def createDataViewTerm(spec: DataViewSpec): String = {
    s"${spec.stateId}_dataview"
  }

  /**
    * Create DataView backup term, for example, acc1_map_dataview_backup.
    * The backup dataview term is used for merging two statebackend
    * dataviews, e.g. session window.
    *
    * @return term to access backup [[MapView]] or [[ListView]]
    */
  private def createDataViewBackupTerm(spec: DataViewSpec): String = {
    s"${spec.stateId}_dataview_backup"
  }

  private def addReusableDataViews(viewSpecs: Array[DataViewSpec]): Unit = {
    // add reusable dataviews to context
    viewSpecs.foreach { spec =>
      val viewFieldTerm = createDataViewTerm(spec)
      val backupViewTerm = createDataViewBackupTerm(spec)
      val viewTypeTerm = spec.getStateDataViewClass(hasNamespace).getCanonicalName
      ctx.addReusableMember(s"private $viewTypeTerm $viewFieldTerm;")
      // always create backup dataview
      ctx.addReusableMember(s"private $viewTypeTerm $backupViewTerm;")

      val descTerm = ctx.addReusableObject(spec.toStateDescriptor, "desc")
      val createStateCall = spec.getCreateStateCall(hasNamespace)

      val openCode =
        s"""
           |$viewFieldTerm = new $viewTypeTerm($CONTEXT_TERM.$createStateCall($descTerm));
           |$backupViewTerm = new $viewTypeTerm($CONTEXT_TERM.$createStateCall($descTerm));
         """.stripMargin
      ctx.addReusableOpenStatement(openCode)

      // only cleanup dataview term, do not cleanup backup
      val cleanupCode = if (hasNamespace) {
        s"""
           |$viewFieldTerm.setKeyNamespace($CURRENT_KEY, $NAMESPACE_TERM);
           |$viewFieldTerm.clear();
        """.stripMargin
      } else {
        s"""
           |$viewFieldTerm.setKey($CURRENT_KEY);
           |$viewFieldTerm.clear();
        """.stripMargin
      }
      ctx.addReusableCleanupStatement(cleanupCode)
    }
  }

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    // do not set dataview into the acc in createAccumulator
    val accField = if (isAccTypeInternal) {
      // do not need convert to internal type
      s"$functionTerm.createAccumulator()"
    } else {
      genToInternal(ctx, externalAccType, s"$functionTerm.createAccumulator()")
    }
    val accInternal = newName("acc_internal")
    val code = s"$accTypeInternalTerm $accInternal = $accField;"
    Seq(GeneratedExpression(accInternal, "false", code, DataTypes.internal(externalAccType)))
  }

  def setAccumulator(generator: ExprCodeGenerator): String = {
    // generate internal acc field
    val expr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      aggBufferOffset,
      viewSpecs,
      useStateDataView = true,
      useBackupDataView = false,
      nullableInput = false,
      nullCheck = true)

    if (isAccTypeInternal) {
      ctx.addReusableMember(s"private $accTypeInternalTerm $accInternalTerm;")
      s"""
         |$accInternalTerm = ${expr.resultTerm};
      """.stripMargin
    } else {
      ctx.addReusableMember(s"private $accTypeInternalTerm $accInternalTerm;")
      ctx.addReusableMember(s"private $accTypeExternalTerm $accExternalTerm;")
      s"""
         |$accInternalTerm = ${expr.resultTerm};
         |$accExternalTerm = ${genToExternal(ctx, externalAccType, accInternalTerm)};
      """.stripMargin
    }
  }

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    val code = if (isAccTypeInternal) {
      // do not need convert to internal type
      ""
    } else {
      s"$accInternalTerm = ${genToInternal(ctx, externalAccType, accExternalTerm)};"
    }
    Seq(GeneratedExpression(accInternalTerm, "false", code, DataTypes.internal(externalAccType)))
  }

  def accumulate(generator: ExprCodeGenerator): String = {
    val parameters = aggParametersCode(generator)
    val call = s"$functionTerm.accumulate($parameters);"
    filterExpression match {
      case None => call
      case Some(expr) =>
        val generated = generator.generateExpression(expr.toRexNode(relBuilder))
        s"""
           |if (${generated.resultTerm}) {
           |  $call
           |}
         """.stripMargin
    }
  }

  def retract(generator: ExprCodeGenerator): String = {
    val parameters = aggParametersCode(generator)
    val call = s"$functionTerm.retract($parameters);"
    filterExpression match {
      case None => call
      case Some(expr) =>
        val generated = generator.generateExpression(expr.toRexNode(relBuilder))
        s"""
           |if (${generated.resultTerm}) {
           |  $call
           |}
         """.stripMargin
    }
  }

  def merge(generator: ExprCodeGenerator): String = {
    val accIterTerm = s"agg${aggIndex}_acc_iter"
    ctx.addReusableMember(s"private final $SINGLE_ITERABLE $accIterTerm = new $SINGLE_ITERABLE();")

    // generate internal acc field
    val expr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      mergedAccOffset + aggBufferOffset,
      viewSpecs,
      useStateDataView = !mergedAccOnHeap,
      useBackupDataView = true,
      nullableInput = false,
      nullCheck = true)

    if (isAccTypeInternal) {
      s"""
         |$accIterTerm.set(${expr.resultTerm});
         |$functionTerm.merge($accInternalTerm, $accIterTerm);
       """.stripMargin
    } else {
      val otherAccExternal = newName("other_acc_external")
      s"""
         |$accTypeExternalTerm $otherAccExternal = ${
            genToExternal(ctx, mergedAccExternalType, expr.resultTerm)};
         |$accIterTerm.set($otherAccExternal);
         |$functionTerm.merge($accExternalTerm, $accIterTerm);
      """.stripMargin
    }
  }

  def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    val valueExternalTerm = newName("value_external")
    val valueExternalTypeTerm = externalBoxedTermForType(resultType)
    val valueInternalTerm = newName("value_internal")
    val valueInternalTypeTerm = boxedTypeTermForType(DataTypes.internal(resultType))
    val nullTerm = newName("valueIsNull")
    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    val code =
      s"""
         |$valueExternalTypeTerm $valueExternalTerm = ($valueExternalTypeTerm)
         |  $functionTerm.getValue($accTerm);
         |$valueInternalTypeTerm $valueInternalTerm =
         |  ${genToInternal(ctx, resultType, valueExternalTerm)};
         |boolean $nullTerm = $valueInternalTerm == null;
      """.stripMargin

    GeneratedExpression(valueInternalTerm, nullTerm, code, DataTypes.internal(resultType))
  }

  private def aggParametersCode(generator: ExprCodeGenerator): String = {
    val externalUDITypes = getAggUserDefinedInputTypes(
      function,
      externalAccType,
      argTypes)
    val inputFields = aggInfo.argIndexes.zipWithIndex.map { case (f, index) =>
      if (f >= inputTypes.length) {
        // index to constant
        val expr = constantExprs(f - inputTypes.length)
        s"${expr.nullTerm} ? null : ${
          genToExternal(ctx, externalUDITypes(index), expr.resultTerm)}"
      } else {
        // index to input field
        val inputRef = ResolvedAggInputReference(f.toString, f, inputTypes(f))
        val inputExpr = generator.generateExpression(inputRef.toRexNode(relBuilder))
        var term = s"${genToExternal(ctx, externalUDITypes(index), inputExpr.resultTerm)}"
        if (needCloneRefForDataType(externalUDITypes(index))) {
          term = s"$term.cloneReference()"
        }
        s"${inputExpr.nullTerm} ? null : $term"
      }
    }

    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    // insert acc to the head of the list
    val fields = Seq(accTerm) ++ inputFields
    // acc, arg1, arg2
    fields.mkString(", ")
  }

  /**
    * This method is mainly the same as CodeGenUtils.generateFieldAccess(), the only difference is
    * that this method using UpdatableRow to wrap BaseRow to handle DataViews.
    */
  def generateAccumulatorAccess(
      ctx: CodeGeneratorContext,
      inputType: InternalType,
      inputTerm: String,
      index: Int,
      viewSpecs: Array[DataViewSpec],
      useStateDataView: Boolean,
      useBackupDataView: Boolean,
      nullableInput: Boolean,
      nullCheck: Boolean): GeneratedExpression = {

    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>
        val expr = if (nullableInput) {
          generateNullableInputFieldAccess(ctx, inputType, inputTerm, index, nullCheck)
        } else {
          generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck)
        }

        val newExpr = inputType match {
          case ct: BaseRowType if isAccTypeInternal =>
            // acc is never be null
            val fieldType = ct.getTypeAt(index).asInstanceOf[BaseRowType]
            val exprGenerator = new ExprCodeGenerator(ctx, false, nullCheck)
              .bindInput(fieldType, inputTerm = expr.resultTerm)
            val converted = exprGenerator.generateConverterResultExpression(
              fieldType,
              outRecordTerm = newName("acc"),
              reusedOutRow = false,
              objReuse = false)
            val code =
              s"""
                 |${expr.code}
                 |${ctx.reuseInputUnboxingCode(Set(expr.resultTerm))}
                 |${converted.code}
               """.stripMargin

            GeneratedExpression(
              converted.resultTerm,
              converted.nullTerm,
              code,
              converted.resultType)
          case _ => expr
        }

        val exprWithDataView = inputType match {
          case ct: BaseRowType if viewSpecs.nonEmpty && useStateDataView =>
            if (isAccTypeInternal) {
              val code =
                s"""
                  |${newExpr.code}
                  |${generateDataViewFieldSetter(newExpr.resultTerm, viewSpecs, useBackupDataView)}
                """.stripMargin
              GeneratedExpression(newExpr.resultTerm, newExpr.nullTerm, code, newExpr.resultType)
            } else {
              val fieldType = ct.getTypeAt(index)
              val fieldTerm = ctx.newReusableField("field", UPDATABLE_ROW)
              val code =
                s"""
                   |${newExpr.code}
                   |$fieldTerm = null;
                   |if (!${newExpr.nullTerm}) {
                   |  $fieldTerm = new $UPDATABLE_ROW(${newExpr.resultTerm}, ${
                        DataTypes.getArity(fieldType)});
                   |  ${generateDataViewFieldSetter(fieldTerm, viewSpecs, useBackupDataView)}
                   |}
                """.stripMargin
              GeneratedExpression(fieldTerm, newExpr.nullTerm, code, newExpr.resultType)
            }

          case _ => newExpr
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, exprWithDataView)
        exprWithDataView
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }


  /**
    * Generate statements to set data view field when use state backend.
    *
    * @param accTerm aggregation term
    * @return data view field set statements
    */
  private def generateDataViewFieldSetter(
      accTerm: String,
      viewSpecs: Array[DataViewSpec],
      useBackupDataView: Boolean): String = {
    ctx.addAllReusableFields(Set(s"$BASE_ROW $CURRENT_KEY = ctx.currentKey();"))
    val setters = for (spec <- viewSpecs) yield {
      if (hasNamespace) {
        val dataViewTerm = if (useBackupDataView) {
          createDataViewBackupTerm(spec)
        } else {
          createDataViewTerm(spec)
        }

        s"""
           |// when namespace is null, the dataview is used in heap, no key and namespace set
           |if ($NAMESPACE_TERM != null) {
           |  $dataViewTerm.setKeyNamespace($CURRENT_KEY, $NAMESPACE_TERM);
           |  $accTerm.update(${spec.fieldIndex}, $dataViewTerm);
           |}
         """.stripMargin
      } else {
        val dataViewTerm = createDataViewTerm(spec)

        s"""
           |$dataViewTerm.setKey($CURRENT_KEY);
           |$accTerm.update(${spec.fieldIndex}, $dataViewTerm);
        """.stripMargin
      }
    }
    setters.mkString("\n")
  }



  def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false): Unit = {

    val methodSignatures = typesToClasses(argTypes)

    if (needAccumulate) {
      getAggFunctionUDIMethod(function, "accumulate", externalAccType, argTypes)
        .getOrElse(
          throw new CodeGenException(
            s"No matching accumulate method found for AggregateFunction " +
              s"'${function.getClass.getCanonicalName}'" +
              s"with parameters '${signatureToString(methodSignatures)}'.")
        )
    }

    if (needRetract) {
      getAggFunctionUDIMethod(function, "retract", externalAccType, argTypes)
        .getOrElse(
          throw new CodeGenException(
            s"No matching retract method found for AggregateFunction " +
              s"'${function.getClass.getCanonicalName}'" +
              s"with parameters '${signatureToString(methodSignatures)}'.")
        )
    }

    if (needMerge) {
      val iterType = DataTypes.extractType(classOf[JIterable[Any]])
      val methods =
        getUserDefinedMethod(function, "merge", Array(externalAccType, iterType))
          .getOrElse(
            throw new CodeGenException(
              s"No matching merge method found for AggregateFunction " +
                s"${function.getClass.getCanonicalName}'.")
          )

      var iterableTypeClass = methods.getGenericParameterTypes.apply(1)
        .asInstanceOf[ParameterizedType].getActualTypeArguments.apply(0)
      // further extract iterableTypeClass if the accumulator has generic type
      iterableTypeClass match {
        case impl: ParameterizedType => iterableTypeClass = impl.getRawType
        case _ =>
      }

      if (iterableTypeClass != TypeUtils.getExternalClassForType(externalAccType)) {
        throw new CodeGenException(
          s"merge method in AggregateFunction ${function.getClass.getCanonicalName} does not " +
            s"have the correct Iterable type. Actually: $iterableTypeClass. " +
            s"Expected: ${TypeUtils.getExternalClassForType(externalAccType)}")
      }
    }

    if (needReset) {
      getUserDefinedMethod(function, "resetAccumulator", Array(externalAccType))
        .getOrElse(
          throw new CodeGenException(
            s"No matching resetAccumulator method found for " +
              s"aggregate ${function.getClass.getCanonicalName}'.")
        )
    }
  }
}
