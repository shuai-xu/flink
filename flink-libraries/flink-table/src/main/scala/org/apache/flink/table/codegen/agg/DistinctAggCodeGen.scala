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
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.codegen.GeneratedExpression._
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.util.DistinctInfo
import org.apache.flink.table.types.{BaseRowType, DataType, DataTypes, InternalType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.typeutils.TypeUtils.createTypeInfoFromDataType

/**
  * It is for code generate distinct aggregate. The distinct aggregate buffer is a MapView which
  * is used to store the unique keys and the frequency of appearance. When a key is been seen the
  * first time, we will trigger the inner aggregate function's accumulate() function.
  *
  * @param ctx  the code gen context
  * @param distinctInfo the distinct information
  * @param distinctIndex  the index of this distinct in all distincts
  * @param innerAggCodeGens the code generator of inner aggregate
  * @param filterExpression filter argument access expression, none if no filter
  * @param mergedAccOffset the mergedAcc may come from local aggregate,
  *                        this is the first buffer offset in the row
  * @param aggBufferOffset  the offset in the buffers of this aggregate
  * @param aggBufferSize  the total size of aggregate buffers
  * @param hasNamespace  whether the accumulators state has namespace
  * @param mergedAccOnHeap  whether the merged accumulator is on heap, otherwise is on state
  * @param consumeRetraction  whether the distinct consumes retraction
  * @param relBuilder the rel builder to translate expressions to calcite rex nodes
  */
class DistinctAggCodeGen(
  ctx: CodeGeneratorContext,
  distinctInfo: DistinctInfo,
  distinctIndex: Int,
  innerAggCodeGens: Array[AggCodeGen],
  filterExpression: Option[Expression],
  mergedAccOffset: Int,
  aggBufferOffset: Int,
  aggBufferSize: Int,
  hasNamespace: Boolean,
  mergedAccOnHeap: Boolean,
  consumeRetraction: Boolean,
  relBuilder: RelBuilder) extends AggCodeGen {

  val MAP_VIEW: String = className[MapView[_, _]]
  val MAP_ENTRY: String = className[java.util.Map.Entry[_, _]]
  val ITERABLE: String = className[java.lang.Iterable[_]]

  val externalAccType: DataType = distinctInfo.accType
  val keyType: DataType = distinctInfo.keyType
  val keyTypeTerm: String = createTypeInfoFromDataType(keyType).getTypeClass.getCanonicalName
  val distinctAccTerm: String = s"distinct_view_$distinctIndex"
  val distinctBackupAccTerm: String = s"distinct_backup_view_$distinctIndex"

  addReusableDistinctAccumulator()

  /**
    * Add the distinct accumulator to the member variable and open close methods.
    */
  private def addReusableDistinctAccumulator(): Unit = {
    // add state mapview to member field
    addReusableStateDataViews(ctx, hasNamespace, distinctInfo.dataViewSpec.toArray)
    // add distinctAccTerm to member field
    ctx.addReusableMember(s"private $MAP_VIEW $distinctAccTerm;")
    ctx.addReusableMember(s"private $MAP_VIEW $distinctBackupAccTerm;")
  }

  override def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    val accTerm = newName("distinct_acc")
    val code = s"$MAP_VIEW $accTerm = new $MAP_VIEW();"
    Seq(GeneratedExpression(accTerm, NEVER_NULL, code, DataTypes.internal(externalAccType)))
  }

  override def setAccumulator(generator: ExprCodeGenerator): String = {
    generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      aggBufferOffset,
      useStateDataView = true,
      useBackupDataView = false)
    // return empty because the access code is set in ctx's ReusableInputUnboxingExprs
    ""
  }

  override def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    Seq(GeneratedExpression(
      distinctAccTerm,
      NEVER_NULL,
      NO_CODE,
      DataTypes.internal(externalAccType)))
  }

  override def accumulate(generator: ExprCodeGenerator): String = {
    val keyExpr = generateKeyExpression(ctx, generator)
    val key = keyExpr.resultTerm
    val accumulateCode = innerAggCodeGens.map(_.accumulate(generator)).mkString("\n")
    val countTerm = newName("count")

    val body = if (consumeRetraction) {
      s"""
         |${keyExpr.code}
         |java.lang.Long $countTerm = (java.lang.Long) $distinctAccTerm.get($key);
         |if ($countTerm != null) {
         |  $countTerm += 1;
         |  if ($countTerm == 0) {    // cnt is -1 before
         |    $distinctAccTerm.remove($key);
         |  } else {
         |    $distinctAccTerm.put($key, $countTerm);
         |  }
         |} else {
         |  // this key is first been seen
         |  $distinctAccTerm.put($key, 1L);
         |  // do accumulate
         |  $accumulateCode
         |  // end do accumulate
         |}
      """.stripMargin
    } else {
      s"""
         |${keyExpr.code}
         |java.lang.Long $countTerm = (java.lang.Long) $distinctAccTerm.get($key);
         |if ($countTerm == null) {
         |  // this key is first been seen
         |  $distinctAccTerm.put($key, 1L);
         |  // do accumulate
         |  $accumulateCode
         |  // end do accumulate
         |}
      """.stripMargin
    }

    filterExpression match {
      case None => body
      case Some(expr) =>
        val generated = generator.generateExpression(expr.toRexNode(relBuilder))
        s"""
           |if (${generated.resultTerm}) {
           |  $body
           |}
         """.stripMargin
    }
  }

  override def retract(generator: ExprCodeGenerator): String = {
    if (!consumeRetraction) {
      throw TableException("This should never happen, please file a issue.")
    }
    val keyExpr = generateKeyExpression(ctx, generator)
    val key = keyExpr.resultTerm
    val retractCode = innerAggCodeGens.map(_.retract(generator)).mkString("\n")
    val countTerm = newName("count")

    val body =
      s"""
         |${keyExpr.code}
         |java.lang.Long $countTerm = (java.lang.Long) $distinctAccTerm.get($key);
         |if ($countTerm != null) {
         |  $countTerm -= 1;
         |  if ($countTerm == 0) { // cnt is +1 before
         |    $distinctAccTerm.remove($key);
         |    // do retract
         |    $retractCode
         |    // end do retract
         |  } else {
         |    $distinctAccTerm.put($key, $countTerm);
         |  }
         |} else {
         |  $distinctAccTerm.put($key, -1L);
         |}
       """.stripMargin

    filterExpression match {
      case None => body
      case Some(expr) =>
        val generated = generator.generateExpression(expr.toRexNode(relBuilder))
        s"""
           |if (${generated.resultTerm}) {
           |  $body
           |}
         """.stripMargin
    }
  }

  override def merge(generator: ExprCodeGenerator): String = {
    // generate other MapView acc field
    val otherAccExpr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      mergedAccOffset + aggBufferOffset,
      useStateDataView = !mergedAccOnHeap,
      useBackupDataView = true)
    val otherAccTerm = otherAccExpr.resultTerm
    val otherEntries = newName("otherEntries")

    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL, nullCheck = true)
      .bindInput(DataTypes.internal(keyType), inputTerm = DISTINCT_KEY_TERM)

    if (consumeRetraction) {
      val retractCodes = innerAggCodeGens.map(_.retract(exprGenerator)).mkString("\n")
      val accumulateCodes = innerAggCodeGens.map(_.accumulate(exprGenerator)).mkString("\n")

      s"""
         |$ITERABLE<$MAP_ENTRY> $otherEntries = ($ITERABLE<$MAP_ENTRY>) $otherAccTerm.entries();
         |if ($otherEntries != null) {
         |  for ($MAP_ENTRY entry: $otherEntries) {
         |    $keyTypeTerm $DISTINCT_KEY_TERM = ($keyTypeTerm) entry.getKey();
         |    ${ctx.reuseInputUnboxingCode(Set(DISTINCT_KEY_TERM))}
         |    java.lang.Long otherCnt = (java.lang.Long) entry.getValue();
         |    java.lang.Long thisCnt = (java.lang.Long) $distinctAccTerm.get($DISTINCT_KEY_TERM);
         |    if (thisCnt != null) {
         |      java.lang.Long mergedCnt = thisCnt + otherCnt;
         |      if (mergedCnt == 0) {
         |        $distinctAccTerm.remove($DISTINCT_KEY_TERM);
         |        if (thisCnt > 0) {
         |          // do retract
         |          $retractCodes
         |          // end do retract
         |        }
         |      } else if (mergedCnt < 0) {
         |        $distinctAccTerm.put($DISTINCT_KEY_TERM, mergedCnt);
         |        if (thisCnt > 0) {
         |          // do retract
         |          $retractCodes
         |          // end do retract
         |        }
         |      } else {    // mergedCnt > 0
         |        $distinctAccTerm.put($DISTINCT_KEY_TERM, mergedCnt);
         |        if (thisCnt < 0) {
         |          // do accumulate
         |          $accumulateCodes
         |          // end do accumulate
         |        }
         |      }
         |    } else {  // thisCnt == null
         |      if (otherCnt > 0) {
         |        $distinctAccTerm.put($DISTINCT_KEY_TERM, otherCnt);
         |        // do accumulate
         |        $accumulateCodes
         |        // end do accumulate
         |      } else if (otherCnt < 0) {
         |        $distinctAccTerm.put($DISTINCT_KEY_TERM, otherCnt);
         |      } // ignore otherCnt == 0
         |    }
         |  } // end foreach
         |} // end otherEntries != null
     """.stripMargin
    } else {
      // distinct without retraction not need to generate retract codes
      val accumulateCodes = innerAggCodeGens.map(_.accumulate(exprGenerator)).mkString("\n")

      s"""
         |$ITERABLE<$MAP_ENTRY> $otherEntries = ($ITERABLE<$MAP_ENTRY>) $otherAccTerm.entries();
         |if ($otherEntries != null) {
         |  for ($MAP_ENTRY entry: $otherEntries) {
         |    $keyTypeTerm $DISTINCT_KEY_TERM = ($keyTypeTerm) entry.getKey();
         |    java.lang.Long thisCnt = (java.lang.Long) $distinctAccTerm.get($DISTINCT_KEY_TERM);
         |    if (thisCnt == null) {
         |      $distinctAccTerm.put($DISTINCT_KEY_TERM, 1L);
         |      ${ctx.reuseInputUnboxingCode(Set(DISTINCT_KEY_TERM))}
         |      // do accumulate
         |      $accumulateCodes
         |      // end do accumulate
         |    }
         |    // ignore thisCnt != null
         |  } // end foreach
         |} // end otherEntries != null
     """.stripMargin
    }
  }

  override def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    throw new TableException(
      "Distinct shouldn't return result value, this is a bug, please file a issue.")
  }

  override def checkNeededMethods(
      needAccumulate: Boolean,
      needRetract: Boolean,
      needMerge: Boolean,
      needReset: Boolean): Unit = {
    // nothing to do
  }


  private def generateKeyExpression(
    ctx: CodeGeneratorContext,
    generator: ExprCodeGenerator
  ): GeneratedExpression = {
    val fieldExprs = distinctInfo.argIndexes.map(generateInputAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      _,
      nullableInput = false,
      nullCheck = true,
      objReuse = false))  // disable object reuse

    // the key expression of MapView
    if (fieldExprs.length > 1) {
      val keyTerm = newName(DISTINCT_KEY_TERM)
      val valueType = new BaseRowTypeInfo(
        classOf[GenericRow],
        fieldExprs.map(_.resultType).map(DataTypes.toTypeInfo): _*)

      // always create a new result row
      generator.generateResultExpression(
        fieldExprs,
        DataTypes.internal(valueType).asInstanceOf[BaseRowType],
        outRow = keyTerm,
        reusedOutRow = false)
    } else {
      fieldExprs.head
    }
  }

  /**
    * This method is mainly the same as CodeGenUtils.generateFieldAccess(), the only difference is
    * that this method using UpdatableRow to wrap BaseRow to handle DataViews.
    */
  private def generateAccumulatorAccess(
    ctx: CodeGeneratorContext,
    inputType: InternalType,
    inputTerm: String,
    index: Int,
    useStateDataView: Boolean,
    useBackupDataView: Boolean,
    nullableInput: Boolean = false,
    nullCheck: Boolean = true): GeneratedExpression = {

    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>

        val expr = if (distinctInfo.dataViewSpec.nonEmpty && useStateDataView) {
          ctx.addAllReusableFields(Set(s"$BASE_ROW $CURRENT_KEY = ctx.currentKey();"))
          val spec = distinctInfo.dataViewSpec.get
          val dataViewTerm = if (useBackupDataView) {
            createDataViewBackupTerm(spec)
          } else {
            createDataViewTerm(spec)
          }
          val resultTerm = if (useBackupDataView) {
            distinctBackupAccTerm
          } else {
            distinctAccTerm
          }

          val code = if (hasNamespace) {
            val expr = generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck = true)
            s"""
               |// when namespace is null, the dataview is used in heap, no key and namespace set
               |if ($NAMESPACE_TERM != null) {
               |  $dataViewTerm.setKeyNamespace($CURRENT_KEY, $NAMESPACE_TERM);
               |  $resultTerm = $dataViewTerm;
               |} else {
               |  ${expr.code}
               |  $resultTerm = ${expr.resultTerm};
               |}
            """.stripMargin
          } else {
            s"""
               |$dataViewTerm.setKey($CURRENT_KEY);
               |$resultTerm = $dataViewTerm;
            """.stripMargin
          }
          GeneratedExpression(resultTerm, NEVER_NULL, code, DataTypes.internal(externalAccType))
        } else {
          val expr = generateFieldAccess(ctx, inputType, inputTerm, index, nullCheck = true)
          if (useBackupDataView) {
            // this is called in the merge method
            expr
          } else {
            val code =
              s"""
                 |${expr.code}
                 |$distinctAccTerm = ${expr.resultTerm};
              """.stripMargin
            GeneratedExpression(distinctAccTerm, NEVER_NULL, code, expr.resultType)
          }
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, expr)
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }
}
