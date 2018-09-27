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

import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.{FlinkTypeFactory, RexAggBufferVariable, RexAggLocalVariable}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.functions.sql.{ProctimeSqlFunction, StreamRecordTimestampSqlFunction}
import org.apache.flink.table.dataformat._
import org.apache.flink.table.types._
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.util.Preconditions.checkArgument

import scala.collection.JavaConversions._

/**
 * This code generator is mainly responsible for generating codes for a given calcite [[RexNode]].
 * It can also generate type conversion codes for the result converter.
 */
class ExprCodeGenerator(ctx: CodeGeneratorContext, nullableInput: Boolean, nullCheck: Boolean)
  extends RexVisitor[GeneratedExpression] {

  // check if nullCheck is enabled when inputs can be null
  if (nullableInput && !nullCheck) {
    throw new CodeGenException("Null check must be enabled if entire rows can be null.")
  }

  /**
   * term of the [[ProcessFunction]]'s context, can be changed when needed
   */
  var contextTerm = "ctx"

  /**
   * information of the first input
   */
  var input1Type: InternalType = _
  var input1Term: String = _
  var input1FieldMapping: Option[Array[Int]] = None

  /**
   * information of the optional second input
   */
  var input2Type: Option[InternalType] = None
  var input2Term: Option[String] = None
  var input2FieldMapping: Option[Array[Int]] = None

  /**
   * Bind the input information, should be called before generating expression.
   */
  def bindInput(
      inputType: InternalType,
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input1Type = inputType
    input1Term = inputTerm
    input1FieldMapping = inputFieldMapping
    this
  }

  /**
   * In some cases, the expression will have two inputs (e.g. join condition and udtf). We should
   * bind second input information before use.
   */
  def bindSecondInput(
      inputType: InternalType,
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input2Type = Some(inputType)
    input2Term = Some(inputTerm)
    input2FieldMapping = inputFieldMapping
    this
  }

  protected lazy val input1Mapping: Array[Int] = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => (0 until DataTypes.getArity(input1Type)).toArray
  }

  protected lazy val input2Mapping: Array[Int] = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ => input2Type match {
      case Some(input) => (0 until DataTypes.getArity(input)).toArray
      case _ => Array[Int]()
    }
  }

  /**
    * Generates an expression from a RexNode. If objects or variables can be reused, they will be
    * added to reusable code sections internally.
    *
    * @param rex Calcite row expression
    * @return instance of GeneratedExpression
    */
  def generateExpression(rex: RexNode): GeneratedExpression = {
    rex.accept(this)
  }

  /**
   * Generates an expression that converts the first input (and second input) into the given type.
   * If two inputs are converted, the second input is appended. If objects or variables can
   * be reused, they will be added to reusable code sections internally. The evaluation result
   * will be stored in the variable outRecordTerm.
   *
   * @param returnType conversion target type. Inputs and output must have the same arity.
   * @param outRecordTerm the result term
   * @param outRecordWriterTerm the result writer term
   * @param reusedOutRow If objects or variables can be reused, they will be added to reusable
   * code sections internally.
   * @return instance of GeneratedExpression
   */
  def generateConverterResultExpression(
      returnType: BaseRowType,
      outRecordTerm: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRow: Boolean = true,
      objReuse: Boolean = true,
      rowtimeExpression: Option[RexNode] = None)
    : GeneratedExpression = {
    val input1AccessExprs = input1Mapping.map {
      case DataTypes.ROWTIME_STREAM_MARKER |
           DataTypes.ROWTIME_BATCH_MARKER if rowtimeExpression.isDefined =>
        // generate rowtime attribute from expression
        generateExpression(rowtimeExpression.get)
      case DataTypes.ROWTIME_STREAM_MARKER |
           DataTypes.ROWTIME_BATCH_MARKER =>
        throw TableException("Rowtime extraction expression missing. Please report a bug.")
      case DataTypes.PROCTIME_STREAM_MARKER =>
        // attribute is proctime indicator.
        // we use a null literal and generate a timestamp when we need it.
        generateNullLiteral(DataTypes.PROCTIME_INDICATOR, nullCheck)
      case DataTypes.PROCTIME_BATCH_MARKER =>
        // attribute is proctime field in a batch query.
        // it is initialized with the current time.
        generateCurrentTimestamp(ctx)
      case idx =>
        // get type of result field
        generateInputAccess(
          ctx,
          input1Type,
          input1Term,
          idx,
          nullableInput,
          nullCheck,
          objReuse)
    }

    val input2AccessExprs = input2Type match {
      case Some(ti) =>
        input2Mapping.map(idx => generateInputAccess(
          ctx,
          ti,
          input2Term.get,
          idx,
          nullableInput,
          nullCheck)
        ).toSeq
      case None => Seq() // add nothing
    }

    generateResultExpression(
      input1AccessExprs ++ input2AccessExprs,
      returnType,
      outRow = outRecordTerm,
      outRowWriter = Some(outRecordWriterTerm),
      reusedOutRow = reusedOutRow)
  }

  /**
    * Generates an expression from a sequence of other expressions. The evaluation result
    * may be stored in the variable outRecordTerm.
    *
    * @param fieldExprs field expressions to be converted
    * @param returnType conversion target type. Type must have the same arity than fieldExprs.
    * @param outRow the result term
    * @param outRowWriter the result writer term for BinaryRow.
    * @param reusedOutRow If objects or variables can be reused, they will be added to reusable
    *                     code sections internally.
    * @param outRowAlreadyExists Don't need addReusableRecord if out row already exists.
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: BaseRowType,
      outRow: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM,
      outRowWriter: Option[String] = Some(CodeGeneratorContext.DEFAULT_OUT_RECORD_WRITER_TERM),
      reusedOutRow: Boolean = true,
      outRowAlreadyExists: Boolean = false): GeneratedExpression = {
    val fieldExprIdxToOutputRowPosMap = fieldExprs.indices.map(i => i -> i).toMap
    generateResultExpression(fieldExprs, fieldExprIdxToOutputRowPosMap, returnType, outRow,
      outRowWriter, reusedOutRow, outRowAlreadyExists)
  }

  /**
   * Generates an expression from a sequence of other expressions. The evaluation result
   * may be stored in the variable outRecordTerm.
   *
   * @param fieldExprs field expressions to be converted
   * @param fieldExprIdxToOutputRowPosMap Mapping index of fieldExpr in `fieldExprs`
   *                                      to position of output row.
   * @param returnType conversion target type. Type must have the same arity than fieldExprs.
   * @param outRow the result term
   * @param outRowWriter the result writer term for BinaryRow.
   * @param reusedOutRow If objects or variables can be reused, they will be added to reusable
   *                     code sections internally.
   * @param outRowAlreadyExists Don't need addReusableRecord if out row already exists.
   * @return instance of GeneratedExpression
   */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      fieldExprIdxToOutputRowPosMap: Map[Int, Int],
      returnType: BaseRowType,
      outRow: String,
      outRowWriter: Option[String],
      reusedOutRow: Boolean,
      outRowAlreadyExists: Boolean)
    : GeneratedExpression = {
    // initial type check
    if (returnType.getArity != fieldExprs.length) {
      throw new CodeGenException(
        s"Arity [${returnType.getArity}] of result type [$returnType] does not match " +
            s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    if (fieldExprIdxToOutputRowPosMap.size != fieldExprs.length) {
      throw new CodeGenException(
        s"Size [${returnType.getArity}] of fieldExprIdxToOutputRowPosMap does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    // type check
    fieldExprs.zipWithIndex foreach {
      // timestamp type(Include TimeIndicator) and generic type can compatible with each other.
      case (fieldExpr, i)
        if fieldExpr.resultType.isInstanceOf[GenericType[_]] ||
            fieldExpr.resultType.isInstanceOf[TimestampType] =>
        if (returnType.getTypeAt(i).getClass != fieldExpr.resultType.getClass) {
          throw new CodeGenException(
            s"Incompatible types of expression and result type, Expression[$fieldExpr] type is " +
                s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
        }
      case (fieldExpr, i) if fieldExpr.resultType != returnType.getTypeAt(i) =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression[$fieldExpr] type is " +
              s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
      case _ => // ok
    }

    def getOutputRowPos(fieldExprIdx: Int): Int =
      fieldExprIdxToOutputRowPosMap.getOrElse(fieldExprIdx,
        throw new CodeGenException(s"Illegal field expr index: $fieldExprIdx"))

    def objectArrayRowWrite(
        genUpdate: (Int, GeneratedExpression) => String): GeneratedExpression = {
      val initReturnRecord = if (outRowAlreadyExists) {
        ""
      } else {
        ctx.addOutputRecord(returnType, outRow, reused = reusedOutRow)
      }
      val resultBuffer: Seq[String] = fieldExprs.zipWithIndex map {
        case (fieldExpr, i) =>
          val idx = getOutputRowPos(i)
          if (nullCheck) {
            s"""
               |${fieldExpr.code}
               |if (${fieldExpr.nullTerm}) {
               |  $outRow.setNullAt($idx);
               |} else {
               |  ${genUpdate(idx, fieldExpr)};
               |}
               |""".stripMargin
          }
          else {
            s"""
               |${fieldExpr.code}
               |${genUpdate(idx, fieldExpr)};
               |""".stripMargin
          }
      }

      val statement =
        s"""
           |$initReturnRecord
           |${resultBuffer.mkString("")}
         """.stripMargin.trim

      GeneratedExpression(outRow, "false", statement, returnType,
        codeBuffer = resultBuffer, preceding = s"$initReturnRecord")
    }

    returnType.getTypeClass match {
      case cls if cls == classOf[BinaryRow] =>
        outRowWriter match {
          case Some(writer) => // binary row writer.
            val initReturnRecord = if (outRowAlreadyExists) {
              ""
            } else {
              ctx.addOutputRecord(returnType, outRow, outRowWriter, reusedOutRow)
            }
            val resetWriter = if (nullCheck) s"$writer.reset();" else s"$writer.resetCursor();"
            val completeWriter: String = s"$writer.complete();"
            val resultBuffer = fieldExprs.zipWithIndex map {
              case (fieldExpr, i) =>
                val t = returnType.getTypeAt(i)
                val idx = getOutputRowPos(i)
                val writeCode = binaryWriterWriteField(ctx, idx, fieldExpr.resultTerm, writer, t)
                if (nullCheck) {
                  s"""
                     |${fieldExpr.code}
                     |if (${fieldExpr.nullTerm}) {
                     |  ${binaryWriterWriteNull(idx, writer, t)};
                     |} else {
                     |  $writeCode;
                     |}
                     |""".stripMargin.trim
                } else {
                  s"""
                     |${fieldExpr.code}
                     |$writeCode;
                     |""".stripMargin.trim
                }
            }

            val statement =
              s"""
                 |$initReturnRecord
                 |$resetWriter
                 |${resultBuffer.mkString("\n")}
                 |$completeWriter
                 |""".stripMargin.trim
            GeneratedExpression(outRow, "false", statement, returnType,
              codeBuffer = resultBuffer,
              preceding = s"$initReturnRecord\n$resetWriter",
              flowing = s"$completeWriter")

          case None => // update to binary row (setXXX).
            checkArgument(outRowAlreadyExists)
            val resultBuffer = fieldExprs.zipWithIndex map {
              case (fieldExpr, i) =>
                val t = returnType.getTypeAt(i)
                val idx = getOutputRowPos(i)
                val writeCode = binaryRowFieldSetAccess(idx, outRow, t, fieldExpr.resultTerm)
                if (nullCheck) {
                  s"""
                     |${fieldExpr.code}
                     |if (${fieldExpr.nullTerm}) {
                     |  ${binaryRowSetNull(idx, outRow, t)};
                     |} else {
                     |  $writeCode;
                     |}
                     |""".stripMargin.trim
                } else {
                  s"""
                     |${fieldExpr.code}
                     |$writeCode;
                     |""".stripMargin.trim
                }
            }
            GeneratedExpression(
              outRow, "false", resultBuffer.mkString(""), returnType)
        }

      case cls if cls == classOf[GenericRow] =>
        objectArrayRowWrite((i: Int, expr: GeneratedExpression) =>
        s"$outRow.update($i, ${expr.resultTerm})")

      case cls if cls == classOf[BoxedWrapperRow] =>
        objectArrayRowWrite((i: Int, expr: GeneratedExpression) =>
          boxedWrapperRowFieldUpdateAccess(i, expr.resultTerm, outRow, expr.resultType))
    }
  }

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < DataTypes.getArity(input1Type)) {
      (input1Type, input1Term)
    } else {
      (input2Type.getOrElse(throw new CodeGenException("Invalid input access.")),
        input2Term.getOrElse(throw new CodeGenException("Invalid input access.")))
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - DataTypes.getArity(input1Type)
    }

    generateInputAccess(ctx, input._1, input._2, index, nullableInput, nullCheck)
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): GeneratedExpression =
    visitInputRef(rexTableInputRef)

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = {
    val refExpr = rexFieldAccess.getReferenceExpr.accept(this)
    val index = rexFieldAccess.getField.getIndex
    val fieldAccessExpr = generateFieldAccess(
      ctx,
      refExpr.resultType,
      refExpr.resultTerm,
      index,
      nullCheck)

    val resultTypeTerm = primitiveTypeTermForType(fieldAccessExpr.resultType)
    val defaultValue = primitiveDefaultValue(fieldAccessExpr.resultType)
    val resultTerm = ctx.newReusableField("result", resultTypeTerm)
    val nullTerm = ctx.newReusableField("isNull", "boolean")

    val resultCode = if (nullCheck) {
      s"""
        |${refExpr.code}
        |if (${refExpr.nullTerm}) {
        |  $resultTerm = $defaultValue;
        |  $nullTerm = true;
        |}
        |else {
        |  ${fieldAccessExpr.code}
        |  $resultTerm = ${fieldAccessExpr.resultTerm};
        |  $nullTerm = ${fieldAccessExpr.nullTerm};
        |}
        |""".stripMargin
    } else {
      s"""
        |${refExpr.code}
        |${fieldAccessExpr.code}
        |$resultTerm = ${fieldAccessExpr.resultTerm};
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, fieldAccessExpr.resultType)
  }

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toInternalType(literal.getType)
    val value = literal.getValue3
    // null value with type
    if (value == null) {
      return generateNullLiteral(resultType, nullCheck)
    }
    // non-null values
    literal.getType.getSqlTypeName match {

      case BOOLEAN =>
        generateNonNullLiteral(resultType, literal.getValue3.toString, literal.getValue3, nullCheck)

      case TINYINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          resultType,
          decimal.byteValue().toString,
          decimal.byteValue(), nullCheck)

      case SMALLINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          resultType,
          decimal.shortValue().toString,
          decimal.shortValue(), nullCheck)

      case INTEGER =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          resultType,
          decimal.intValue().toString,
          decimal.intValue(), nullCheck)

      case BIGINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(
          resultType,
          decimal.longValue().toString + "L",
          decimal.longValue(), nullCheck)

      case FLOAT =>
        val floatValue = value.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NaN => generateNonNullLiteral(
            resultType, "java.lang.Float.NaN", Float.NaN, nullCheck)
          case Float.NegativeInfinity =>
            generateNonNullLiteral(
              resultType,
              "java.lang.Float.NEGATIVE_INFINITY",
              Float.NegativeInfinity, nullCheck)
          case Float.PositiveInfinity => generateNonNullLiteral(
            resultType,
            "java.lang.Float.POSITIVE_INFINITY",
            Float.PositiveInfinity, nullCheck)
          case _ => generateNonNullLiteral(
            resultType,
            floatValue.toString + "f",
            floatValue,
            nullCheck)
        }

      case DOUBLE =>
        val doubleValue = value.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NaN => generateNonNullLiteral(
            resultType, "java.lang.Double.NaN", Double.NaN, nullCheck)
          case Double.NegativeInfinity =>
            generateNonNullLiteral(
              resultType,
              "java.lang.Double.NEGATIVE_INFINITY",
              Double.NegativeInfinity, nullCheck)
          case Double.PositiveInfinity =>
            generateNonNullLiteral(
              resultType,
              "java.lang.Double.POSITIVE_INFINITY",
              Double.PositiveInfinity, nullCheck)
          case _ => generateNonNullLiteral(
            resultType, doubleValue.toString + "d", doubleValue, nullCheck)
        }
      case DECIMAL =>
        val precision = literal.getType.getPrecision
        val scale = literal.getType.getScale
        val fieldTerm = newName("decimal")
        val fieldDecimal =
          s"""
             |${classOf[Decimal].getCanonicalName} $fieldTerm =
             |    ${Decimal.Ref.castFrom}("${value.toString}", $precision, $scale);
             |""".stripMargin
        ctx.addReusableMember(fieldDecimal)
        generateNonNullLiteral(
          resultType,
          fieldTerm,
          Decimal.fromBigDecimal(value.asInstanceOf[JBigDecimal], precision, scale),
          nullCheck)

      case VARCHAR | CHAR =>
        val escapedValue = StringEscapeUtils.escapeJava(
          StringEscapeUtils.unescapeJava(value.toString)
        )
        val field = ctx.addReusableStringConstants(escapedValue)
        generateNonNullLiteral(
          resultType,
          field,
          BinaryString.fromString(escapedValue),
          nullCheck)
      case VARBINARY | BINARY =>
        val bytesVal = value.asInstanceOf[ByteString].getBytes
        val fieldTerm = ctx.addReusableObject(bytesVal, "binary",
          bytesVal.getClass.getCanonicalName)
        generateNonNullLiteral(
          resultType,
          fieldTerm,
          BinaryString.fromBytes(bytesVal),
          nullCheck)
      case SYMBOL =>
        generateSymbol(value.asInstanceOf[Enum[_]])

      case DATE =>
        generateNonNullLiteral(resultType, value.toString, value, nullCheck)

      case TIME =>
        generateNonNullLiteral(resultType, value.toString, value, nullCheck)

      case TIMESTAMP =>
        // Hack
        // Currently, in RexLiteral/SqlLiteral(Calcite), TimestampString has no time zone.
        // TimeString, DateString TimestampString are treated as UTC time/(unix time)
        // when they are converted/formatted/validated
        // Here, we adjust millis before Calcite solve TimeZone perfectly
        val millis = value.asInstanceOf[Long]
        val adjustedValue = millis - ctx.getTableConfig.getTimeZone.getOffset(millis)
        generateNonNullLiteral(
          resultType, adjustedValue.toString + "L", value, nullCheck)
      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(
            resultType,
            decimal.intValue().toString,
            decimal.intValue(), nullCheck)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of months.")
        }

      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(
            resultType,
            decimal.longValue().toString + "L",
            decimal.longValue(), nullCheck)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of milliseconds.")
        }

      case t@_ =>
        throw new CodeGenException(s"Type not supported: $t")
    }
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1Type)
  }

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression = localRef match {
    case localVar: RexAggBufferVariable =>
      val resultTerm = localVar.getName
      val nullTerm = resultTerm + "IsNull"
      val pType = primitiveTypeTermForType(localVar.internalType)
      ctx.addReusableMember(s"$pType $resultTerm;")
      ctx.addReusableMember(s"boolean $nullTerm;")
      GeneratedExpression(resultTerm, nullTerm, "", localVar.internalType)
    case local: RexAggLocalVariable =>
      GeneratedExpression(local.fieldTerm, local.nullTerm, NO_CODE, local.internalType)
    case _ => throw new CodeGenException("Local variables are not supported yet.")
  }

  override def visitRangeRef(rangeRef: RexRangeRef): GeneratedExpression =
    throw new CodeGenException("Range references are not supported yet.")

  override def visitDynamicParam(dynamicParam: RexDynamicParam): GeneratedExpression =
    throw new CodeGenException("Dynamic parameter references are not supported yet.")

  override def visitCall(call: RexCall): GeneratedExpression = {

    // special case: time materialization
    if (call.getOperator == ProctimeSqlFunction) {
      return generateProctimeTimestamp(contextTerm, ctx)
    }

    if (call.getOperator == StreamRecordTimestampSqlFunction) {
      return generateRowtimeAccess(contextTerm, ctx)
    }

    val isIdempotent = call.getOperator.isDeterministic && (!call.getOperator.isDynamicFunction)
    if (isIdempotent) {
      val key = getReusableExpressionKey(call)
      ctx.getReusableExpression(key) match {
        case Some(expr) => return expr
        case _ =>
      }
    }

    val resultType = FlinkTypeFactory.toInternalType(call.getType)

    // convert operands and help giving untyped NULL literals a type
    val operands = call.getOperands.zipWithIndex.map {

      // this helps e.g. for AS(null)
      // we might need to extend this logic in case some rules do not create typed NULLs
      case (operandLiteral: RexLiteral, 0) if
          operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
          call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
        generateNullLiteral(resultType, nullCheck)

      case (o@_, idx) =>
        // for AND/OR/CASE/COALESCE, except for the first operand, other
        // operands will enter a new scope
        // Note: 'IF' function was introduced by Blink, it is a scalar function,
        // without specific SqlKind info. Here we have to use the 'name'
        // 'IF' function does create new variable scope(see #IfCallGen)
        val newScope = idx > 0 &&
          (call.getKind == SqlKind.AND
            || call.getKind == SqlKind.OR
            || call.getKind == SqlKind.CASE
            || call.getKind == SqlKind.COALESCE
            || call.op.getName == "IF")
        if (newScope) {
          ctx.enterScope()
        }
        val operands = o.accept(this)
        if (newScope) {
          ctx.exitScope()
        }
        operands
    }

    val expr = generateCallExpression(
      ctx, call.getOperator, operands, resultType, nullCheck)


    // only reuse scalar function right now.
    if (isIdempotent && call.getKind == SqlKind.OTHER_FUNCTION) {
      // remove the code. it will be executed only once.
      val reusedExpr = GeneratedExpression(expr.resultTerm, expr.nullTerm, "", expr.resultType)
      val key = getReusableExpressionKey(call)
      ctx.addReusableExpression(key, reusedExpr)
    }

    expr
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")

  private def getReusableExpressionKey(call: RexCall): String =
    call.toString + " | " + call.`type`.getFullTypeString
}
