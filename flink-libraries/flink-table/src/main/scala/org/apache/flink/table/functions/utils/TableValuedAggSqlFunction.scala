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

package org.apache.flink.table.functions.utils

import java.util

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.{SqlCallBinding, SqlIdentifier, SqlOperandCountRange, SqlOperator, SqlOperatorBinding}
import org.apache.calcite.sql.`type`.{SqlOperandCountRanges, SqlOperandTypeChecker, SqlOperandTypeInference, SqlReturnTypeInference}
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.TableValuedAggregateFunction
import org.apache.flink.table.functions.utils.TableValuedAggSqlFunction.{createOperandTypeChecker, createOperandTypeInference, createReturnTypeInference}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.types.{DataType, DataTypes, InternalType}

/**
  * Calcite wrapper for user-defined table-valued aggregate functions.
  *
  * @param tableValuedAggregateFunction table-valued aggregate function to be called
  * @param externalResultType           the type information of returned value
  * @param externalAccType              the type information of the accumulator
  * @param typeFactory                  type factory for converting Flink's between Calcite's types
  */
class TableValuedAggSqlFunction(
    name: String,
    displayName: String,
    tableValuedAggregateFunction: TableValuedAggregateFunction[_, _],
    val externalResultType: DataType,
    val externalAccType: DataType,
    typeFactory: FlinkTypeFactory)
  extends SqlUserDefinedAggFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    createReturnTypeInference(DataTypes.internal(externalResultType), typeFactory),
    createOperandTypeInference(name, tableValuedAggregateFunction, typeFactory),
    createOperandTypeChecker(name, tableValuedAggregateFunction),
    // Do not need to provide a calcite tableValuedAggregateFunction here.
    // Flink aggregateion function will be generated when translating the calcite relnode to flink
    // runtime execution plan
    null,
    false,
    tableValuedAggregateFunction.requiresOver(),
    typeFactory
  ) {

  def getFunction: TableValuedAggregateFunction[_, _] = tableValuedAggregateFunction

  override def isDeterministic: Boolean = tableValuedAggregateFunction.isDeterministic

  override def toString: String = displayName

  override def getParamTypes: util.List[RelDataType] = null
}

object TableValuedAggSqlFunction {

  def apply(
    name: String,
    displayName: String,
    tableValuedAggregateFunction: TableValuedAggregateFunction[_, _],
    externalResultType: DataType,
    externalAccType: DataType,
    typeFactory: FlinkTypeFactory): TableValuedAggSqlFunction = {

    new TableValuedAggSqlFunction(
      name,
      displayName,
      tableValuedAggregateFunction,
      externalResultType,
      externalAccType,
      typeFactory)
  }

  private[flink] def createOperandTypeInference(
    name: String,
    tableValuedAggregateFunction: TableValuedAggregateFunction[_, _],
    typeFactory: FlinkTypeFactory)
  : SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[TableValuedAggregateFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
        callBinding: SqlCallBinding,
        returnType: RelDataType,
        operandTypes: Array[RelDataType]): Unit = {

        val operandTypeInfo = getOperandType(callBinding)

        val foundSignature =
          getAccumulateMethodSignature(tableValuedAggregateFunction, operandTypeInfo)
          .getOrElse(
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
                s"Expected: ${signaturesToString(tableValuedAggregateFunction, "accumulate")}"))

        val inferredTypes = getParameterTypes(tableValuedAggregateFunction, foundSignature.drop(1))
                            .map(typeFactory.createTypeFromInternalType(_, isNullable = true))

        for (i <- operandTypes.indices) {
          if (i < inferredTypes.length - 1) {
            operandTypes(i) = inferredTypes(i)
          } else if (null != inferredTypes.last.getComponentType) {
            // last argument is a collection, the array type
            operandTypes(i) = inferredTypes.last.getComponentType
          } else {
            operandTypes(i) = inferredTypes.last
          }
        }
      }
    }
  }

  private[flink] def createReturnTypeInference(
    resultType: InternalType,
    typeFactory: FlinkTypeFactory)
  : SqlReturnTypeInference = {

    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        typeFactory.createTypeFromInternalType(resultType, isNullable = true)
      }
    }
  }

  private[flink] def createOperandTypeChecker(
    name: String,
    tableValuedAggregateFunction: TableValuedAggregateFunction[_, _])
  : SqlOperandTypeChecker = {

    val methods = checkAndExtractMethods(tableValuedAggregateFunction, "accumulate")

    /**
      * Operand type checker based on [[TableValuedAggregateFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(tableValuedAggregateFunction, "accumulate")}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        var min = 253
        var max = -1
        var isVarargs = false
        methods.foreach( m => {
          // do not count accumulator as input
          val inputParams = m.getParameterTypes.drop(1)
          var len = inputParams.length
          if (len > 0 && m.isVarArgs && inputParams(len - 1).isArray) {
            isVarargs = true
            len = len - 1
          }
          max = Math.max(len, max)
          min = Math.min(len, min)
        })
        if (isVarargs) {
          // if eval method is varargs, set max to -1 to skip length check in Calcite
          max = -1
        }

        SqlOperandCountRanges.between(min, max)
      }

      override def checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean)
      : Boolean = {
        val operandTypeInfo = getOperandType(callBinding)

        val foundSignature =
          getAccumulateMethodSignature(tableValuedAggregateFunction, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
                s"Expected: ${signaturesToString(tableValuedAggregateFunction, "accumulate")}")
          } else {
            false
          }
        } else {
          true
        }
      }

      override def isOptional(i: Int): Boolean = false

      override def getConsistency: Consistency = Consistency.NONE

    }
  }
}
