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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.CoTableValuedAggregateFunction
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getResultTypeOfAggregateFunction}

/**
  * A class which creates a call to a co-table-valued aggregateFunction
  */
class UDCTVAGGExpression[T: TypeInformation, ACC: TypeInformation](
  coAggregateFunction: CoTableValuedAggregateFunction[T, ACC]) {

  /**
    * Creates a call to a [[CoTableValuedAggregateFunction]].
    *
    * @param params1 actual parameters of function
    * @param params2 actual parameters of function
    * @return a [[AggFunctionCall]]
    */
  def apply(params1: Expression*)(params2: Expression*): CoTableValuedAggFunctionCall = {
    val resultType = getResultTypeOfAggregateFunction(
      coAggregateFunction,
      DataTypes.of(implicitly[TypeInformation[T]]))

    val accType = getAccumulatorTypeOfAggregateFunction(
      coAggregateFunction,
      DataTypes.of(implicitly[TypeInformation[ACC]]))

    CoTableValuedAggFunctionCall(
      coAggregateFunction,
      resultType,
      accType,
      ExpressionList(params1),
      ExpressionList(params2))
  }
}
