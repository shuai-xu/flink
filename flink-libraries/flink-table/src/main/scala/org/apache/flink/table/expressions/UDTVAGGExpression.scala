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
import org.apache.flink.table.functions.TableValuedAggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getResultTypeOfAggregateFunction}
import org.apache.flink.table.types.DataTypes

/**
  * A class which creates a call to a table-valued aggregateFunction
  */
case class UDTVAGGExpression[T: TypeInformation, ACC: TypeInformation](
  aggregateFunction: TableValuedAggregateFunction[T, ACC]) {

  /**
    * Creates a call to a [[TableValuedAggregateFunction]].
    *
    * @param params actual parameters of function
    * @return a [[AggFunctionCall]]
    */
  def apply(params: Expression*): TableValuedAggFunctionCall = {
    val resultType = getResultTypeOfAggregateFunction(
      aggregateFunction,
      DataTypes.of(implicitly[TypeInformation[T]]))

    val accType = getAccumulatorTypeOfAggregateFunction(
      aggregateFunction,
      DataTypes.of(implicitly[TypeInformation[ACC]]))

    TableValuedAggFunctionCall(aggregateFunction, resultType, accType, params)
  }
}
