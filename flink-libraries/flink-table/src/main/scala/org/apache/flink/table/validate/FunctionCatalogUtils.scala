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

package org.apache.flink.table.validate

import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.functions._
import org.apache.flink.table.api.types.TypeInfoWrappedDataType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.catalog.CatalogFunction
import org.apache.flink.table.functions.utils.{ScalarSqlFunction, UserDefinedFunctionUtils}

/**
  * Utils for FunctionCatalog.
  */
object FunctionCatalogUtils {

  /**
    * Convert [[CatalogFunction]] to calcite SqlFunction.
    * note: tableEnvironment should not be null
    */
  def toSqlFunction(name: String, catalogFunc: CatalogFunction, typeFactory: FlinkTypeFactory):
    SqlFunction = {

    val functionInstance = UserDefinedFunctionUtils.createUserDefinedFunction(
      getClass.getClassLoader,
      name,
      catalogFunc.getClazzName)

    val sqlFunction = {
      functionInstance match {
        case _: ScalarFunction =>
          UserDefinedFunctionUtils.createScalarSqlFunction(
            name,
            name,
            functionInstance.asInstanceOf[ScalarFunction],
            typeFactory)
            .asInstanceOf[ScalarSqlFunction]

        // Does not support TableFunction and AggregateFunction currently
        // because, according to testing, the following previous code does not work
        // TODO: [BLINK-18982742] support initializing TableFunction from catalogs function classes
//        case _: TableFunction[_] =>
//          val implicitResultType = UserDefinedFunctionUtils.getImplicitResultType(
//            functionInstance.asInstanceOf[TableFunction[_]])
//          UserDefinedFunctionUtils.createTableSqlFunction(
//            name,
//            name,
//            functionInstance.asInstanceOf[TableFunction[_]],
//            implicitResultType.asInstanceOf[DataType],
//            typeFactory
//          )
        case _: AggregateFunction[_, _] =>
          val f = functionInstance.asInstanceOf[AggregateFunction[_, _]]
          val implicitResultType = new TypeInfoWrappedDataType(TypeExtractor
            .createTypeInfo(f, classOf[AggregateFunction[_, _]], f.getClass, 0))
          val implicitAccType = new TypeInfoWrappedDataType(TypeExtractor
            .createTypeInfo(f, classOf[AggregateFunction[_, _]], f.getClass, 1))
          val externalResultType = UserDefinedFunctionUtils.getResultTypeOfAggregateFunction(
            f, implicitResultType)
          val externalAccType = UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction(
            f, implicitAccType)
          UserDefinedFunctionUtils.createAggregateSqlFunction(
            name,
            name,
            f,
            externalResultType,
            externalAccType,
            typeFactory
          )
        case default =>
          throw new UnsupportedOperationException(
            "Does not support initializing functions other than ScalarFunction")
      }
    }

    sqlFunction
  }
}
