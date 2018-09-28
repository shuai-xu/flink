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

import org.apache.calcite.sql._
import org.apache.calcite.sql.util.ListSqlOperatorTable
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.catalog.{CrudExternalCatalog, ExternalCatalog, ExternalCatalogFunction}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getResultTypeOfCTDFunction
import org.apache.flink.table.functions.utils.{AggSqlFunction, ScalarSqlFunction, TableSqlFunction, UserDefinedFunctionUtils}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.types.DataTypes

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.mutable

/**
  * A catalog for looking up (user-defined) functions, used during validation phases
  * of both Table API and SQL API.
  */
class ExternalFunctionCatalog(
    externalCatalog: ExternalCatalog,
    typeFactory: FlinkTypeFactory)
  extends FunctionCatalog {

  override def registerFunction(name: String, builder: Class[_]): Unit = {
    throw new UnsupportedOperationException("Unsupported operation of registering function")
  }

  override def registerSqlFunction(sqlFunction: SqlFunction): Unit = {
    externalCatalog match {
      case catalog: CrudExternalCatalog => {
        // TODO ignoreIfExists should not fixed
        catalog.createFunction(sqlFunction.getName, sqlFunction.getClass.getCanonicalName, false)
      }
      case _ =>
        throw new UnsupportedOperationException(
          "Unsupported operation of registering sql function in non CrudExternalCatalog!")
    }
  }

  override def getSqlOperatorTable: SqlOperatorTable = {
    val externalSqlFunctions = mutable.ListBuffer[SqlFunction]()
    externalCatalog.listFunctions().foreach(externalFunc => {
      val sqlFunc = toSqlFunction(externalFunc)
      externalSqlFunctions --= externalSqlFunctions.filter(_.getName == sqlFunc.getName)
      externalSqlFunctions += sqlFunc
    })

    new ListSqlOperatorTable(externalSqlFunctions)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val externalFunc = externalCatalog.asInstanceOf[CrudExternalCatalog].getFunction(name)

    val sqlFunction = toSqlFunction(externalFunc)
    sqlFunction match {
      case _: ScalarSqlFunction =>
        val scalarSqlFunction = sqlFunction.asInstanceOf[ScalarSqlFunction]
        ScalarFunctionCall(scalarSqlFunction.getScalarFunction, children)
      case _: TableSqlFunction =>
        val tableSqlFunction = sqlFunction.asInstanceOf[TableSqlFunction]
        TableFunctionCall(
          name,
          tableSqlFunction.getTableFunction,
          children,
          getResultTypeOfCTDFunction(
            tableSqlFunction.getTableFunction,
            children.toArray, () => tableSqlFunction.getImplicitResultType))
      case _: AggSqlFunction =>
        val aggSqlFunction = sqlFunction.asInstanceOf[AggregateFunction[_, _]]
        AggFunctionCall(
          aggSqlFunction,
          aggSqlFunction.getResultType,
          aggSqlFunction.getAccumulatorType,
          children)
    }
  }

  /**
    * Convert ExternalCatalogFunction to calcite SqlFunction.
    * note: tableEnvironment should not be null
    */
  def toSqlFunction(externalFunc: ExternalCatalogFunction): SqlFunction = {
    val name = externalFunc.funcName

    val functionInstance = UserDefinedFunctionUtils.createUserDefinedFunction(
      Thread.currentThread().getContextClassLoader,
      name,
      externalFunc.className)

    val sqlFunction = {
      functionInstance match {
        case _: ScalarFunction =>
          UserDefinedFunctionUtils.createScalarSqlFunction(
            name,
            name,
            functionInstance.asInstanceOf[ScalarFunction],
            typeFactory)
            .asInstanceOf[ScalarSqlFunction]
        case _: TableFunction[_] =>
          val implicitResultType = UserDefinedFunctionUtils.getImplicitResultType(
            functionInstance.asInstanceOf[TableFunction[_]])
          UserDefinedFunctionUtils.createTableSqlFunction(
            name,
            name,
            functionInstance.asInstanceOf[TableFunction[_]],
            implicitResultType,
            typeFactory
          ).asInstanceOf[TableSqlFunction]
        case _: AggregateFunction[_, _] =>
          val f = functionInstance.asInstanceOf[AggregateFunction[_, _]]
          val implicitResultType = DataTypes.of(TypeExtractor
            .createTypeInfo(f, classOf[AggregateFunction[_, _]], f.getClass, 0))
          val implicitAccType = DataTypes.of(TypeExtractor
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
      }
    }

    sqlFunction
  }

  override def dropFunction(name: String): Boolean = {
    // TODO drop from externalCatalog
    throw new UnsupportedOperationException("Unsupported operation of dropping function!")
  }

  override def clear(): Unit = {
    // TODO drop from externalCatalog
    throw new UnsupportedOperationException("Unsupported operation of clearing function!")
  }
}
