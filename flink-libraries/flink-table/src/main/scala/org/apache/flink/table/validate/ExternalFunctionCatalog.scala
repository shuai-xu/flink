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
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.catalog._
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getResultTypeOfCTDFunction
import org.apache.flink.table.functions.utils.{AggSqlFunction, ScalarSqlFunction, TableSqlFunction}
import org.apache.flink.table.util.Logging

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.mutable

/**
  * A catalog for looking up UDFs in catalogs via CatalogManager, used during validation phases
  * of both Table API and SQL API.
  */
class ExternalFunctionCatalog(catalogManager: CatalogManager, typeFactory: FlinkTypeFactory)
  extends FunctionCatalog with Logging {

  val sqlFunctions = mutable.ListBuffer[SqlFunction]()

  override def registerFunction(name: String, catalogFunction: CatalogFunction): Unit = {
    catalogManager.getDefaultCatalog.asInstanceOf[ReadableWritableCatalog]
      .createFunction(
        new ObjectPath(catalogManager.getDefaultDatabaseName, name.toLowerCase),
        catalogFunction,
        false
      )

    sqlFunctions += FunctionCatalogUtils.toSqlFunction(
      name,
      catalogFunction,
      typeFactory
    )
  }

  override def getSqlOperatorTable: SqlOperatorTable = {
    LOG.info("Getting sql operator tables")

    val catalog = catalogManager.getDefaultCatalog
    catalog.listFunctions(catalogManager.getDefaultDatabaseName).foreach(functionPath =>
      sqlFunctions += FunctionCatalogUtils.toSqlFunction(
        functionPath.getObjectName,
        catalog.getFunction(functionPath),
        typeFactory
      )
    )

    new ListSqlOperatorTable(sqlFunctions)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {

    val catalog = catalogManager.getDefaultCatalog

    val externalFunc =
      catalog.getFunction(new ObjectPath(catalogManager.getDefaultDatabaseName, name.toLowerCase))

    val sqlFunction = FunctionCatalogUtils.toSqlFunction(name, externalFunc, typeFactory)
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
        val aggSqlFunction = sqlFunction.asInstanceOf[AggSqlFunction]
        AggFunctionCall(
          aggSqlFunction.getFunction,
          aggSqlFunction.externalResultType,
          aggSqlFunction.externalAccType,
          children)
      case _ =>
        throw new RuntimeException(s"Cannot match sql function ${name} with any existing types")
    }
  }

  def dropFunction(name: String): Unit = {
    catalogManager.getDefaultCatalog.asInstanceOf[ReadableWritableCatalog]
      .dropFunction(new ObjectPath(catalogManager.getDefaultDatabaseName, name), false)
  }
}
