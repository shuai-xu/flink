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

import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.catalog.CatalogFunction
import org.apache.flink.table.expressions._
import org.apache.flink.table.util.Logging

import scala.collection.JavaConversions._

/**
  * A chained catalog for looking up (user-defined) functions through chained catalogs,
  * used during validation phases of both Table API and SQL API.
  */
class ChainedFunctionCatalog(
    externalFunctionCatalog: ExternalFunctionCatalog,
    builtInFunctionCatalog: BuiltInFunctionCatalog)
  extends FunctionCatalog with Logging {

  override def registerFunction(name: String, catalogFunction: CatalogFunction): Unit =
    externalFunctionCatalog.registerFunction(name, catalogFunction)

  override def getSqlOperatorTable: SqlOperatorTable = {
    LOG.info("Getting sql operator tables")

    new ChainedSqlOperatorTable(
      Seq(externalFunctionCatalog.getSqlOperatorTable, builtInFunctionCatalog.getSqlOperatorTable))
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    // Search externalFunctionCatalog first
    try {
      externalFunctionCatalog.lookupFunction(name, children)
    } catch {
      case t: Throwable => {
        LOG.warn(s"Failed to find function ${name} in ExternalFunctionCatalog: " + t.getMessage)

        // Search builtinFunctionCatalog second
        try {
          builtInFunctionCatalog.lookupFunction(name, children);
        } catch {
          case t: Throwable => {
            LOG.warn(s"Failed to find function ${name} in BuiltInFunctionCatalog: " + t.getMessage)

            throw new ValidationException(
              s"Cannot find function ${name} with given parameters in any function catalogs")
          }
        }
      }
    }
  }

  override def dropFunction(name: String): Unit =
    externalFunctionCatalog.dropFunction(name)
}
