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

package org.apache.flink.table.api

import com.google.common.base.Joiner
import org.apache.flink.table.catalog.TableSourceConverter
import org.apache.flink.table.catalog.ExternalCatalogTypes.PartitionSpec
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.factories.{TableFactory => JTableFactory}

/**
  * Exception for all errors occurring during expression parsing.
  */
case class ExpressionParserException(msg: String) extends RuntimeException(msg)

/**
  * Exception for all errors occurring during sql parsing.
  */
case class SqlParserException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

/**
  * General Exception for all errors during table handling.
  */
case class TableException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

object TableException {
  def apply(msg: String): TableException = new TableException(msg)
}

/**
  * Exception for all errors occurring during validation phase.
  */
case class ValidationException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

object ValidationException {
  def apply(msg: String): ValidationException = new ValidationException(msg)
}

/**
  * Exception for unwanted method calling on unresolved expression.
  */
case class UnresolvedException(msg: String) extends RuntimeException(msg)

/**
  * Exception for operation on a nonexistent partition
  *
  * @param catalog       catalog name
  * @param table         table name
  * @param partitionSpec partition spec
  * @param cause         the cause
  */
case class PartitionNotExistException(
    catalog: String,
    table: String,
    partitionSpec: PartitionSpec,
    cause: Throwable)
    extends RuntimeException(
      s"partition [${Joiner.on(",").withKeyValueSeparator("=").join(partitionSpec)}] " +
          s"does not exist in table $catalog.$table!", cause) {

  def this(catalog: String, table: String, partitionSpec: PartitionSpec) =
    this(catalog, table, partitionSpec, null)

}

/**
  * Exception for adding an already existed partition
  *
  * @param catalog       catalog name
  * @param table         table name
  * @param partitionSpec partition spec
  * @param cause         the cause
  */
case class PartitionAlreadyExistException(
    catalog: String,
    table: String,
    partitionSpec: PartitionSpec,
    cause: Throwable)
    extends RuntimeException(
      s"partition [${Joiner.on(",").withKeyValueSeparator("=").join(partitionSpec)}] " +
          s"already exists in table $catalog.$table!", cause) {

  def this(catalog: String, table: String, partitionSpec: PartitionSpec) =
    this(catalog, table, partitionSpec, null)

}

/**
  * Exception for an operation on a nonexistent table.
  *
  * @param catalog    catalog name
  * @param table      table name
  * @param cause      the cause
  */
case class TableNotExistException(
    catalog: String,
    table: String,
    cause: Throwable)
    extends RuntimeException(s"Table $catalog.$table does not exist.", cause) {

  def this(catalog: String, table: String) = this(catalog, table, null)

}

/**
  * Exception for an operation on a nonexistent function.
  *
  * @param catalog    catalog name
  * @param function   function name
  * @param cause      the cause
  */
case class FunctionNotExistException(
    catalog: String,
    function: String,
    cause: Throwable)
  extends RuntimeException(s"Function $catalog.$function does not exist.", cause) {

  def this(catalog: String, function: String) = this(catalog, function, null)

}

/**
  * Exception for an invalid function.
  * @param catalog catalog name
  * @param functionName function name
  * @param className class name
  * @param cause the cause
  */
case class InvalidFunctionException(
    catalog: String,
    functionName: String,
    className: String,
    cause: Throwable)
    extends RuntimeException(s"Function $functionName, class $className is invalid.", cause) {
}

/**
  * Exception for adding an already existent table
  *
  * @param catalog    catalog name
  * @param table      table name
  * @param cause      the cause
  */
case class TableAlreadyExistException(
    catalog: String,
    table: String,
    cause: Throwable)
    extends RuntimeException(s"Table $catalog.$table already exists.", cause) {

  def this(catalog: String, table: String) = this(catalog, table, null)

}

/**
  * Exception for adding an already existent function
  *
  * @param catalog    catalog name
  * @param function   function name
  * @param cause      the cause
  */
case class FunctionAlreadyExistException(
    catalog: String,
    function: String,
    cause: Throwable)
  extends RuntimeException(s"Function $catalog.$function already exists.", cause) {

  def this(catalog: String, function: String) = this(catalog, function, null)

}

/**
  * Exception for operation on a nonexistent catalog
  *
  * @param catalog catalog name
  * @param cause the cause
  */
case class CatalogNotExistException(
    catalog: String,
    cause: Throwable)
    extends RuntimeException(s"Catalog $catalog does not exist.", cause) {

  def this(catalog: String) = this(catalog, null)
}

/**
  * Exception for adding an already existent catalog
  *
  * @param catalog catalog name
  * @param cause the cause
  */
case class CatalogAlreadyExistException(
    catalog: String,
    cause: Throwable)
    extends RuntimeException(s"Catalog $catalog already exists.", cause) {

  def this(catalog: String) = this(catalog, null)
}

/**
  * Exception for not finding a [[JTableFactory]] for the given properties.
  *
  * @param message message that indicates the current matching step
  * @param factoryClass required factory class
  * @param factories all found factories
  * @param properties properties that describe the configuration
  * @param cause the cause
  */
case class NoMatchingTableFactoryException(
  message: String,
  factoryClass: Class[_],
  factories: Seq[JTableFactory],
  properties: Map[String, String],
  cause: Throwable)
  extends RuntimeException(
    s"""Could not find a suitable table factory for '${factoryClass.getName}' in
       |the classpath.
       |
        |Reason: $message
       |
        |The following properties are requested:
       |${DescriptorProperties.toString(properties)}
       |
        |The following factories have been considered:
       |${factories.map(_.getClass.getName).mkString("\n")}
       |""".stripMargin,
    cause) {

  def this(
    message: String,
    factoryClass: Class[_],
    factories: Seq[JTableFactory],
    properties: Map[String, String]) = {
    this(message, factoryClass, factories, properties, null)
  }
}

/**
  * Exception for finding more than one [[JTableFactory]] for the given properties.
  *
  * @param matchingFactories factories that match the properties
  * @param factoryClass required factory class
  * @param factories all found factories
  * @param properties properties that describe the configuration
  * @param cause the cause
  */
case class AmbiguousTableFactoryException(
  matchingFactories: Seq[JTableFactory],
  factoryClass: Class[_],
  factories: Seq[JTableFactory],
  properties: Map[String, String],
  cause: Throwable)
  extends RuntimeException(
    s"""More than one suitable table factory for '${factoryClass.getName}' could
       |be found in the classpath.
       |
        |The following factories match:
       |${matchingFactories.map(_.getClass.getName).mkString("\n")}
       |
        |The following properties are requested:
       |${DescriptorProperties.toString(properties)}
       |
        |The following factories have been considered:
       |${factories.map(_.getClass.getName).mkString("\n")}
       |""".stripMargin,
    cause) {

  def this(
    matchingFactories: Seq[JTableFactory],
    factoryClass: Class[_],
    factories: Seq[JTableFactory],
    properties: Map[String, String]) = {
    this(matchingFactories, factoryClass, factories, properties, null)
  }
}

/**
  * Exception for not finding a [[TableSourceConverter]] for a given table type.
  *
  * @param tableType table type
  * @param cause the cause
  */
case class NoMatchedTableSourceConverterException(
    tableType: String,
    cause: Throwable)
    extends RuntimeException(s"Could not find a TableSourceConverter for table type $tableType.",
      cause) {

  def this(tableType: String) = this(tableType, null)
}

/**
  * Exception for finding more than one [[TableSourceConverter]] for a given table type.
  *
  * @param tableType table type
  * @param cause the cause
  */
case class AmbiguousTableSourceConverterException(
    tableType: String,
    cause: Throwable)
    extends RuntimeException(s"More than one TableSourceConverter for table type $tableType.",
      cause) {

  def this(tableType: String) = this(tableType, null)
}

/**
  * Exception for operation on a nonexistent external catalog
  *
  * @param catalogName external catalog name
  * @param cause the cause
  */
case class ExternalCatalogNotExistException(
    catalogName: String,
    cause: Throwable)
    extends RuntimeException(s"External catalog $catalogName does not exist.", cause) {

  def this(catalogName: String) = this(catalogName, null)
}

/**
  * Exception for adding an already existent external catalog
  *
  * @param catalogName external catalog name
  * @param cause the cause
  */
case class ExternalCatalogAlreadyExistException(
    catalogName: String,
    cause: Throwable)
    extends RuntimeException(s"External catalog $catalogName already exists.", cause) {

  def this(catalogName: String) = this(catalogName, null)
}
