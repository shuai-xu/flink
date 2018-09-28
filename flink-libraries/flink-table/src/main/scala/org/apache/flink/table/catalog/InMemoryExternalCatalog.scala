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

package org.apache.flink.table.catalog

import java.util.{List => JList}

import org.apache.commons.collections.CollectionUtils
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ExternalCatalogTypes.PartitionSpec
import org.apache.flink.table.plan.stats.TableStats
import org.slf4j.LoggerFactory

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * This class is an in-memory implementation of [[ExternalCatalog]].
  *
  * @param name      The name of the catalog
  *
  * It could be used for testing or developing instead of used in production environment.
  */
class InMemoryExternalCatalog(name: String) extends CrudExternalCatalog {
  private val LOG = LoggerFactory.getLogger(classOf[InMemoryExternalCatalog])

  private val databases = new mutable.HashMap[String, ExternalCatalog]
  private val tables = new mutable.HashMap[String, ExternalCatalogTable]
  private val functions = new mutable.HashMap[String, ExternalCatalogFunction]
  private val partitions =
    new mutable.HashMap[String, mutable.HashMap[PartitionSpec, ExternalCatalogTablePartition]]

  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionAlreadyExistException]
  override def createPartition(
    tableName: String,
    part: ExternalCatalogTablePartition,
    ignoreIfExists: Boolean): Unit = synchronized {
    val newPartSpec = part.partitionSpec
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    checkPartitionSpec(newPartSpec, table)
    if (partitions.contains(newPartSpec)) {
      if (!ignoreIfExists) {
        throw new PartitionAlreadyExistException(name, tableName, newPartSpec)
      }
    } else {
      partitions.put(newPartSpec, part)
    }
  }

  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionNotExistException]
  override def dropPartition(
    tableName: String,
    partSpec: PartitionSpec,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    checkPartitionSpec(partSpec, table)
    if (partitions.remove(partSpec).isEmpty && !ignoreIfNotExists) {
      throw new PartitionNotExistException(name, tableName, partSpec)
    }
  }

  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionNotExistException]
  override def alterPartition(
    tableName: String,
    part: ExternalCatalogTablePartition,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    val updatedPartSpec = part.partitionSpec
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    checkPartitionSpec(updatedPartSpec, table)
    if (partitions.contains(updatedPartSpec)) {
      partitions.put(updatedPartSpec, part)
    } else if (!ignoreIfNotExists) {
      throw new PartitionNotExistException(name, tableName, updatedPartSpec)
    }
  }

  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionNotExistException]
  override def getPartition(
    tableName: String,
    partSpec: PartitionSpec): ExternalCatalogTablePartition = synchronized {
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    partitions.get(partSpec) match {
      case Some(part) => part
      case None =>
        throw new PartitionNotExistException(name, tableName, partSpec)
    }
  }

  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  override def listPartitions(
    tableName: String): JList[PartitionSpec] = synchronized {
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    partitions.keys.toList.asJava
  }

  @throws[TableAlreadyExistException]
  override def createTable(
    tableName: String,
    table: ExternalCatalogTable,
    ignoreIfExists: Boolean): Unit = synchronized {
    tables.get(tableName) match {
      case Some(_) if !ignoreIfExists => throw new TableAlreadyExistException(name, tableName)
      case _ => tables.put(tableName, table)
    }
  }

  @throws[TableNotExistException]
  override def dropTable(tableName: String, ignoreIfNotExists: Boolean): Unit = synchronized {
    if (tables.remove(tableName).isEmpty && !ignoreIfNotExists) {
      throw new TableNotExistException(name, tableName)
    }
  }

  @throws[TableNotExistException]
  override def alterTable(
    tableName: String,
    table: ExternalCatalogTable,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (tables.contains(tableName)) {
      tables.put(tableName, table)
    } else if (!ignoreIfNotExists) {
      throw new TableNotExistException(name, tableName)
    }
  }

  @throws[TableNotExistException]
  def alterTableStats(
    tableName: String,
    stats: Option[TableStats],
    ignoreIfNotExists: Boolean): Unit = {
    if (tables.contains(tableName)) {
      val oldTable = tables(tableName)
      tables(tableName) = oldTable.copy(stats = stats.orNull)
    } else if (!ignoreIfNotExists) {
      throw new TableNotExistException(name, tableName)
    }
  }

  @throws[CatalogAlreadyExistException]
  override def createSubCatalog(
    catalogName: String,
    catalog: ExternalCatalog,
    ignoreIfExists: Boolean): Unit = synchronized {
    databases.get(catalogName) match {
      case Some(_) if !ignoreIfExists => throw CatalogAlreadyExistException(catalogName, null)
      case _ => databases.put(catalogName, catalog)
    }
  }

  @throws[CatalogNotExistException]
  override def dropSubCatalog(
    catalogName: String,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.remove(catalogName).isEmpty && !ignoreIfNotExists) {
      throw CatalogNotExistException(catalogName, null)
    }
  }

  override def alterSubCatalog(
    catalogName: String,
    catalog: ExternalCatalog,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.contains(catalogName)) {
      databases.put(catalogName, catalog)
    } else if (!ignoreIfNotExists) {
      throw new CatalogNotExistException(catalogName)
    }
  }

  override def getTable(tableName: String): ExternalCatalogTable = synchronized {
    tables.get(tableName) match {
      case Some(t) => t
      case _ => throw TableNotExistException(name, tableName, null)
    }
  }

  override def listTables(): JList[String] = synchronized {
    tables.keys.toList.asJava
  }

  @throws[CatalogNotExistException]
  override def getSubCatalog(catalogName: String): ExternalCatalog = synchronized {
    databases.get(catalogName) match {
      case Some(d) => d
      case _ => throw CatalogNotExistException(catalogName, null)
    }
  }

  override def listSubCatalogs(): JList[String] = synchronized {
    databases.keys.toList.asJava
  }

  private def getPartitions(tableName: String, table: ExternalCatalogTable)
  : mutable.HashMap[PartitionSpec, ExternalCatalogTablePartition] = {
    if (!table.isPartitioned) {
      throw new UnsupportedOperationException(
        s"cannot do any operation about partition on the non-partitioned table $tableName")
    }
    partitions.getOrElseUpdate(
      tableName, new mutable.HashMap[PartitionSpec, ExternalCatalogTablePartition])
  }

  private def checkPartitionSpec(partSpec: PartitionSpec, table: ExternalCatalogTable): Unit = {
    if (!CollectionUtils.isEqualCollection(partSpec.keySet, table.partitionColumnNames)) {
      throw new IllegalArgumentException("Input partition specification is invalid!")
    }
  }

  override def createFunction(
    functionName: String,
    className: String,
    ignoreIfExists: Boolean): Unit = synchronized {
    functions.get(functionName) match {
      case Some(_) => {
        if (!ignoreIfExists) {
          throw new FunctionAlreadyExistException(name, functionName)
        }
      }
      case _ => {
        functions.put(functionName, new ExternalCatalogFunction(
          name,
          functionName,
          className,
          "Flink",
          System.currentTimeMillis()))
      }
    }
  }

  override def dropFunction(
    functionName: String,
    ignoreIfNotExists: Boolean): Unit = synchronized {

    LOG.info("dropFunction, functionName={}, ignoreIfNotExists={}",
      functionName, ignoreIfNotExists)

    functions.get(functionName) match {
      case Some(_) => functions.remove(functionName)
      case _ if !ignoreIfNotExists => throw new FunctionNotExistException(name, functionName)
      case _ => // do nothing
    }
  }

  /**
    * Get the function from a external catalog.
    *
    * @param functionName The function name.
    * @return The external catalog function.
    */
  override def getFunction(functionName: String): ExternalCatalogFunction = {
    functions.get(functionName) match {
      case Some(t) => t
      case _ => throw new FunctionNotExistException(name, functionName)
    }
  }

  /**
    * List functions from a external catalog.
    *
    * @return The external catalog functions.
    */
  override def listFunctions(): JList[ExternalCatalogFunction] = synchronized {
    functions.values.toList.asJava
  }
}
