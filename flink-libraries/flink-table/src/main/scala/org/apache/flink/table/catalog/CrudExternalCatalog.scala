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

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ExternalCatalogTypes.PartitionSpec
import org.apache.flink.table.plan.stats.TableStats

/**
  * The CrudExternalCatalog provides methods to create, drop, and alter (sub-)catalogs or tables.
  */
trait CrudExternalCatalog extends ExternalCatalog {

  /**
    * Adds partition into an external Catalog table
    *
    * @param tableName      table name
    * @param part           partition description of partition which to create
    * @param ignoreIfExists if partition already exists in the catalog, not throw exception and
    *                       leave the existed partition if ignoreIfExists is true;
    *                       else throw PartitionAlreadyExistException
    * @throws CatalogNotExistException       if database does not exist in the catalog yet
    * @throws TableNotExistException         if table does not exist in the catalog yet
    * @throws PartitionAlreadyExistException if partition exists in the catalog and
    *                                        ignoreIfExists is false
    */
  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionAlreadyExistException]
  def createPartition(
      tableName: String,
      part: ExternalCatalogTablePartition,
      ignoreIfExists: Boolean): Unit

  /**
    * Deletes partition of an external Catalog table
    *
    * @param tableName         table name
    * @param partSpec          partition specification
    * @param ignoreIfNotExists if partition not exist yet, not throw exception if ignoreIfNotExists
    *                          is true; else throw PartitionNotExistException
    * @throws CatalogNotExistException   if database does not exist in the catalog yet
    * @throws TableNotExistException     if table does not exist in the catalog yet
    * @throws PartitionNotExistException if partition does not exist in the catalog yet
    */
  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionNotExistException]
  def dropPartition(
      tableName: String,
      partSpec: PartitionSpec,
      ignoreIfNotExists: Boolean): Unit

  /**
    * Alters an existed external Catalog table partition
    *
    * @param tableName         table name
    * @param part              description of partition which to alter
    * @param ignoreIfNotExists if the partition not exist yet, not throw exception if
    *                          ignoreIfNotExists is true; else throw PartitionNotExistException
    * @throws CatalogNotExistException  if database does not exist in the catalog yet
    * @throws TableNotExistException     if table does not exist in the catalog yet
    * @throws PartitionNotExistException if partition does not exist in the catalog yet
    */
  @throws[CatalogNotExistException]
  @throws[TableNotExistException]
  @throws[PartitionNotExistException]
  def alterPartition(
      tableName: String,
      part: ExternalCatalogTablePartition,
      ignoreIfNotExists: Boolean): Unit

  /**
    * Adds a table to this catalog.
    *
    * @param tableName      The name of the table to add.
    * @param table          The table to add.
    * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
    *                       if set to false, it throws a TableAlreadyExistException,
    *                       if set to true, nothing happens.
    * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
    */
  @throws[TableAlreadyExistException]
  def createTable(tableName: String, table: ExternalCatalogTable, ignoreIfExists: Boolean): Unit

  /**
    * Deletes table from this catalog.
    *
    * @param tableName         Name of the table to delete.
    * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws TableNotExistException    thrown if the table does not exist in the catalog
    */
  @throws[TableNotExistException]
  def dropTable(tableName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing table of this catalog.
    *
    * @param tableName         The name of the table to modify.
    * @param table             The new table which replaces the existing table.
    * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws TableNotExistException   thrown if the table does not exist in the catalog
    */
  @throws[TableNotExistException]
  def alterTable(tableName: String, table: ExternalCatalogTable, ignoreIfNotExists: Boolean): Unit

  /**
    * Alter the statistics of a table. If `stats` is None, then remove existing TableStats.
    *
    * @param tableName         The name of the table to modify.
    * @param stats             The new stats.
    * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws TableNotExistException   thrown if the table does not exist in the catalog
    */
  @throws[TableNotExistException]
  def alterTableStats(
    tableName: String,
    stats: Option[TableStats],
    ignoreIfNotExists: Boolean): Unit

  /**
    * Adds a subcatalog to this catalog.
    *
    * @param name           The name of the sub catalog to add.
    * @param catalog        Description of the catalog to add.
    * @param ignoreIfExists Flag to specify behavior if a sub catalog with the given name already
    *                       exists: if set to false, it throws a CatalogAlreadyExistException,
    *                       if set to true, nothing happens.
    * @throws CatalogAlreadyExistException
    *                       thrown if the sub catalog does already exist in the catalog
    *                       and ignoreIfExists is false
    */
  @throws[CatalogAlreadyExistException]
  def createSubCatalog(name: String, catalog: ExternalCatalog, ignoreIfExists: Boolean): Unit

  /**
    * Deletes a sub catalog from this catalog.
    *
    * @param name              Name of the sub catalog to delete.
    * @param ignoreIfNotExists Flag to specify behavior if the catalog does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws CatalogNotExistException thrown if the sub catalog does not exist in the catalog
    */
  @throws[CatalogNotExistException]
  def dropSubCatalog(name: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing sub catalog of this catalog.
    *
    * @param name              Name of the catalog to modify.
    * @param catalog           The new sub catalog to replace the existing sub catalog.
    * @param ignoreIfNotExists Flag to specify behavior if the sub catalog does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws CatalogNotExistException thrown if the sub catalog does not exist in the catalog
    */
  @throws[CatalogNotExistException]
  def alterSubCatalog(name: String, catalog: ExternalCatalog, ignoreIfNotExists: Boolean): Unit

}
