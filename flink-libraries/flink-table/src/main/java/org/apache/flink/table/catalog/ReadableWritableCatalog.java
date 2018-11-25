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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;

/**
 * Responsible for creating, dropping, altering, and renaming databases/tables/views/UDFs.
 */
public interface ReadableWritableCatalog extends ReadableCatalog {

	/**
	 * Adds a database to this catalog.
	 *
	 * @param dbName    The name of the database to add.
	 * @param database        The database to add.
	 * @param ignoreIfExists Flag to specify behavior if a database with the given name already
	 *                       exists: if set to false, it throws a SchemaAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws DatabaseAlreadyExistException
	 *                       thrown if the database does already exist in the catalog
	 *                       and ignoreIfExists is false
	 */
	void createDatabase(String dbName, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException;

	/**
	 * Deletes a database from this catalog.
	 *
	 * @param dbName        Name of the database to delete.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
	 */
	void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException;

	/**
	 * Modifies an existing database.
	 *
	 * @param dbName        Name of the database to modify.
	 * @param database           The new database to replace the existing database.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
	 */
	void alterDatabase(String dbName, CatalogDatabase database, boolean ignoreIfNotExists)
		throws DatabaseNotExistException;

	/**
	 * Renames an existing database.
	 *
	 * @param dbName        Name of the database to modify.
	 * @param newSchemaName     New name of the database.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
	 */
	void renameDatabase(String dbName, String newSchemaName, boolean ignoreIfNotExists)
		throws DatabaseNotExistException;

	/**
	 * Adds a table.
	 *
	 * @param tableName      The path of the table to add.
	 * @param table          The table to add.
	 * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
	 *                       if set to false, it throws a TableAlreadyExistException,
	 *                       if set to true, nothing happens.
	 * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
	 * @throws DatabaseNotExistException thrown if the database that this table belongs to doesn't exist
	 */
	void createTable(ObjectPath tableName, ExternalCatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException;

	/**
	 * Deletes table.
	 *
	 * @param tableName         Name of the table to delete.
	 * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException    thrown if the table does not exist
	 */
	void dropTable(ObjectPath tableName, boolean ignoreIfNotExists) throws TableNotExistException;

	/**
	 * Modifies an existing table.
	 *
	 * @param tableName         The name of the table to modify.
	 * @param table             The new table which replaces the existing table.
	 * @param ignoreIfNotExists Flag to specify behavior if the table does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException   thrown if the table does not exist
	 */
	void alterTable(ObjectPath tableName, ExternalCatalogTable table, boolean ignoreIfNotExists)
		throws TableNotExistException;

	/**
	 * Renames an existing table.
	 *
	 * @param tableName        Name of the table to modify.
	 * @param newTableName     New name of the table.
	 * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
	 *                          if set to false, throw an exception,
	 *                          if set to true, nothing happens.
	 * @throws TableNotExistException thrown if the table does not exist
	 * @throws DatabaseNotExistException thrown if the new database that this table belongs to doesn't exist
	 */
	void renameTable(ObjectPath tableName, ObjectPath newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, DatabaseNotExistException;
}
