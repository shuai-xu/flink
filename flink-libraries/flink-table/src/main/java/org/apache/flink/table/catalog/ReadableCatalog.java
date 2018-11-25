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

import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableNotExistException;

import java.io.Closeable;
import java.util.List;

/**
 * This class is responsible for read database/table/views/UDFs from a registered catalog. It is
 * the connector between a catalog and Flink's Table API.
 */
public interface ReadableCatalog extends Closeable {

	/**
	 * Called when init a ReadableCatalog. Used for any required preparation in initialization.
	 */
	void open();

	/**
	 * Gets the names of all databases registered in this catalog.
	 *
	 * @return The list of the names of all registered databases.
	 */
	List<String> listDatabases();

	/**
	 * Gets a database from this catalog.
	 *
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
	 * @return The requested database.
	 */
	CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException;

	/**
	 * Gets the names of all tables registered in this database. An empty list is returned if non match.
	 *
	 * @return A list of the names of all registered tables in this database.
	 */
	List<ObjectPath> listTablesByDatabase(String dbName);

	/**
	 * Gets the names of all tables registered in all databases of the catalog. An empty list is returned if non match.
	 *
	 * @return A list of the names of all registered tables in all databases in the catalog.
	 */
	List<ObjectPath> listAllTables();

	/**
	 * Get a table located in this ObjectPath.
	 *
	 * @param tableName The name of the table.
	 * @throws TableNotExistException    thrown if the table does not exist in the catalog.
	 * @return The requested table.
	 */
	ExternalCatalogTable getTable(ObjectPath tableName) throws TableNotExistException;

}
