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
import org.apache.flink.table.api.FunctionNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.plan.stats.TableStats;

import java.io.Closeable;
import java.util.List;

/**
 * This interface is responsible for reading database/table/views/UDFs from a registered catalog.
 * It connects a registered catalog and Flink's Table API.
 */
public interface ReadableCatalog extends Closeable {

	/**
	 * Gets the default database of this type of catalog. This is used when users only set a default catalog
	 * without a default db. For example, the default db in Hive metastore is always 'default'.
	 *
	 * @return Name of the default database.
	 */
	String getDefaultDatabaseName();

	/**
	 * Set the default database name to be used when users only set a default catalog without a default db.
	 *
	 * @param databaseName	The database name to be set.
	 */
	void setDefaultDatabaseName(String databaseName);

	/**
	 * Called when init a ReadableCatalog. Used for any required preparation in initialization phase.
	 */
	void open();

	// ------ databases ------
	/**
	 * Gets the names of all databases registered in this catalog.
	 *
	 * @return The list of the names of all registered databases.
	 */
	List<String> listDatabases();

	/**
	 * Gets a database from this catalog.
	 *
	 * @param dbName	Name of the database.
	 * @return The requested database.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
	 */
	CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException;

	/**
	 * Check whether a database exists in this catalog.
	 *
	 * @param dbName		Name of the database.
	 */
	boolean dbExists(String dbName);

	// ------ tables and views ------

	/**
	 * Gets paths of all tables and views registered in all databases of the catalog. An empty list is returned if none is registered.
	 *
	 * @return A list of the names of all registered tables and views in all databases in the catalog.
	 */
	List<ObjectPath> listAllTables();

	/**
	 * Gets paths of all tables and views registered in this database. An empty list is returned if none is registered.
	 *
	 * @return A list of the names of all registered tables and views in this database.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
	 */
	List<ObjectPath> listTables(String dbName) throws DatabaseNotExistException;

	/**
	 * Gets a CatalogTable or CatalogView registered in this ObjectPath.
	 *
	 * @param objectName		Path of the table or view.
	 * @throws TableNotExistException    thrown if the target does not exist in the catalog.
	 * @return The requested table.
	 */
	CatalogTable getTable(ObjectPath objectName) throws TableNotExistException;

	/**
	 * Checks whether a table or view exists in this catalog.
	 *
	 * @param path			Path of the table or view.
	 */
	boolean tableExists(ObjectPath path);

	// ------ views ------

	/**
	 * Gets paths of all views registered in this database. An empty list is returned if none is registered.
	 *
	 * @return A list of the names of all registered  views in this database.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
	 */
	List<ObjectPath> listViews(String dbName) throws DatabaseNotExistException;


	// ------ table and column stats ------

	/**
	 * Gets TableStats of a table. This only works for non-partitioned tables.
	 *
	 * @param tablePath		Path of the table.
	 * @return TableStats of the requested table.
	 * @throws TableNotExistException    thrown if the table does not exist in the catalog.
	 */
	TableStats getTableStats(ObjectPath tablePath) throws TableNotExistException;

	// ------ partitions ------

	/**
	 * Gets PartitionSpec of all partitions of the table.
	 *
	 * @param tablePath		Path of the table.
	 * @return	A list of PartitionSpec of the table.
	 * @throws TableNotExistException	thrown if the table does not exist in the catalog.
	 * @throws TableNotPartitionedException		thrown if the table is not partitioned.
	 */
	List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath tablePath)
		throws TableNotExistException, TableNotPartitionedException;

	/**
	 * Gets PartitionSpec of all partitions that is under the given PartitionSpec in the table .
	 *
	 * @param tablePath			Path of the table
	 * @param partitionSpecs	The partition spec to list
	 * @return A list of PartitionSpec that is under the given ParitionSpec in the table.
	 * @throws TableNotExistException	thrown if the table does not exist in the catalog.
	 * @throws TableNotPartitionedException		thrown if the table is not partitioned.
	 */
	List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpecs)
		throws TableNotExistException, TableNotPartitionedException;

	/**
	 * Gets a partition of the given table.
	 *
	 * @param tablePath		Path of the table
	 * @param partitionSpecs	Partition spec of partition to get
	 * @return The requested partition.
	 * @throws TableNotExistException	thrown if the table does not exist in the catalog.
	 * @throws TableNotPartitionedException		thrown if the table is not partitioned.
	 * @throws PartitionNotExistException		thrown if the partition is not partitioned.
	 */
	CatalogPartition getPartition(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpecs)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException;

	/**
	 * Checks whether a partition exists or not.
	 *
	 * @param tablePath		Path of the table
	 * @param partitionSpec	Partition spec of the partition to check
	 */
	boolean partitionExists(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpec);

	// ------ functions ------

	/**
	 * Lists functions in the given database. An empty list is returned if none is registered.
	 *
	 * @param dbName	Name of the database.
	 * @return	A list of the names of all registered functions in this database.
	 * @throws DatabaseNotExistException
	 */
	List<ObjectPath> listFunctions(String dbName) throws DatabaseNotExistException;

	/**
	 * Gets the function.
	 *
	 * @param functionPath	Path of the function.
	 * @return	The requested function.
	 * @throws FunctionNotExistException thrown if the partition does not exist in the catalog.
	 */
	CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException;

	/**
	 * Checks whether a function exists or not.
	 *
	 * @param functionPath	Path of the function
	 */
	boolean functionExists(ObjectPath functionPath);
}
