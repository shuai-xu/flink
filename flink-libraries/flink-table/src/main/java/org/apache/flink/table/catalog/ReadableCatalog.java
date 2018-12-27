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
import org.apache.flink.table.api.PartitionNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;

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
	 * @return The requested database.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
	 */
	CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException;

	/**
	 * Check if a database exists in this catalog.
	 *
	 * @param dbName		Name of the database.
	 */
	boolean dbExists(String dbName);

	// ------ tables ------

	/**
	 * Gets the names of all tables registered in all databases of the catalog. An empty list is returned if non match.
	 *
	 * @return A list of the names of all registered tables in all databases in the catalog.
	 */
	List<ObjectPath> listAllTables();

	/**
	 * Gets the names of all tables registered in this database. An empty list is returned if non match.
	 *
	 * @return A list of the names of all registered tables in this database.
	 * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
	 */
	List<ObjectPath> listTables(String dbName) throws DatabaseNotExistException;

	/**
	 * Gets a table located in this ObjectPath.
	 *
	 * @param tableName The name of the table.
	 * @throws TableNotExistException    thrown if the table does not exist in the catalog.
	 * @return The requested table.
	 */
	ExternalCatalogTable getTable(ObjectPath tableName) throws TableNotExistException;

	/**
	 * Check if a table exists in this catalog.
	 *
	 * @param path			Path of the table.
	 */
	boolean tableExists(ObjectPath path);

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
	 * Checks whether the table is partitioned.
	 *
	 * @param tablePath		Path of the table
	 * @return boolean of whether the table is partitioned.
	 * @throws TableNotExistException  thrown if the table does not exist in the catalog.
	 */
	default boolean isTablePartitioned(ObjectPath tablePath) throws TableNotExistException {
		return getTable(tablePath).isPartitioned();
	}
}
