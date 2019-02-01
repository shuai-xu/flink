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
import org.apache.flink.table.api.FunctionAlreadyExistException;
import org.apache.flink.table.api.FunctionNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An in-memory catalog.
 */
public class FlinkInMemoryCatalog implements ReadableWritableCatalog {

	public static final String DEFAULT_DB = "default";

	private String defaultDatabaseName = DEFAULT_DB;

	private final String catalogName;
	private final Map<String, CatalogDatabase> databases;
	private final Map<ObjectPath, CatalogTable> tables;
	private final Map<ObjectPath, Map<CatalogPartition.PartitionSpec, CatalogPartition>> partitions;
	private final Map<ObjectPath, CatalogFunction> functions;

	public FlinkInMemoryCatalog(String name) {
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");

		this.catalogName = name;
		this.databases = new LinkedHashMap<>();
		this.databases.put(DEFAULT_DB, new CatalogDatabase());
		this.tables = new LinkedHashMap<>();
		this.partitions = new LinkedHashMap<>();
		this.functions = new LinkedHashMap<>();
	}

	@Override
	public String getDefaultDatabaseName() {
		return defaultDatabaseName;
	}

	@Override
	public void setDefaultDatabaseName(String databaseName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		defaultDatabaseName = databaseName;
	}

	@Override
	public void open() {

	}

	@Override
	public void close() throws IOException {

	}

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfExists)
		throws FunctionAlreadyExistException, DatabaseNotExistException {

		if (!dbExists(path.getDbName())) {
			throw new DatabaseNotExistException(catalogName, path.getDbName());
		}

		if (functionExists(path)) {
			if (!ignoreIfExists) {
				throw new FunctionAlreadyExistException(catalogName, path.getFullName());
			}
		} else {
			functions.put(path, function.deepCopy());
		}
	}

	@Override
	public void dropFunction(ObjectPath path, boolean ignoreIfNotExists) throws FunctionNotExistException {
		if (functionExists(path)) {
			functions.remove(path);
		} else if (!ignoreIfNotExists) {
			throw new FunctionNotExistException(catalogName, path.getFullName());
		}
	}

	@Override
	public CatalogFunction getFunction(ObjectPath path) throws FunctionNotExistException {
		if (!functionExists(path)) {
			throw new FunctionNotExistException(catalogName, path.getFullName());
		} else {
			return functions.get(path).deepCopy();
		}
	}

	@Override
	public boolean functionExists(ObjectPath path) {
		return dbExists(path.getDbName()) && functions.containsKey(path);
	}

	// ------ tables ------

	@Override
	public void createTable(ObjectPath tableName, CatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {

		if (!dbExists(tableName.getDbName())) {
			throw new DatabaseNotExistException(catalogName, tableName.getDbName());
		}

		if (tableExists(tableName)) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, tableName.getFullName());
			}
		} else {
			tables.put(tableName, deepCopyCatalogTable(table));

			if (table.isPartitioned()) {
				partitions.put(tableName, new LinkedHashMap<>());
			}
		}
	}

	@Override
	public void alterTable(ObjectPath tableName, CatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		if (tableExists(tableName)) {
			tables.put(tableName, newTable);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		}
	}

	@Override
	public void renameTable(ObjectPath tableName, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException {

		if (tableExists(tableName)) {
			ObjectPath newPath = new ObjectPath(tableName.getDbName(), newTableName);

			if (tableExists(newPath)) {
				throw new TableAlreadyExistException(catalogName, newPath.getFullName());
			} else {
				tables.put(newPath, tables.remove(tableName));
			}
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		}
	}

	// ------ tables and views ------

	@Override
	public void dropTable(ObjectPath path, boolean ignoreIfNotExists) throws TableNotExistException {
		if (tableExists(path)) {
			tables.remove(path);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, path.getFullName());
		}
	}

	@Override
	public List<ObjectPath> listTables(String dbName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");

		if (!dbExists(dbName)) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}

		return tables.keySet().stream()
			.filter(k -> k.getDbName().equals(dbName))
			.collect(Collectors.toList());
	}

	@Override
	public List<ObjectPath> listAllTables() {
		return new ArrayList<>(tables.keySet());
	}

	@Override
	public CatalogTable getTable(ObjectPath tableName) throws TableNotExistException {

		if (!tableExists(tableName)) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		} else {
			CatalogTable table = tables.get(tableName);

			// TODO: Should return a deep copy since operations outside the catalog should not impact table stored internally
			// [BLINK-18866721] read/write APIs in FlinkInMemoryCatalog for meta objects should use a deep copy of those objects
			// This is only a shallow copy
			return deepCopyCatalogTable(table);
		}
	}

	private CatalogTable deepCopyCatalogTable(CatalogTable table) {
		if (table instanceof FlinkTempTable) {
			return table;
		} else if (table instanceof CatalogView) {
			return new CatalogView(
				table.getTableType(),
				table.getTableSchema(),
				new HashMap<>(table.getProperties()),
				table.getRichTableSchema(),
				table.getTableStats(),
				table.getComment(),
				table.getPartitionColumnNames(),
				table.isPartitioned(),
				table.getComputedColumns(),
				table.getRowTimeField(),
				table.getWatermarkOffset(),
				table.getCreateTime(),
				table.getLastAccessTime(),
				((CatalogView) table).getOriginalQuery(),
				((CatalogView) table).getExpandedQuery()
			);
		} else {
			return new CatalogTable(
				table.getTableType(),
				table.getTableSchema(),
				new HashMap<>(table.getProperties()),
				table.getRichTableSchema(),
				table.getTableStats(),
				table.getComment(),
				table.getPartitionColumnNames(),
				table.isPartitioned(),
				table.getComputedColumns(),
				table.getRowTimeField(),
				table.getWatermarkOffset(),
				table.getCreateTime(),
				table.getLastAccessTime()
			);
		}
	}

	@Override
	public boolean tableExists(ObjectPath path) {
		return dbExists(path.getDbName()) && tables.containsKey(path);
	}

	// ------ views ------

	@Override
	public List<ObjectPath> listViews(String dbName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");

		if (!dbExists(dbName)) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}

		return tables.keySet().stream()
			.filter(k -> k.getDbName().equals(dbName))
			.filter(k -> (tables.get(k) instanceof CatalogView))
			.collect(Collectors.toList());
	}

	@Override
	public void createView(ObjectPath viewPath, CatalogView view, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException {
		createTable(viewPath, view, ignoreIfExists);
	}

	@Override
	public void alterView(ObjectPath viewPath, CatalogView newView, boolean ignoreIfNotExists) throws TableNotExistException {
		alterTable(viewPath, newView, ignoreIfNotExists);
	}

	// ------ table and column stats ------

	@Override
	public TableStats getTableStats(ObjectPath path) throws TableNotExistException {
		if (!tableExists(path)) {
			throw new TableNotExistException(catalogName, path.getFullName());
		} else {
			if (!isTablePartitioned(path)) {
				return tables.get(path).getTableStats();
			} else {
				// TableStats of partitioned table is unknown, the behavior is same as HIVE
				return TableStats.UNKNOWN();
			}
		}
	}

	@Override
	public void alterTableStats(ObjectPath tablePath, TableStats newtTableStats, boolean ignoreIfNotExists) throws TableNotExistException {
		if (tableExists(tablePath)) {
			if (!isTablePartitioned(tablePath)) {
				CatalogTable oldTable = tables.get(tablePath);

				tables.put(tablePath, new CatalogTable(
					oldTable.getTableType(),
					oldTable.getTableSchema(),
					oldTable.getProperties(),
					oldTable.getRichTableSchema(),
					newtTableStats,
					oldTable.getComment(),
					oldTable.getPartitionColumnNames(),
					oldTable.isPartitioned(),
					oldTable.getComputedColumns(),
					oldTable.getRowTimeField(),
					oldTable.getWatermarkOffset(),
					oldTable.getCreateTime(),
					oldTable.getLastAccessTime()
				));
			}
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tablePath.getFullName());
		}
	}

	// ------ databases ------

	@Override
	public void createDatabase(String dbName, CatalogDatabase db, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		if (dbExists(dbName)) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, dbName);
			}
		} else {
			databases.put(dbName, db);
		}
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (databases.containsKey(dbName)) {
			databases.remove(dbName);
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (dbExists(dbName)) {
			databases.put(dbName, newDatabase);
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}
	}

	@Override
	public List<String> listDatabases() {
		return new ArrayList<>(databases.keySet());
	}

	@Override
	public CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException {
		if (!dbExists(dbName)) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} else {
			// TODO: Should return a deep copy since operations outside the catalog should not impact db stored internally
			// This is only a shallow copy
			CatalogDatabase db = databases.get(dbName);
			return new CatalogDatabase(db.getProperties());
		}
	}

	@Override
	public boolean dbExists(String dbName) {
		return databases.containsKey(dbName);
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath path, CatalogPartition partition, boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException {

		if (!tableExists(path)) {
			throw new TableNotExistException(catalogName, path.getFullName());
		}

		if (!isTablePartitioned(path)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		if (partitionExists(path, partition.getPartitionSpec())) {
			if (!ignoreIfExists) {
				throw new PartitionAlreadyExistException(catalogName, path, partition.getPartitionSpec());
			}
		} else {
			partitions.get(path).put(partition.getPartitionSpec(), partition);
		}
	}

	@Override
	public void dropPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {

		if (!tableExists(path)) {
			throw new TableNotExistException(catalogName, path.getFullName());
		}

		if (!isTablePartitioned(path)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		if (partitionExists(path, partitionSpec)) {
			partitions.get(path).remove(partitionSpec);
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, path, partitionSpec);
		}
	}

	@Override
	public void alterPartition(ObjectPath path, CatalogPartition newPartition, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		if (!tableExists(path)) {
			throw new TableNotExistException(catalogName, path.getFullName());
		}

		if (!isTablePartitioned(path)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		CatalogPartition.PartitionSpec partitionSpec = newPartition.getPartitionSpec();

		if (partitionExists(path, partitionSpec)) {
			partitions.get(path).put(partitionSpec, newPartition);
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, path, partitionSpec);
		}
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path)
		throws TableNotExistException, TableNotPartitionedException {
		return new ArrayList<>(partitions.get(path).keySet());
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path, CatalogPartition.PartitionSpec partitionSpecs)
		throws TableNotExistException, TableNotPartitionedException {
		return partitions.get(path).keySet().stream()
			.filter(ps -> ps.contains(partitionSpecs))
			.collect(Collectors.toList());
	}

	@Override
	public CatalogPartition getPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {

		CatalogTable table = getTable(path);

		if (!table.isPartitioned()) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		if (partitions.get(new ObjectPath(path.getDbName(), path.getObjectName())).get(partitionSpec) != null) {
			return partitions.get(path).get(partitionSpec);
		} else {
			throw new PartitionNotExistException(catalogName, path, partitionSpec);
		}
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpec) {
		return tableExists(tablePath) && partitions.get(tablePath).containsKey(partitionSpec);
	}

	private boolean isTablePartitioned(ObjectPath tablePath) throws TableNotExistException {
		return getTable(tablePath).isPartitioned();
	}
}
