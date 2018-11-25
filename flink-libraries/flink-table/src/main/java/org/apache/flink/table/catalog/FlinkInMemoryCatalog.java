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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An in-memory catalog.
 */
public class FlinkInMemoryCatalog implements ReadableWritableCatalog {

	private String catalogName;

	private final Map<String, CatalogDatabase> databases;
	private final Map<ObjectPath, ExternalCatalogTable> tables;

	public FlinkInMemoryCatalog(String name) {
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");

		this.catalogName = name;
		this.databases = new HashMap<>();
		this.tables = new HashMap<>();
	}

	@Override
	public void open() {

	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void createTable(ObjectPath tableName, ExternalCatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {

		if (tables.containsKey(tableName) && !ignoreIfExists) {
			throw new TableAlreadyExistException(catalogName, tableName.getFullName());
		}

		if (!databases.containsKey(tableName.getSchemaName())) {
			throw new DatabaseNotExistException(catalogName, tableName.getSchemaName());
		}

		tables.put(tableName, table);
	}

	@Override
	public void dropTable(ObjectPath tableName, boolean ignoreIfNotExists) throws TableNotExistException {
		if (tables.remove(tableName) == null && !ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		}
	}

	@Override
	public void alterTable(ObjectPath tableName, ExternalCatalogTable table, boolean ignoreIfNotExists) throws TableNotExistException {
		if (tables.containsKey(tableName)) {
			tables.put(tableName, table);
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		}
	}

	@Override
	public void renameTable(ObjectPath tableName, ObjectPath newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, DatabaseNotExistException {
		if (tables.containsKey(tableName)) {
			if (!databases.containsKey(newTableName.getSchemaName())) {
				throw new DatabaseNotExistException(catalogName, newTableName.getSchemaName());
			}

			tables.put(newTableName, tables.remove(tableName));
		} else if (!ignoreIfNotExists) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		}
	}

	@Override
	public List<ObjectPath> listTablesByDatabase(String dbName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");

		return tables.keySet().stream()
			.filter(k -> k.getSchemaName().equals(dbName))
			.collect(Collectors.toList());
	}

	@Override
	public List<ObjectPath> listAllTables() {
		return new ArrayList<>(tables.keySet());
	}

	@Override
	public ExternalCatalogTable getTable(ObjectPath tableName) throws TableNotExistException {

		ExternalCatalogTable table = tables.get(tableName);

		if (table == null) {
			throw new TableNotExistException(catalogName, tableName.getFullName());
		} else {
			return table;
		}
	}

	@Override
	public void createDatabase(String dbName, CatalogDatabase schema, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		if (databases.containsKey(dbName) && !ignoreIfExists) {
			throw new DatabaseAlreadyExistException(catalogName, dbName);
		} else {
			databases.put(dbName, schema);
		}
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (databases.remove(dbName) == null && !ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase schema, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (databases.containsKey(dbName)) {
			databases.put(dbName, schema);
		} else if (!ignoreIfNotExists) {
			throw new DatabaseNotExistException(catalogName, dbName);
		}
	}

	@Override
	public void renameDatabase(String dbName, String newSchemaName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		if (databases.containsKey(dbName)) {
			databases.put(newSchemaName, databases.remove(dbName));
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
		CatalogDatabase schema = databases.get(dbName);

		if (schema == null) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} else {
			return schema;
		}
	}
}
