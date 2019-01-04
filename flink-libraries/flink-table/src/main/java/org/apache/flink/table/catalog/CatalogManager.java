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

import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CatalogManager manages all the registered ReadableCatalog instances in a table environment.
 * It also has a concept of default catalog, which will be selected when a catalog name isn’t given
 * in a meta-object reference.
 *
 * <p>CatalogManger also encapsulates Calcite’s schema framework such that no code outside CatalogManager
 * needs to interact with Calcite’s schema except the parser which needs all catalogs. (All catalogs will
 * be added to Calcite schema so that all external tables and tables can be resolved by Calcite during
 * query parsing and analysis.)
 */
public class CatalogManager {
	// Cannot use 'default' here because 'default' is a reserved keyword in Calcite query parser
	public static final String DEFAULT_CATALOG_NAME = "default_catalog";
	public static final String DEFAULT_DATABASE_NAME = "default_db";

	// The catalog to hold all registered and translated tables
	// We disable caching here to prevent side effects
	private CalciteSchema internalSchema = CalciteSchema.createRootSchema(true, false);
	private SchemaPlus rootSchema = internalSchema.plus();

	// A list of named catalogs.
	private Map<String, ReadableCatalog> catalogs;

	// The name of the default catalog and schema
	private String defaultCatalog;
	private String defaultDb;

	public CatalogManager() {
		catalogs = new HashMap<>();

		FlinkInMemoryCatalog inMemoryCatalog = new FlinkInMemoryCatalog(DEFAULT_CATALOG_NAME);
		inMemoryCatalog.createDatabase(DEFAULT_DATABASE_NAME, new CatalogDatabase(), false);
		catalogs.put(DEFAULT_CATALOG_NAME, inMemoryCatalog);
		defaultCatalog = DEFAULT_CATALOG_NAME;
		defaultDb = DEFAULT_DATABASE_NAME;

		// TODO: re-evaluate isStreaming
		CatalogCalciteSchema.registerCatalog(rootSchema, DEFAULT_CATALOG_NAME, inMemoryCatalog, true);
	}

	/**
	 * Currently in design doc but not used. May be used in the future when TableEnvironment is initialized with
	 * multiple catalogs
	 */
	public CatalogManager(Map<String, ReadableCatalog> catalogs, String defaultCatalog) {
		this.catalogs = checkNotNull(catalogs, "catalogs cannot be null");

		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultCatalog), "defaultCatalog cannot be null or empty");
		checkArgument(catalogs.keySet().contains(defaultCatalog), "defaultCatalog must be in catalogs");

		this.defaultCatalog = defaultCatalog;

		// TODO: re-evaluate isStreaming
		for (Map.Entry<String, ReadableCatalog> e : catalogs.entrySet()) {
			CatalogCalciteSchema.registerCatalog(rootSchema, e.getKey(), e.getValue(), true);
		}
	}

	public void registerCatalog(String catalogName, ReadableCatalog catalog, boolean isStreaming) throws CatalogAlreadyExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(catalog, "catalog cannot be null");

		if (catalogs.containsKey(catalogName)) {
			throw new CatalogAlreadyExistException(catalogName);
		}

		catalog.open();
		catalogs.put(catalogName, catalog);
		catalog.open();
		CatalogCalciteSchema.registerCatalog(rootSchema, catalogName, catalog, isStreaming);
	}

	public ReadableCatalog getCatalog(String catalogName) throws CatalogNotExistException {
		if (!catalogs.keySet().contains(catalogName)) {
			throw new CatalogNotExistException(catalogName);
		}

		return catalogs.get(catalogName);
	}

	public Set<String> getCatalogs() {
		return catalogs.keySet();
	}

	public void setDefaultCatalog(String catalogName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(catalogs.keySet().contains(catalogName),
			String.format("Cannot find registered catalog %s", catalogName));

		if (!defaultCatalog.equals(catalogName)) {
			defaultCatalog = catalogName;
			defaultDb = null;
		}
	}

	public ReadableCatalog getDefaultCatalog() {
		return catalogs.get(defaultCatalog);
	}

	public String getDefaultCatalogName() {
		return defaultCatalog;
	}

	public void setDefaultDatabase(String catalogName, String dbName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");
		checkArgument(catalogs.containsKey(catalogName),
			String.format("Cannot find registered catalog %s", catalogName));
		checkArgument(catalogs.get(catalogName).listDatabases().contains(dbName),
			String.format("Cannot find registered database %s", dbName));

		defaultCatalog = catalogName;
		defaultDb = dbName;
	}

	public String getDefaultDatabaseName() {
		return defaultDb;
	}

	public SchemaPlus getRootSchema() {
		return rootSchema;
	}

	/**
	 * Returns the full name of the given table name.
	 *
	 * @param paths Table paths whose format can be among "catalog.db.table", "db.table", or "table"
	 * @return An array of complete table path
	 */
	public String[] resolveTableName(String... paths) {
		checkNotNull(paths, "paths cannot be null");
		checkArgument(paths.length >= 1 && paths.length <= 3, "paths length has to be between 1 and 3");
		checkArgument(!Arrays.stream(paths).anyMatch(p -> StringUtils.isNullOrWhitespaceOnly(p)),
			"Paths contains null or while-space-only string");

		if (paths.length == 3) {
			return paths;
		}

		String catalogName;
		String dbName;
		String tableName;

		if (paths.length == 1) {
			catalogName = getDefaultCatalogName();
			dbName = getDefaultDatabaseName();
			tableName = paths[0];
		} else {
			catalogName = getDefaultCatalogName();
			dbName = paths[0];
			tableName = paths[1];
		}

		return new String[]{catalogName, dbName, tableName};
	}

	/**
	 * Returns the full name of the given table name.
	 *
	 * @param paths Table paths whose format can be among "catalog.db.table", "db.table", or "table"
	 * @return A string of complete table path
	 */
	public String resolveTableNameAsString(String[] paths) {
		return String.join(".", resolveTableName(paths));
	}

	/**
	 * Checks if a table is registered under the given name.
	 *
	 * @param tableName The table name to check.
	 * @return true, if a table is registered under the name, false otherwise.
	 */
	public boolean isRegistered(String tableName) {
		// TODO: need to consider if there's no default database
		return getCatalog(getDefaultCatalogName())
			.listTables(getDefaultDatabaseName())
			.stream()
			.map(op -> op.getObjectName())
			.anyMatch(o -> o.equals(tableName));
	}

	public List<List<String>> getCalciteReaderDefaultPaths(SchemaPlus defaultSchema) {
		List<List<String>> paths = new ArrayList<>();

		// Add both catalog and catalog.db, if there's a default db, as default schema paths
		paths.add(new ArrayList<>(CalciteSchema.from(defaultSchema).path(getDefaultCatalogName())));

		if (getDefaultDatabaseName() != null && defaultSchema.getSubSchema(getDefaultCatalogName()) != null) {
			paths.add(new ArrayList<>(
				CalciteSchema.from(defaultSchema.getSubSchema(getDefaultCatalogName())).path(getDefaultDatabaseName())));
		}

		return paths;
	}
}
