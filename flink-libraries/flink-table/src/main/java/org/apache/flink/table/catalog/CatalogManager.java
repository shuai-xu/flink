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

import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CatalogManager manages all the registered ReadableCatalog instances in a table environment.
 * It also has a concept of default catalog, which will be selected when a catalog name isn’t given
 * in a meta-object reference.
 *
 * <p>CatalogManger also encapsulate Calcite’s schema framework such that no code outside CatalogManager
 * needs to interact with Calcite’s schema except the parser which needs all catalogs. (All catalogs will
 * be added to Calcite schema so that all external tables and tables can be resolved by Calcite during
 * query parsing and analysis.)
 */
public class CatalogManager {
	// The catalog to hold all registered and translated tables
	// We disable caching here to prevent side effects
	private CalciteSchema internalSchema = CalciteSchema.createRootSchema(true, false);
	private SchemaPlus rootSchema = internalSchema.plus();

	// A list of named catalogs.
	private Map<String, ReadableCatalog> catalogs;

	// The name of the default catalog and schema
	private String defaultCatalog;
	private String defaultDb;

	public CatalogManager(Map<String, ReadableCatalog> catalogs, String defaultCatalog) {
		this.catalogs = checkNotNull(catalogs, "catalogs cannot be null");

		checkArgument(StringUtils.isNullOrWhitespaceOnly(defaultCatalog), "defaultCatalog cannot be null or empty");
		checkArgument(catalogs.keySet().contains(defaultCatalog), "defaultCatalog must be in catalogs");

		this.defaultCatalog = defaultCatalog;
	}

	public void registerCatalog(String catalogName, ReadableCatalog catalog, boolean isStreaming) {
		checkArgument(StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(catalog, "catalog cannot be null");

		if (catalogs.containsKey(catalogName)) {
			throw new CatalogAlreadyExistException(catalogName);
		}

		catalogs.put(catalogName, catalog);
//		ExternalCatalogSchema.registerCatalog(rootSchema, catalogName, catalog, isStreaming);
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
		checkArgument(StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(catalogs.keySet().contains(catalogName),
			String.format("Cannot find registered catalog %s", catalogName));

		defaultCatalog = catalogName;
	}

	public ReadableCatalog getDefaultCatalog() {
		return catalogs.get(defaultCatalog);
	}

	public void setDefaultSchema(String catalogName, String dbName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(dbName), "dbName cannot be null or empty");
		checkArgument(catalogs.containsKey(catalogName),
			String.format("Cannot find registered catalog %s", catalogName));
		checkArgument(catalogs.get(catalogName).listDatabases().contains(dbName),
			String.format("Cannot find registered database %s", dbName));

		defaultCatalog = catalogName;
		defaultDb = dbName;
	}

	public SchemaPlus getRootSchema() {
		return rootSchema;
	}
}
