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

package org.apache.flink.table.client.catalog;

import org.apache.flink.table.catalog.hive.HiveCatalogConfig;

/**
 * Configs for Catalog.
 */
public class CatalogConfigs {
	public static final String CATALOG_CONNECTOR_PREFIX = "catalog.connector";

	public static final String CATALOG_TYPE = "catalog.type";
	public static final String CATALOG_IS_DEFAULT = "catalog.is-default";
	public static final String CATALOG_DEFAULT_DB = "catalog.default-db";

	// Hive-specific
	public static final String CATALOG_CONNECTOR_HIVE_METASTORE_URIS =
		CATALOG_CONNECTOR_PREFIX + "." + HiveCatalogConfig.HIVE_METASTORE_URIS;
	public static final String CATALOG_CONNECTOR_HIVE_METASTORE_USERNAME =
		CATALOG_CONNECTOR_PREFIX + "." + HiveCatalogConfig.HIVE_METASTORE_USERNAME;
}
