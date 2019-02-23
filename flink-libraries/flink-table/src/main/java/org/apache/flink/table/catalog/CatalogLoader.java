/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Dynamic loader for catalogs.
 */
public class CatalogLoader {

	private static final Logger LOG = LoggerFactory.getLogger(CatalogLoader.class);

	// ------------------------------------------------------------------------
	//  Catalog Factory Classes
	// ------------------------------------------------------------------------

	private static final String HIVE_CATALOG_FACTORY_CLASS_NAME = "org.apache.flink.table.catalog.hive.HiveCatalogFactory";

	private static final String GENERIC_HIVE_METASTORE_FACTORY_CLASS_NAME = "org.apache.flink.table.catalog.hive.GenericHiveMetastoreCatalogFactory";

	// ------------------------------------------------------------------------
	//  Loading the state backend from a configuration
	// ------------------------------------------------------------------------

	public static ReadableCatalog loadCatalogFromConfig(
		ClassLoader cl,
		String catalogType,
		Optional<String> catalogFactoryClass,
		String catalogName,
		Map<String, String> properties) throws DynamicCodeLoadingException {

		checkNotNull(cl, "class loader cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogType), "catalogType cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(properties, "properties cannot be null or empty");

		CatalogType type = CatalogType.valueOf(catalogType);

		switch (type) {
			case flink_in_memory:
				return new FlinkInMemoryCatalogFactory().createCatalog(catalogName, properties);
			case hive:
				return loadCatalog(cl, HIVE_CATALOG_FACTORY_CLASS_NAME, catalogName, properties);
			case generic_hive_metastore:
				return loadCatalog(cl, GENERIC_HIVE_METASTORE_FACTORY_CLASS_NAME, catalogName, properties);
			case custom:
				LOG.info("Loading cutom catalog with factory class %s with class loader", catalogType);

				return loadCatalog(cl, catalogFactoryClass.get(), catalogName, properties);
			default:
				// Never reach here
				throw new IllegalStateException("Should never reach here");
		}
	}

	private static ReadableCatalog loadCatalog(
		ClassLoader cl,
		String catalogFactoryClass,
		String catalogName,
		Map<String, String> properties) throws DynamicCodeLoadingException {

		CatalogFactory<?> factory;

		try {
			Class<? extends CatalogFactory> clazz = Class.forName(catalogFactoryClass, false, cl)
				.asSubclass(CatalogFactory.class);
			factory = clazz.newInstance();
		} catch (ClassNotFoundException e) {
			throw new DynamicCodeLoadingException(
				String.format("Cannot find configured catalog factory class: %s", catalogFactoryClass), e);
		} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
			throw new DynamicCodeLoadingException(
				String.format(
					"The class configured for catalog does not have a valid catalog factory (%s)", catalogFactoryClass), e);
		}

		return factory.createCatalog(catalogName, properties);
	}
}
