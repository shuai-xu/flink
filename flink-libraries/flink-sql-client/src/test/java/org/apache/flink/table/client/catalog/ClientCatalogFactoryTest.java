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

import org.apache.flink.table.catalog.FlinkInMemoryCatalog;
import org.apache.flink.table.catalog.ReadableCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogConfig;
import org.apache.flink.table.client.config.Catalog;
import org.apache.flink.table.client.config.CatalogType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for ClientCatalogFactory.
 */
public class ClientCatalogFactoryTest {
	@Test
	public void testCleanProperties() {
		Map<String, String> map = new HashMap<>();

		map.put(CatalogConfigs.CATALOG_CONNECTOR_HIVE_METASTORE_URIS, "");

		assertEquals(
			new HashMap<String, String>() {{
				put(HiveCatalogConfig.HIVE_METASTORE_URIS, "");
			}},
			ClientCatalogFactory.cleanProperties(map)
		);
	}

	@Test
	public void testCreateCatalog() {
		ReadableCatalog catalog = ClientCatalogFactory.createCatalog(
			new Catalog("test", getProperties(CatalogType.flink_in_memory.name())));

		assertTrue(catalog instanceof FlinkInMemoryCatalog);
	}

	@Test (expected = IllegalArgumentException.class)
	public void testFailCreateCatalog() {
		ClientCatalogFactory.createCatalog(
			new Catalog("test", getProperties("nonexist")));
	}

	private Map<String, String> getProperties(String type) {
		return new HashMap<String, String>() {{
			put(CatalogConfigs.CATALOG_TYPE, type);
		}};
	}
}
