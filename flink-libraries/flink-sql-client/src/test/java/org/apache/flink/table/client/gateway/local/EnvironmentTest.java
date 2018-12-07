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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.table.client.catalog.CatalogConfigs;
import org.apache.flink.table.client.config.Catalog;
import org.apache.flink.table.client.config.CatalogType;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.utils.EnvironmentUtil;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for Environment.
 */
public class EnvironmentTest {

	@Test
	public void testParsingCatalog() throws IOException {
		Environment env = EnvironmentUtil.getDefaultTestEnvironment();

		assertEquals(new HashSet<>(Arrays.asList("myhive", "myinmemory")), env.getCatalogs().keySet());

		Catalog hive = env.getCatalogs().get("myhive");
		assertEquals(
			new HashMap<String, String>() {{
				put(CatalogConfigs.CATALOG_CONNECTOR_HIVE_METASTORE_URIS, "thrift://host1:10000,thrift://host2:10000");
				put(CatalogConfigs.CATALOG_CONNECTOR_HIVE_METASTORE_USERNAME, "flink");
				put(CatalogConfigs.CATALOG_TYPE, CatalogType.hive.name());
				put(CatalogConfigs.CATALOG_IS_DEFAULT, "true");
				put(CatalogConfigs.CATALOG_DEFAULT_DB, "mydb");
			}},
			hive.getProperties());

		assertTrue(hive.isDefaultCatalog());
		assertEquals("mydb", hive.getDefaultDatabase().get());

		assertEquals(
			new HashMap<String, String>() {{
				put(CatalogConfigs.CATALOG_TYPE, CatalogType.flink_in_memory.name());
			}},
			env.getCatalogs().get("myinmemory").getProperties());
	}
}
