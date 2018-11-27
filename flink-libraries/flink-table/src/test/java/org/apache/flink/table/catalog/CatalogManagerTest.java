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

import org.apache.flink.table.runtime.utils.CommonTestData;

import org.apache.calcite.schema.SchemaPlus;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for CatalogManager.
 */
public class CatalogManagerTest {
	private static final String TEST_CATALOG_NAME = "test";

	private CatalogManager manager;
	private SchemaPlus rootSchema;

	@Before
	public void init() {
		manager = new CatalogManager();
		rootSchema = manager.getRootSchema();
	}

	@Test
	public void testRegisterCatalog() {
		assertEquals(1, manager.getCatalogs().size());
		assertFalse(manager.getCatalogs().contains(TEST_CATALOG_NAME));
		assertFalse(rootSchema.getSubSchemaNames().contains(TEST_CATALOG_NAME));

		manager.registerCatalog(TEST_CATALOG_NAME, CommonTestData.getTestFlinkInMemoryCatalog(), true);

		assertEquals(2, manager.getCatalogs().size());
		assertTrue(manager.getCatalogs().contains(TEST_CATALOG_NAME));
		assertTrue(rootSchema.getSubSchemaNames().contains(TEST_CATALOG_NAME));
	}

	@Test
	public void testSetDefaultCatalog() {
		manager.registerCatalog(TEST_CATALOG_NAME, CommonTestData.getTestFlinkInMemoryCatalog(), true);

		assertEquals(manager.getCatalog(CatalogManager.DEFAULT_CATALOG_NAME), manager.getDefaultCatalog());

		manager.setDefaultCatalog(TEST_CATALOG_NAME);

		assertEquals(manager.getCatalog(TEST_CATALOG_NAME), manager.getDefaultCatalog());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetNonExistDefaultCatalog() {
		manager.setDefaultCatalog("nonexist");
	}

	@Test
	public void testSetDefaultDatabase() {
		// TODO: implement this
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetNonExistDefaultDatabase() {
		manager.setDefaultDatabase(CatalogManager.DEFAULT_CATALOG_NAME, "nonexist");
	}
}
