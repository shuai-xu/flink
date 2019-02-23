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

import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for CatalogLoader.
 */
public class CatalogLoaderTest {

	private final ClassLoader cl = getClass().getClassLoader();

	@Test(expected = IllegalArgumentException.class)
	public void testNoCatalogDefined() throws Exception {
		assertNull(CatalogLoader.loadCatalogFromConfig(cl, "fake", Optional.empty(), "test", new HashMap<>()));
	}

	@Test
	public void testFlinkInMemoryCatalog() throws Exception {
		ReadableCatalog catalog =
			CatalogLoader.loadCatalogFromConfig(cl, CatalogType.flink_in_memory.name(), Optional.empty(), "test", new HashMap<>());

		assertTrue(catalog instanceof FlinkInMemoryCatalog);
	}

	@Test
	public void testDynamicLoadingCatalog() throws Exception {
		ReadableCatalog catalog =
			CatalogLoader.loadCatalogFromConfig(
				cl, CatalogType.custom.name(), Optional.of("org.apache.flink.table.catalog.FlinkInMemoryCatalogFactory"), "test", new HashMap<>());

		assertTrue(catalog instanceof FlinkInMemoryCatalog);
	}
}

