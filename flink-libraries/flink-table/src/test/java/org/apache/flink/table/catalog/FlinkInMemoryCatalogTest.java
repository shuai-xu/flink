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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for FlinkInMemoryCatalog.
 */
public class FlinkInMemoryCatalogTest extends CatalogTestBase {

	@Override
	public String getTableType() {
		return "csv";
	}

	@Before
	public void setUp() {
		catalog = new FlinkInMemoryCatalog(db1);
	}

	@Test
	public void testRenameDb() {
		CatalogDatabase schema = createDb();
		catalog.createDatabase(db1, schema, false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.renameDatabase(db1, db2, false);

		assertTrue(catalog.listDatabases().contains(db2));
		assertFalse(catalog.listDatabases().contains(db1));
	}
}
