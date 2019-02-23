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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.hive.config.HiveTableConfig;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Test for HiveCatalog.
 */
public class HiveCatalogTest extends CatalogTestBase {

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	@Override
	@After
	public void close() {
		catalog.dropTable(path1, true);
		catalog.dropTable(path2, true);
		catalog.dropTable(path3, true);
		catalog.dropDatabase(db1, true);
		catalog.dropDatabase(db2, true);
	}

	@Override
	public String getTableType() {
		return "hive";
	}

	@Override
	protected Map<String, String> getBatchTableProperties() {
		Map<String, String> properties = super.getBatchTableProperties();

		properties.put(HiveTableConfig.HIVE_TABLE_LOCATION, HiveTestUtils.warehouseDir + "/tmp");

		return properties;
	}

	// ------------------------------------------------
	// Override the following tests in CatalogTestBase since HiveCatalog doesn't support create/alter/drop functions.
	// Tests annotated with only @Override will be ignored.
	// ------------------------------------------------

	@Override
	@Test
	public void testCreateFunction() {
		exception.expect(UnsupportedOperationException.class);
		catalog.createFunction(path1, createFunction(), false);
	}

	@Override
	public void testCreateFunction_DatabaseNotExistException() {
		// Ignore
	}

	@Override
	public void testCreateFunction_FunctionAlreadyExistException() {
		// Ignore
	}

	@Override
	public void testCreateFunction_FunctionAlreadyExist_ignored() {
		// Ignore
	}

	@Override
	@Test
	public void testAlterFunction() {
		exception.expect(UnsupportedOperationException.class);
		catalog.alterFunction(path1, createFunction(), false);
	}

	@Override
	public void testAlterFunction_FunctionNotExistException() {
		// Ignore
	}

	@Override
	public void testAlterFunction_FunctionNotExist_ignored() {
		// Ignore
	}

	@Override
	@Test
	public void testListFunctions() {
		// TODO: test listFunctions() return both UDFs and built-in functions
	}

	@Override
	@Test
	public void testDropFunction() {
		exception.expect(UnsupportedOperationException.class);
		catalog.dropFunction(path1, false);
	}

	@Override
	public void testDropFunction_FunctionNotExistException() {
		// Ignore
	}

	@Override
	public void testDropFunction_FunctionNotExist_ignored() {
		// Ignore
	}
}
