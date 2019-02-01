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

import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.FunctionAlreadyExistException;
import org.apache.flink.table.api.FunctionNotExistException;

import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.catalog.CatalogTestUtil.compare;
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
	public void testCreateFunction() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.functionExists(path1));

		catalog.createFunction(path1, createFunction(), false);

		assertTrue(catalog.functionExists(path1));
	}

	@Test
	public void testCreateFunction_DatabaseNotExistException() {
		assertFalse(catalog.dbExists(db1));

		exception.expect(DatabaseNotExistException.class);
		catalog.createFunction(path1, createFunction(), false);
	}

	@Test
	public void testCreateFunction_FunctionAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createFunction(path1, createFunction(), false);

		exception.expect(FunctionAlreadyExistException.class);
		catalog.createFunction(path1, createFunction(), false);
	}

	@Test
	public void testCreateFunction_FunctionAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func = createFunction();
		catalog.createFunction(path1, func, false);

		compare(func, catalog.getFunction(path1));

		catalog.createFunction(path1, createAnotherFunction(), true);

		compare(func, catalog.getFunction(path1));
	}

	@Test
	public void testGetFunction_FunctionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(FunctionNotExistException.class);
		catalog.getFunction(nonExistObjectPath);
	}

	@Test
	public void testGetFunction_FunctionNotExistException_NoDb() {
		exception.expect(FunctionNotExistException.class);
		catalog.getFunction(nonExistObjectPath);
	}

	@Test
	public void testDropFunction() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createFunction(path1, createFunction(), false);

		assertTrue(catalog.functionExists(path1));

		catalog.dropFunction(path1, false);

		assertFalse(catalog.functionExists(path1));
	}

	@Test
	public void testDropFunction_FunctionNotExistException() {
		exception.expect(FunctionNotExistException.class);
		catalog.dropFunction(nonExistDbPath, false);
	}

	@Test
	public void testDropFunction_FunctionNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropFunction(nonExistObjectPath, true);
	}

	private CatalogFunction createFunction() {
		return new CatalogFunction("test");
	}

	private CatalogFunction createAnotherFunction() {
		return new CatalogFunction("test2");
	}
}
