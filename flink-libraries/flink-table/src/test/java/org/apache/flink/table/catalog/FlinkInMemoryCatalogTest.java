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

import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for FlinkInMemoryCatalog.
 */
public class FlinkInMemoryCatalogTest extends CatalogTestBase {

	private FlinkInMemoryCatalog catalog;

	private final String db1 = "db1";
	private final String db2 = "db2";
	private final ObjectPath path1 = ObjectPath.fromString("db1.t1");
	private final ObjectPath path2 = ObjectPath.fromString("db2.t2");
	private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	private final ObjectPath nonExistTablePath = ObjectPath.fromString("db1.nonexist");

	@Before
	public void setUp() {
		catalog = new FlinkInMemoryCatalog(db1);
	}

	@Test
	public void testCreateTable() {
		assertTrue(catalog.listAllTables().isEmpty());

		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		List<ObjectPath> tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		List<ObjectPath> s1Tables = catalog.listTablesByDatabase(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testCreateTableNotExistDb() {
		assertTrue(catalog.listAllTables().isEmpty());

		catalog.createTable(ObjectPath.fromString("nonexist.t1"), createTable(), false);
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testCreateExistedTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testGetTable() {
		ExternalCatalogTable originTable = createTable();
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, originTable, false);

		assertEquals(catalog.getTable(path1), originTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetNotExistDb() {
		catalog.getTable(nonExistDbPath);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetNotExistTable() {
		catalog.createDatabase(db1, createDb(), false);

		catalog.getTable(nonExistTablePath);
	}

	@Test
	public void testAlterTable() {
		ExternalCatalogTable table = createTable();
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		assertEquals(catalog.getTable(path1), table);

		ExternalCatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);
		ExternalCatalogTable currentTable = catalog.getTable(path1);

		assertNotEquals(table, currentTable);
		assertEquals(newTable, currentTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testAlterNotExistDb() {
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test(expected = TableNotExistException.class)
	public void testAlterNotExistTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistTablePath, createTable(), false);
	}

	@Test
	public void testRenameTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createDatabase(db2, createAnotherDb(), false);

		ExternalCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(path1, catalog.listAllTables().get(0));

		catalog.renameTable(path1, path2.getObjectName(), false);

		assertEquals(new ObjectPath(path1.getDbName(), path2.getObjectName()), catalog.listAllTables().get(0));
	}

	@Test(expected = TableNotExistException.class)
	public void testRenameNotExistDb() {
		catalog.renameTable(nonExistDbPath, "", false);
	}

	@Test(expected = TableNotExistException.class)
	public void testRenameNotExistTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.renameTable(nonExistTablePath, path2.getObjectName(), false);
	}

	@Test
	public void testDropTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.listAllTables().contains(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.listAllTables().contains(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testDropNotExistDb() {
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test(expected = TableNotExistException.class)
	public void testDropNotExistTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistTablePath, false);
	}

	@Test
	public void testCreateDb() {
		catalog.createDatabase("db2", createDb(), false);

		assertEquals(1, catalog.listDatabases().size());
	}

	@Test(expected = DatabaseAlreadyExistException.class)
	public void testCreateExistedDb() {
		catalog.createDatabase("existed", createDb(), false);

		assertNotNull(catalog.getDatabase("existed"));

		List<String> schemas = catalog.listDatabases();

		assertEquals(1, schemas.size());
		assertEquals("existed", schemas.get(0));

		catalog.createDatabase("existed", createDb(), false);
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testGetDbNotExistDb() {
		catalog.getDatabase("notexisted");
	}

	@Test
	public void testAlterDb() {
		CatalogDatabase schema = createDb();
		catalog.createDatabase(db1, schema, false);

		assertEquals(schema, catalog.getDatabase(db1));

		CatalogDatabase newSchema = createAnotherDb();
		catalog.alterDatabase(db1, newSchema, false);
		CatalogDatabase currentSchema = catalog.getDatabase(db1);

		assertNotEquals(schema, currentSchema);
		assertEquals(newSchema, currentSchema);
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testAlterDbNotExistDb() {
		catalog.alterDatabase("nonexist", createDb(), false);
	}

	@Test
	public void testRenameDb() {
		CatalogDatabase schema = createDb();
		catalog.createDatabase(db1, schema, false);

		assertEquals(db1, catalog.listDatabases().get(0));

		catalog.renameDatabase(db1, db2, false);

		assertEquals(db2, catalog.listDatabases().get(0));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testRenameDbNotExistDb() {
		catalog.renameDatabase("nonexisit", db2, false);
	}

	@Test
	public void testDropDb() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}
}
