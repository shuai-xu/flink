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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for FlinkInMemoryCatalog.
 */
public class FlinkInMemoryCatalogTest {

	private FlinkInMemoryCatalog catalog;

	private final String db1 = "db1";
	private final String db2 = "db2";
	private final ObjectPath table1 = ObjectPath.fromString("db1.t1");
	private final ObjectPath table2 = ObjectPath.fromString("db2.t2");
	private final ObjectPath nonExistTable = ObjectPath.fromString("non.exist");

	@Before
	public void setUp() {
		catalog = new FlinkInMemoryCatalog(db1);
	}

	@Test
	public void testCreateTable() {
		assertTrue(catalog.listAllTables().isEmpty());

		catalog.createDatabase(db1, createSchema(), false);
		catalog.createTable(table1, createTable(), false);
		List<ObjectPath> tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(table1.getFullName(), tables.get(0).getFullName());

		List<ObjectPath> s1Tables = catalog.listTablesByDatabase(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(table1.getFullName(), tables.get(0).getFullName());
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testCreateExistedTable() {
		catalog.createDatabase(db1, createSchema(), false);
		catalog.createTable(table1, createTable(), false);
		catalog.createTable(table1, createTable(), false);
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testCreateTableNotExistSchema() {
		assertTrue(catalog.listAllTables().isEmpty());

		catalog.createTable(ObjectPath.fromString("nonexist.t1"), createTable(), false);
	}

	@Test
	public void testGetTable() {
		CatalogTable originTable = createTable();
		catalog.createDatabase(db1, createSchema(), false);
		catalog.createTable(table1, originTable, false);

		assertEquals(catalog.getTable(table1), originTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetNotExistTable() {
		catalog.getTable(nonExistTable);
	}

	@Test
	public void testAlterTable() {
		CatalogTable table = createTable();
		catalog.createDatabase(db1, createSchema(), false);
		catalog.createTable(table1, table, false);

		assertEquals(catalog.getTable(table1), table);

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(table1, newTable, false);
		CatalogTable currentTable = catalog.getTable(table1);

		assertNotEquals(table, currentTable);
		assertEquals(newTable, currentTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testAlterNotExistTable() {
		catalog.alterTable(nonExistTable, createTable(), false);
	}

	@Test
	public void testRenameTable() {
		catalog.createDatabase(db1, createSchema(), false);
		catalog.createDatabase(db2, createAnotherSchema(), false);

		CatalogTable table = createTable();
		catalog.createTable(table1, table, false);

		assertEquals(table1, catalog.listAllTables().get(0));

		catalog.renameTable(table1, table2, false);

		assertEquals(table2, catalog.listAllTables().get(0));
	}

	@Test(expected = TableNotExistException.class)
	public void testRenameNotExistTable() {
		catalog.renameTable(nonExistTable, table2, false);
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testRenameTableNotExistSchema() {
		CatalogTable table = createTable();
		catalog.createTable(table1, table, false);

		assertEquals(table1, catalog.listAllTables().get(0));

		catalog.renameTable(table1, ObjectPath.fromString("nonexist.t1"), false);
	}

	@Test
	public void testDropTable() {
		catalog.createDatabase(db1, createSchema(), false);
		catalog.createTable(table1, createTable(), false);

		assertTrue(catalog.listAllTables().contains(table1));

		catalog.dropTable(table1, false);

		assertFalse(catalog.listAllTables().contains(table1));
	}

	@Test(expected = TableNotExistException.class)
	public void testDropNotExistTable() {
		catalog.dropTable(nonExistTable, false);
	}

	@Test
	public void testCreateSchema() {
		catalog.createDatabase("db2", createSchema(), false);

		assertEquals(1, catalog.listDatabases().size());
	}

	@Test(expected = DatabaseAlreadyExistException.class)
	public void testCreateExistedSchema() {
		catalog.createDatabase("existed", createSchema(), false);

		assertNotNull(catalog.getDatabase("existed"));

		List<String> schemas = catalog.listDatabases();

		assertEquals(1, schemas.size());
		assertEquals("existed", schemas.get(0));

		catalog.createDatabase("existed", createSchema(), false);
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testGetNotExistSchema() {
		catalog.getDatabase("notexisted");
	}

	@Test
	public void testAlterSchema() {
		CatalogDatabase schema = createSchema();
		catalog.createDatabase(db1, schema, false);

		assertEquals(schema, catalog.getDatabase(db1));

		CatalogDatabase newSchema = createAnotherSchema();
		catalog.alterDatabase(db1, newSchema, false);
		CatalogDatabase currentSchema = catalog.getDatabase(db1);

		assertNotEquals(schema, currentSchema);
		assertEquals(newSchema, currentSchema);
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testAlterNotExistSchema() {
		catalog.alterDatabase("nonexist", createSchema(), false);
	}

	@Test
	public void testRenameSchema() {
		CatalogDatabase schema = createSchema();
		catalog.createDatabase(db1, schema, false);

		assertEquals(db1, catalog.listDatabases().get(0));

		catalog.renameDatabase(db1, db2, false);

		assertEquals(db2, catalog.listDatabases().get(0));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testRenameNotExistSchema() {
		catalog.renameDatabase("nonexisit", db2, false);
	}

	@Test
	public void testDropSchema() {
		catalog.createDatabase(db1, createSchema(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}

	private CatalogDatabase createSchema() {
		return new CatalogDatabase(new HashMap<>());
	}

	private CatalogDatabase createAnotherSchema() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("key", "value");
		}});
	}

	private CatalogTable createTable() {
		TableSchema schema = new TableSchema(
			new String[] {"first", "second"},
			new InternalType[]{ DataTypes.STRING, DataTypes.INT }
		);

		return new CatalogTable("csv", schema, null, new HashMap<>());
	}

	private CatalogTable createAnotherTable() {
		TableSchema schema = new TableSchema(
			new String[] {"first", "second"},
			new InternalType[]{ DataTypes.STRING, DataTypes.STRING }
		);

		return new CatalogTable("csv", schema, null, new HashMap<>());
	}
}
