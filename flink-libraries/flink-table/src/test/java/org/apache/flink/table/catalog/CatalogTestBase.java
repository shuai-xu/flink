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

import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base for unit tests of a specific catalog, like FlinkInMemoryCatalog and HiveCatalog.
 */
public abstract class CatalogTestBase {
	protected final String db1 = "db1";
	protected final String db2 = "db2";

	protected final String t1 = "t1";
	protected final String t2 = "t2";
	protected final ObjectPath path1 = new ObjectPath(db1, t1);
	protected final ObjectPath path2 = new ObjectPath(db2, t2);
	protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	protected final ObjectPath nonExistTablePath = ObjectPath.fromString("db1.nonexist");

	protected static ReadableWritableCatalog catalog;

	public abstract String getTableType();

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
	public void testCreateTableWithNonexistentDb() {
		assertFalse(catalog.dbExists(db1));

		catalog.createTable(nonExistTablePath, createTable(), false);
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testCreateExistedTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateExistedTableIgnore() {
		catalog.createDatabase(db1, createDb(), false);

		ExternalCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		assertEquals(table, catalog.getTable(path1));
	}

	@Test
	public void testGetTable() {
		ExternalCatalogTable originTable = createTable();

		assertFalse(catalog.tableExists(path1));

		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, originTable, false);

		assertEquals(catalog.getTable(path1), originTable);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetNonexistentTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.getTable(nonExistTablePath);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetNonexistentTableWithMissingDb() {
		catalog.getTable(nonExistDbPath);
	}

	@Test
	public void testDropTable() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testDropNonexistentTable() {
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropNonexistentTableIgnore() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistTablePath, true);
	}

	@Test
	public void testAlterTable() {
		ExternalCatalogTable table = createTable();
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		assertEquals(catalog.getTable(path1), table);

		ExternalCatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testAlterNonexisttentTable() {
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterNonexistentTableIgnore() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistTablePath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistTablePath));
	}

	@Test
	public void testCreateDb() {
		catalog.createDatabase(db2, createDb(), false);

		assertEquals(1, filterBuiltInDb(catalog.listDatabases()).size());
	}

	@Test(expected = DatabaseAlreadyExistException.class)
	public void testCreateExistentDb() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createDatabase(db1, createDb(), false);
	}

	@Test
	public void testCreateExistentDbIgnore() {
		CatalogDatabase cd1 = createDb();
		catalog.createDatabase(db1, cd1, false);
		List<String> dbs = catalog.listDatabases();

		assertEquals(cd1, catalog.getDatabase(db1));
		assertEquals(1, filterBuiltInDb(dbs).size());
		assertEquals(db1, filterBuiltInDb(dbs).get(0));

		catalog.createDatabase(db1, createAnotherDb(), true);

		assertEquals(cd1, catalog.getDatabase(db1));
		assertEquals(1, filterBuiltInDb(dbs).size());
		assertEquals(db1, filterBuiltInDb(dbs).get(0));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testGetNonexistentDb() {
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testDropDb() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}

	@Test (expected = DatabaseNotExistException.class)
	public void testDropNonexistentDb() {
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropNonexistentDbIgnore() {
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testAlterDb() {
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		assertEquals(db, catalog.getDatabase(db1));

		CatalogDatabase newDb = createAnotherDb();
		catalog.alterDatabase(db1, newDb, false);

		assertNotEquals(db, catalog.getDatabase(db1));
		assertEquals(newDb, catalog.getDatabase(db1));
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testAlterNonexistentDb() {
		catalog.alterDatabase("nonexistent", createDb(), false);
	}

	@Test
	public void testAlterNonexistentDbIgnore() {
		catalog.alterDatabase("nonexistent", createDb(), true);

		assertFalse(catalog.dbExists("nonexistent"));
	}

	@Test
	public void testDbExists() {
		assertFalse(catalog.dbExists("nonexistent"));

		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.dbExists(db1));
	}

	@Test
	public void testTableExists() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}

	private List<String> filterBuiltInDb(List<String> dbs) {
		return dbs.stream()
			.filter(db -> !db.equals("default"))
			.collect(Collectors.toList());
	}

	protected CatalogDatabase createDb() {
		return new CatalogDatabase(new HashMap<>());
	}

	protected CatalogDatabase createAnotherDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("key", "value");
		}});
	}

	protected ExternalCatalogTable createTable() {
		TableSchema schema = new TableSchema(
			new String[] {"first", "second"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.INT
			}
		);

		return createTable(schema);
	}

	protected ExternalCatalogTable createAnotherTable() {
		TableSchema schema = new TableSchema(
			new String[] {"first", "second"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.STRING  // different from create table instance.
			}
		);

		return createTable(schema);
	}

	private ExternalCatalogTable createTable(TableSchema schema) {
		return new ExternalCatalogTable(
			getTableType(),
			schema,
			getTableProperties(),
			null,
			null,
			null,
			new LinkedHashSet<>(),
			false,
			null,
			null,
			-1L,
			0L,
			-1L);
	}

	protected Map<String, String> getTableProperties() {
		return new HashMap<>();
	}
}
