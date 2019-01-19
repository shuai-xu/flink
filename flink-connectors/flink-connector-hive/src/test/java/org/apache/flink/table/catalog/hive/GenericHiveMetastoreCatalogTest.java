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

import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ReadableWritableCatalog;
import org.apache.flink.table.plan.stats.TableStats;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for GenericHiveMetastoreCatalog.
 *
 * <p>Since GenericHiveMetastoreCatalog is unfinished, we temporarily copy necessary tests from CatalogTestBase.
 * Once GenericHiveMetastoreCatalog is finished, we should remove all unit tests in this class and make this test class
 * extend CatalogTestBase.
 */
public class GenericHiveMetastoreCatalogTest {
	protected final String db1 = "db1";
	protected final String db2 = "db2";

	protected final String t1 = "t1";
	protected final String t2 = "t2";
	protected final ObjectPath path1 = new ObjectPath(db1, t1);
	protected final ObjectPath path2 = new ObjectPath(db2, t2);
	protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	protected final ObjectPath nonExistTablePath = ObjectPath.fromString("db1.nonexist");

	protected static ReadableWritableCatalog catalog;

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createGenericHiveMetastoreCatalog();
		catalog.open();
	}

	@After
	public void close() {
		catalog.dropTable(new ObjectPath(db1, t1), true);
		catalog.dropTable(new ObjectPath(db2, t2), true);
		catalog.dropDatabase(db1, true);
		catalog.dropDatabase(db2, true);
	}

	@AfterClass
	public static void clean() throws IOException {
		catalog.close();
	}

	public String getTableType() {
		return "generic_hive_metastore";
	}

	// ------ tables ------

	@Test
	public void testCreateTable() {
		assertTrue(catalog.listAllTables().isEmpty());
		assertFalse(catalog.tableExists(path1));

		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		List<ObjectPath> tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		List<ObjectPath> s1Tables = catalog.listTables(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		catalog.dropTable(path1, false);

		// TODO: enable creating partitioned table
//		// Partitioned table
//		table = createPartitionedTable();
//		catalog.createHiveTable(path1, table, false);
//
//		assertEquals(table, catalog.getTable(path1));
//
//		tables = catalog.listAllTables();
//
//		assertEquals(1, tables.size());
//		assertEquals(path1.getFullName(), tables.get(0).getFullName());
//
//		s1Tables = catalog.listTables(db1);
//
//		assertEquals(1, s1Tables.size());
//		assertEquals(path1.getFullName(), tables.get(0).getFullName());
	}

	@Test(expected = DatabaseNotExistException.class)
	public void testCreateTable_DatabaseNotExistException() {
		assertFalse(catalog.dbExists(db1));

		catalog.createTable(nonExistTablePath, createTable(), false);
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testCreateTable_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		assertEquals(table, catalog.getTable(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testGetTable_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.getTable(nonExistTablePath);
	}

	@Test(expected = TableNotExistException.class)
	public void testGetTable_TableNotExistException_NoDb() {
		catalog.getTable(nonExistTablePath);
	}

	@Test
	public void testDropTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));

		// Partitioned table
		catalog.createTable(path1, createPartitionedTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test(expected = TableNotExistException.class)
	public void testDropTable_TableNotExistException() {
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistTablePath, true);
	}

	@Ignore
	@Test
	public void testAlterTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		assertEquals(table, catalog.getTable(path1));

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		assertEquals(catalog.getTable(path1), table);

		newTable = createAnotherPartitionedTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));
	}

	@Ignore
	@Test
	public void testAlterTable_withTableStats() {
		CatalogTable table = createTable();
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		assertEquals(catalog.getTable(path1), table);

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		assertEquals(newTable, catalog.getTable(path1));
	}

	@Ignore
	@Test(expected = TableNotExistException.class)
	public void testAlterTable_TableNotExistException() {
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Ignore
	@Test
	public void testAlterTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistTablePath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistTablePath));
	}

	@Test
	public void testTableExists() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}



	// TODO: Code below is copied from CatalogTestBase. Should be removed once GenericHiveMetastoreCatalog is finished.
	// ------ utilities ------

	protected CatalogTable createTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			getTableProperties());
	}

	protected CatalogTable createAnotherTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createAnotherTableSchema(),
			getTableProperties());
	}

	protected CatalogTable createPartitionedTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			new TableStats(),
			getTableProperties(),
			createPartitionCols());
	}

	protected CatalogTable createAnotherPartitionedTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createAnotherTableSchema(),
			new TableStats(),
			getTableProperties(),
			createPartitionCols());
	}

	private LinkedHashSet<String> createPartitionCols() {
		return new LinkedHashSet<String>() {{
			add("second");
			add("third");
		}};
	}

	protected CatalogPartition.PartitionSpec createPartitionSpec() {
		return new CatalogPartition.PartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2000");
				put("second", "bob");
			}});
	}

	protected CatalogPartition.PartitionSpec createAnotherPartitionSpec() {
		return new CatalogPartition.PartitionSpec(
			new HashMap<String, String>() {{
				put("third", "2010");
				put("second", "bob");
			}});
	}

	protected CatalogPartition.PartitionSpec createPartitionSpecSubset() {
		return new CatalogPartition.PartitionSpec(
			new HashMap<String, String>() {{
				put("second", "bob");
			}});
	}

	protected CatalogPartition createPartition() {
		return createPartition(createPartitionSpec(), getTableProperties());
	}

	protected CatalogPartition createAnotherPartition() {
		return createPartition(createAnotherPartitionSpec(), getTableProperties());
	}

	protected CatalogPartition createPartition(CatalogPartition.PartitionSpec partitionSpec, Map<String, String> partitionProperties) {
		return new CatalogPartition(partitionSpec, partitionProperties);
	}

	protected CatalogDatabase createDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("k1", "v1");
		}});
	}

	protected CatalogDatabase createAnotherDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("k2", "v2");
		}});
	}

	private TableSchema createTableSchema() {
		return new TableSchema(
			new String[] {"first", "second", "third"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.INT,
				DataTypes.STRING,
			}
		);
	}

	private TableSchema createAnotherTableSchema() {
		return new TableSchema(
			new String[] {"first2", "second", "third"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.STRING,  // different from create table instance.
				DataTypes.STRING
			}
		);
	}

	protected Map<String, String> getTableProperties() {
		return new HashMap<>();
	}
}
