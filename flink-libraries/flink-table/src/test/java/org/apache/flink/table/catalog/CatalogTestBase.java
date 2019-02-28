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
import org.apache.flink.table.api.FunctionAlreadyExistException;
import org.apache.flink.table.api.FunctionNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.config.CatalogDatabaseConfig;
import org.apache.flink.table.catalog.config.CatalogTableConfig;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.table.types.TimestampType;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.CatalogTestUtil.compare;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base for unit tests of a specific catalog, like FlinkInMemoryCatalog and HiveCatalog.
 */
public abstract class CatalogTestBase {

	protected final String testCatalogName = "test-catalog";
	protected final String db1 = "db1";
	protected final String db2 = "db2";

	protected final String t1 = "t1";
	protected final String t2 = "t2";
	protected final ObjectPath path1 = new ObjectPath(db1, t1);
	protected final ObjectPath path2 = new ObjectPath(db2, t2);
	protected final ObjectPath path3 = new ObjectPath(db1, t2);
	protected final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	protected final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

	protected static final String TEST_COMMENT = "test comment";

	protected static ReadableWritableCatalog catalog;

	public abstract String getTableType();

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@After
	public void close() {
		catalog.dropTable(path1, true);
		catalog.dropTable(path2, true);
		catalog.dropTable(path3, true);
		catalog.dropFunction(path1, true);
		catalog.dropFunction(path3, true);
		catalog.dropDatabase(db1, true);
		catalog.dropDatabase(db2, true);
	}

	@AfterClass
	public static void clean() throws IOException {
		catalog.close();
	}

	// ------ tables ------

	@Test
	public void testCreateTable_Streaming() {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable streamingTable = createStreamingTable();
		catalog.createTable(path1, streamingTable, false);

		compare(streamingTable, catalog.getTable(path1));
	}

	@Test
	public void testCreateTable_Batch() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		compare(table, catalog.getTable(path1));

		List<ObjectPath> tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		List<ObjectPath> s1Tables = catalog.listTables(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		compare(table, catalog.getTable(path1));

		tables = catalog.listAllTables();

		assertEquals(1, tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());

		s1Tables = catalog.listTables(db1);

		assertEquals(1, s1Tables.size());
		assertEquals(path1.getFullName(), tables.get(0).getFullName());
	}

	@Test
	public void testCreateTable_DatabaseNotExistException() {
		assertFalse(catalog.dbExists(db1));

		exception.expect(DatabaseNotExistException.class);
		catalog.createTable(nonExistObjectPath, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(TableAlreadyExistException.class);
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		compare(table, catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		compare(table, catalog.getTable(path1));
	}

	@Test
	public void testGetTable_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testGetTable_TableNotExistException_NoDb() {
		exception.expect(TableNotExistException.class);
		catalog.getTable(nonExistObjectPath);
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

	@Test
	public void testDropTable_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistObjectPath, true);
	}

	@Test
	public void testAlterTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		compare(table, catalog.getTable(path1));

		CatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		compare(newTable, catalog.getTable(path1));

		catalog.dropTable(path1, false);

		// Partitioned table
		table = createPartitionedTable();
		catalog.createTable(path1, table, false);

		compare(catalog.getTable(path1), table);

		newTable = createAnotherPartitionedTable();
		catalog.alterTable(path1, newTable, false);

		compare(newTable, catalog.getTable(path1));
	}

	@Test
	public void testAlterTable_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistObjectPath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testRenameTable() {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		compare(table, catalog.getTable(path1));

		catalog.renameTable(path1, t2, false);

		compare(table, catalog.getTable(path3));
		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testRenameTable_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testRenameTable_TableNotExistException_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.renameTable(path1, t2, true);
	}

	@Test
	public void testRenameTable_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);
		catalog.createTable(path3, createAnotherTable(), false);

		exception.expect(TableAlreadyExistException.class);
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testTableExists() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}

	// ------ table and column stats ------

	@Test
	public void testGetTableStats_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.getTableStats(path1);
	}

	@Test
	public void testAlterTableStats() {
		// Non-partitioned table
		catalog.createDatabase(db1, createDb(), false);

		TableSchema schema = new TableSchema(
			new String[] {
				"1",
				"2",
				"3",
				"4",
				"5",
				"6",
				"7",
				"8",
				"9",
				"10",
				"11",
				"12"
			},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.BOOLEAN,
				DataTypes.BYTE,
				DataTypes.SHORT,
				DataTypes.INT,
				DataTypes.LONG,
				DataTypes.FLOAT,
				DataTypes.DOUBLE,
				DataTypes.DATE,
				DataTypes.CHAR,
				new DecimalType(6, 2),
				TimestampType.TIMESTAMP,
			}
		);

		CatalogTable table = CatalogTestUtil.createCatalogTable(
			getTableType(),
			schema,
			getBatchTableProperties());
		catalog.createTable(path1, table, false);

		TableStats tableStats = TableStats.builder().rowCount(100L).colStats(new HashMap<String, ColumnStats>() {{
			// StringType
			put("1", new ColumnStats(11L, 1L, 1.1, 1, null, null));
			// BooleanType
			put("2", new ColumnStats(null, 2L, null, null, null, null));
			// ByteType
			put("3", new ColumnStats(13L, 3L, null, null, Byte.valueOf((byte) 3), Byte.valueOf((byte) 2)));
			// ShortType
			put("4", new ColumnStats(15L, 5L, null, null, Short.valueOf((short) 4), Short.valueOf((short) 3)));
			// IntType
			put("5", new ColumnStats(14L, 4L, null, null, Integer.valueOf(5), Integer.valueOf(4)));
			// LongType
			put("6", new ColumnStats(16L, 7L, null, null, Long.valueOf(6L), Long.valueOf(5L)));
			// FloatType
			put("7", new ColumnStats(17L, 8L, null, null, Float.valueOf(8.8f), Float.valueOf(7.7f)));
			// DoubleType
			put("8", new ColumnStats(18L, 9L, null, null, Double.valueOf(9.9d), Double.valueOf(8.8d)));
			// DateType
			put("9", new ColumnStats(19L, 10L, null, null, new Date(1547529235000L), new Date(1540529200000L)));
			// CharType
			put("10", new ColumnStats(19L, 10L, 1.0, 1, null, null));
			// DecimalType
			put("11", new ColumnStats(19L, 10L, null, null, Decimal.fromLong(999999, 6, 3), Decimal.fromLong(666666, 6, 3)));
			// TimetampType.Timestamp
			put("12", new ColumnStats(19L, 10L, null, null, new Timestamp(1547529235000L), new Timestamp(1540529200000L)));
		}}).build();

		catalog.alterTableStats(path1, tableStats, false);
		TableStats actual = catalog.getTableStats(path1);

		assertEquals(tableStats.toString(), actual.toString());
	}

	@Test
	public void testAlterTableStats_partitionedTable() {
		// alterTableStats() should do nothing for partitioned tables
		// getTableStats() should return empty column stats for partitioned tables
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		TableStats stats = TableStats.builder().rowCount(0L).colStats(new HashMap<String, ColumnStats>() {{
			put("first", new ColumnStats(11L, 1L, 1.1, 1, null, null));
			put("second", new ColumnStats(14L, 4L, null, null, 5, 4));
			put("third", new ColumnStats(11L, 1L, 1.1, 1, null, null));
		}}).build();

		catalog.alterTableStats(path1, stats, false);

		assertEquals(TableStats.UNKNOWN(), catalog.getTableStats(path1));
	}

	@Test
	public void testAlterTableStats_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.alterTableStats(new ObjectPath(catalog.getDefaultDatabaseName(), "nonexist"), null, false);
	}

	@Test
	public void testAlterTableStats_TableNotExistException_ignore() {
		catalog.alterTableStats(new ObjectPath("non", "exist"), null, true);
	}

	// ------ views ------

	@Test
	public void testCreateView() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		CatalogView view = createView();
		catalog.createView(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		compare(view, catalog.getTable(path1));
	}

	@Test
	public void testCreateView_DatabaseNotExistException() {
		assertFalse(catalog.dbExists(db1));

		exception.expect(DatabaseNotExistException.class);
		catalog.createView(nonExistObjectPath, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createView(path1, createView(), false);

		exception.expect(TableAlreadyExistException.class);
		catalog.createView(path1, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createView(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		compare(view, catalog.getTable(path1));

		catalog.createView(path1, createAnotherView(), true);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		compare(view, catalog.getTable(path1));
	}

	@Test
	public void testDropView() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createView(path1, createView(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testAlterView() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createView(path1, view, false);

		compare(view, catalog.getTable(path1));

		CatalogView newView = createAnotherView();
		catalog.alterTable(path1, newView, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		compare(newView, catalog.getTable(path1));
	}

	@Test
	public void testAlterView_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterView_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterView(nonExistObjectPath, createView(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testListView() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listAllTables().isEmpty());

		catalog.createView(path1, createView(), false);
		catalog.createTable(path3, createTable(), false);

		assertEquals(2, catalog.listTables(db1).size());
		assertEquals(new HashSet<>(Arrays.asList(path1, path3)), new HashSet<>(catalog.listTables(db1)));
		assertEquals(Arrays.asList(path1), catalog.listViews(db1));
	}

	// ------ databases ------

	@Test
	public void testCreateDb() {
		catalog.createDatabase(db2, createDb(), false);

		assertEquals(2, catalog.listDatabases().size());
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(DatabaseAlreadyExistException.class);
		catalog.createDatabase(db1, createDb(), false);
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExist_ignored() {
		CatalogDatabase cd1 = createDb();
		catalog.createDatabase(db1, cd1, false);
		List<String> dbs = catalog.listDatabases();

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabaseName())), new HashSet<>(dbs));

		catalog.createDatabase(db1, createAnotherDb(), true);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabaseName())), new HashSet<>(dbs));
	}

	@Test
	public void testGetDb_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testDropDb() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}

	@Test
	public void testDropDb_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropDb_DatabaseNotExist_Ignore() {
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testDropDb_databaseIsNotEmpty() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(FlinkCatalogException.class);
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testAlterDb() {
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));

		CatalogDatabase newDb = createAnotherDb();
		catalog.alterDatabase(db1, newDb, false);

		assertFalse(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));
		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(newDb.getProperties().entrySet()));
	}

	@Test
	public void testAlterDb_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.alterDatabase("nonexistent", createDb(), false);
	}

	@Test
	public void testAlterDb_DatabaseNotExist_ignored() {
		catalog.alterDatabase("nonexistent", createDb(), true);

		assertFalse(catalog.dbExists("nonexistent"));
	}

	@Test
	public void testDbExists() {
		assertFalse(catalog.dbExists("nonexistent"));

		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.dbExists(db1));
	}

	// ------ partitions ------

	@Test
	public void testCreatePartition() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		assertTrue(catalog.listPartitions(path1).isEmpty());

		catalog.createPartition(path1, createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));
		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1, createPartitionSpecSubset()));
		compare(createPartition(), catalog.getPartition(path1, createPartitionSpec()));

		catalog.createPartition(path1, createAnotherPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec(), createAnotherPartitionSpec()), catalog.listPartitions(path1));
		assertEquals(Arrays.asList(createPartitionSpec(), createAnotherPartitionSpec()), catalog.listPartitions(path1, createPartitionSpecSubset()));
		compare(createAnotherPartition(), catalog.getPartition(path1, createAnotherPartitionSpec()));
	}

	@Test
	public void testCreateParition_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.createPartition(path1, createPartition(), false);
	}

	@Test
	public void testCreateParition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(TableNotPartitionedException.class);
		catalog.createPartition(path1, createPartition(), false);
	}

	@Test
	public void testCreateParition_PartitionAlreadExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		exception.expect(PartitionAlreadyExistException.class);
		catalog.createPartition(path1, createPartition(), false);
	}

	@Test
	public void testCreateParition_PartitionAlreadExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);
		catalog.createPartition(path1, createPartition(), true);
	}

	@Test
	public void testDropPartition() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));

		catalog.dropPartition(path1, createPartitionSpec(), false);

		assertEquals(Arrays.asList(), catalog.listPartitions(path1));
	}

	@Test
	public void testDropPartition_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.dropPartition(path1, createPartitionSpec(), false);
	}

	@Test
	public void testDropPartition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(TableNotPartitionedException.class);
		catalog.dropPartition(path1, createPartitionSpec(), false);
	}

	@Test
	public void testDropPartition_PartitionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		exception.expect(PartitionNotExistException.class);
		catalog.dropPartition(path1, createPartitionSpec(), false);
	}

	@Test
	public void testDropPartition_PartitionNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.dropPartition(path1, createPartitionSpec(), true);
	}

	@Test
	public void testAlterPartition() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));
		CatalogPartition cp = catalog.getPartition(path1, createPartitionSpec());
		compare(createPartition(), cp);
		assertNull(cp.getProperties().get("k"));

		Map<String, String> partitionProperties = getBatchTableProperties();
		partitionProperties.put("k", "v");

		CatalogPartition another = createPartition(cp.getPartitionSpec(), partitionProperties);
		catalog.alterPartition(path1, another, false);

		assertEquals(Arrays.asList(createPartitionSpec()), catalog.listPartitions(path1));
		cp = catalog.getPartition(path1, createPartitionSpec());
		compare(another, cp);
		assertEquals("v", cp.getProperties().get("k"));
	}

	@Test
	public void testAlterPartition_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.alterPartition(path1, createPartition(), false);
	}

	@Test
	public void testAlterPartition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(TableNotPartitionedException.class);
		catalog.alterPartition(path1, createPartition(), false);
	}

	@Test
	public void testAlterPartition_PartitionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		exception.expect(PartitionNotExistException.class);
		catalog.alterPartition(path1, createPartition(), false);
	}

	@Test
	public void testAlterPartition_PartitionNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.alterPartition(path1, createPartition(), true);
	}

	@Test
	public void testGetPartition_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test
	public void testGetPartition_TableNotPartitionedException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(TableNotPartitionedException.class);
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test
	public void testGetParition_PartitionNotExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);

		exception.expect(PartitionNotExistException.class);
		catalog.getPartition(path1, createPartitionSpec());
	}

	@Test
	public void testPartitionExists() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(), false);
		catalog.createPartition(path1, createPartition(), false);

		assertTrue(catalog.partitionExists(path1, createPartitionSpec()));
		assertFalse(catalog.partitionExists(path2, createPartitionSpec()));
		assertFalse(catalog.partitionExists(ObjectPath.fromString("non.exist"), createPartitionSpec()));
	}

	// ------ functions ------

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
	public void testAlterFunction() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func = createFunction();
		catalog.createFunction(path1, func, false);

		compare(func, catalog.getFunction(path1));

		CatalogFunction newFunc = createAnotherFunction();
		catalog.alterFunction(path1, newFunc, false);

		assertNotEquals(func, catalog.getFunction(path1));
		compare(newFunc, catalog.getFunction(path1));
	}

	@Test
	public void testAlterFunction_FunctionNotExistException() {
		exception.expect(FunctionNotExistException.class);
		catalog.alterFunction(nonExistObjectPath, createFunction(), false);
	}

	@Test
	public void testAlterFunction_FunctionNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterFunction(nonExistObjectPath, createFunction(), true);

		assertFalse(catalog.functionExists(nonExistObjectPath));
	}

	@Test
	public void testListFunctions() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func = createFunction();
		catalog.createFunction(path1, func, false);

		assertEquals(path1, catalog.listFunctions(db1).get(0));
	}

	@Test
	public void testListFunctions_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.listFunctions(db1);
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

	// ------ utilities ------

	protected CatalogTable createStreamingTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			getStreamingTableProperties());
	}

	protected CatalogTable createTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			getBatchTableProperties());
	}

	protected CatalogTable createAnotherTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createAnotherTableSchema(),
			getBatchTableProperties());
	}

	protected CatalogTable createPartitionedTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createTableSchema(),
			TableStats.UNKNOWN(),
			getBatchTableProperties(),
			createPartitionCols());
	}

	protected CatalogTable createAnotherPartitionedTable() {
		return CatalogTestUtil.createCatalogTable(
			getTableType(),
			createAnotherTableSchema(),
			TableStats.UNKNOWN(),
			getBatchTableProperties(),
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
		return createPartition(createPartitionSpec(), getBatchTableProperties());
	}

	protected CatalogPartition createAnotherPartition() {
		return createPartition(createAnotherPartitionSpec(), getBatchTableProperties());
	}

	protected CatalogPartition createPartition(CatalogPartition.PartitionSpec partitionSpec, Map<String, String> partitionProperties) {
		return new CatalogPartition(partitionSpec, partitionProperties);
	}

	protected CatalogDatabase createDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("k1", "v1");
			put(CatalogDatabaseConfig.DATABASE_COMMENT, TEST_COMMENT);
		}});
	}

	protected CatalogDatabase createAnotherDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("k2", "v2");
			put(CatalogDatabaseConfig.DATABASE_COMMENT, TEST_COMMENT);
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

	protected CatalogView createView() {
		return CatalogView.createCatalogView(
			createTable(),
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", testCatalogName, path1.getFullName()));
	}

	protected CatalogView createAnotherView() {
		return CatalogView.createCatalogView(
			createAnotherTable(),
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", testCatalogName, path2.getFullName()));
	}

	protected Map<String, String> getBatchTableProperties() {
		return new HashMap<String, String>() {{
			put(CatalogTableConfig.IS_STREAMING, "false");
		}};
	}

	protected Map<String, String> getStreamingTableProperties() {
		return new HashMap<String, String>() {{
			put(CatalogTableConfig.IS_STREAMING, "true");
		}};
	}

	protected CatalogFunction createFunction() {
		return new CatalogFunction("test");
	}

	protected CatalogFunction createAnotherFunction() {
		return new CatalogFunction("test2");
	}
}
