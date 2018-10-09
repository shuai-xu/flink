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

package org.apache.flink.externalcatalog.hive;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CrudExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalogTablePartition;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.DateType;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.IntType;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.table.types.TimeType;
import org.apache.flink.table.types.TimestampType;
import org.apache.flink.util.FileUtils;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;


/**
 * A Test case for HiveExternalCatalog.
 * We use derby ($CWD/metastore_db) as an object store.
 */
public class HiveExternalCatalogTest {

	private String database = "default";
	private HiveConf hiveConf = new HiveConf();
	private CrudExternalCatalog catalog;

	@Before
	public void setup() {
		hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
		hiveConf.setBoolean("datanucleus.schema.autoCreateTables", true);
		hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "file:///tmp/hive");
		catalog = new HiveExternalCatalog(database, hiveConf);
	}

	@Test
	public void testCreateTable() throws TException {

		// It will create metastore_db directory under the default directory
		String[] names = new String[1];
		names[0] = "a";
		InternalType[] types = new InternalType[1];
		types[0] = IntType.INSTANCE;
		TableSchema tableSchema = new TableSchema(names, types);

		Table hiveTable = MetaConverter.getEmptyTable(database, "src");
		ExternalCatalogTable table = MetaConverter.convertToExternalCatalogTable(
				hiveTable, null);
		table = new ExternalCatalogTable(
				table.tableType(),
				tableSchema,
				table.properties(),
				table.stats(),
				table.comment(),
				table.partitionColumnNames(),
				table.isPartitioned(),
				table.createTime(),
				table.lastAccessTime());

		catalog.createTable(
				"src",
				table,
				false);

		ExternalCatalogTable t = catalog.getTable("src");
		Assert.assertEquals(t.tableType(), "hive");
		Assert.assertTrue((new File("/tmp/hive/src")).exists());
		Assert.assertEquals(1, t.schema().getColumns().length);
	}

	@Test
	public void testGetStatistics() throws TException {

		// It will create metastore_db directory under the default directory
		String[] names = new String[8];
		names[0] = "a";
		names[1] = "b";
		names[2] = "c";
		names[3] = "d";
		names[4] = "e";
		names[5] = "f";
		names[6] = "g";
		names[7] = "h";

		InternalType[] types = new InternalType[8];
		types[0] = IntType.INSTANCE;
		types[1] = DateType.DATE;
		types[2] = DecimalType.DEFAULT;
		types[3] = TimeType.INSTANCE;
		types[4] = TimestampType.TIMESTAMP;
		types[5] = DateType.DATE;
		types[6] = TimeType.INSTANCE;
		types[7] = TimestampType.TIMESTAMP;

		TableSchema tableSchema = new TableSchema(names, types);

		Table hiveTable = MetaConverter.getEmptyTable(database, "src");
		// Table Level Statistics are properties
		hiveTable.getParameters().put("numRows", "8");

		ExternalCatalogTable table = MetaConverter.convertToExternalCatalogTable(
				hiveTable, null);
		table = new ExternalCatalogTable(
				table.tableType(),
				tableSchema,
				table.properties(),
				table.stats(),
				table.comment(),
				table.partitionColumnNames(),
				table.isPartitioned(),
				table.createTime(),
				table.lastAccessTime());

		catalog.createTable(
				"src",
				table,
				false);

		// Set statistics from hive API (metastore RPC)
		IMetaStoreClient msc = ((HiveExternalCatalog) catalog).getMSC();
		ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(
				true, "default", "src");

		List<ColumnStatisticsObj> colStats = new ArrayList<>(names.length);
		LongColumnStatsData longColumnStats = new LongColumnStatsData(1, 5);
		longColumnStats.setHighValue(500);
		longColumnStats.setLowValue(100);
		ColumnStatisticsData statsData = ColumnStatisticsData.longStats(longColumnStats);
		ColumnStatisticsObj colAStats = new ColumnStatisticsObj("a", "int", statsData);
		colStats.add(colAStats);

		DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(2, 6);
		dateColumnStatsData.setHighValue(new Date(10000));
		dateColumnStatsData.setLowValue(new Date(20));
		statsData = ColumnStatisticsData.dateStats(dateColumnStatsData);
		ColumnStatisticsObj colBStats = new ColumnStatisticsObj("b", "date", statsData);
		colStats.add(colBStats);

		DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData(3, 7);
		decimalColumnStatsData.setHighValue(
				new Decimal(ByteBuffer.wrap(new BigInteger("123456").toByteArray()), (short) 3));
		decimalColumnStatsData.setLowValue(
				new Decimal(ByteBuffer.wrap(new BigInteger("123").toByteArray()), (short) 3));
		statsData = ColumnStatisticsData.decimalStats(decimalColumnStatsData);
		ColumnStatisticsObj colCStats = new ColumnStatisticsObj("c", "decimal", statsData);
		colStats.add(colCStats);

		LongColumnStatsData longColumnStats2 = new LongColumnStatsData(0, 8);
		longColumnStats2.setHighValue(
				11000 * DateTimeUtils.MILLIS_PER_DAY);
		longColumnStats2.setLowValue(
				1000 * DateTimeUtils.MILLIS_PER_DAY);
		statsData = ColumnStatisticsData.longStats(longColumnStats2);
		// See StatObjectConverter in hive
		ColumnStatisticsObj colDStats = new ColumnStatisticsObj("d", "timestamp", statsData);
		colStats.add(colDStats);

		LongColumnStatsData longColumnStats3 = new LongColumnStatsData(1, 8);
		longColumnStats3.setHighValue(
				11000 * DateTimeUtils.MILLIS_PER_DAY);
		longColumnStats3.setLowValue(
				1000 * DateTimeUtils.MILLIS_PER_DAY);
		statsData = ColumnStatisticsData.longStats(longColumnStats3);
		ColumnStatisticsObj colEStats = new ColumnStatisticsObj("e", "timestamp", statsData);
		colStats.add(colEStats);

		DateColumnStatsData dateColumnStatsData1 = new DateColumnStatsData(1, 8);
		dateColumnStatsData1.setHighValue(new Date(11000));
		dateColumnStatsData1.setLowValue(new Date(1000));
		statsData = ColumnStatisticsData.dateStats(dateColumnStatsData1);
		ColumnStatisticsObj colFStats = new ColumnStatisticsObj("f", "date", statsData);
		colStats.add(colFStats);

		DateColumnStatsData dateColumnStatsData2 = new DateColumnStatsData(1, 8);
		dateColumnStatsData2.setHighValue(new Date(11000));
		dateColumnStatsData2.setLowValue(new Date(1000));
		statsData = ColumnStatisticsData.dateStats(dateColumnStatsData2);
		ColumnStatisticsObj colGStats = new ColumnStatisticsObj("g", "date", statsData);
		colStats.add(colGStats);

		DateColumnStatsData dateColumnStatsData3 = new DateColumnStatsData(1, 8);
		dateColumnStatsData3.setHighValue(new Date(11000));
		dateColumnStatsData3.setLowValue(new Date(1000));
		statsData = ColumnStatisticsData.dateStats(dateColumnStatsData3);
		ColumnStatisticsObj colHStats = new ColumnStatisticsObj("h", "date", statsData);
		colStats.add(colHStats);

		ColumnStatistics statsObj = new ColumnStatistics(statsDesc, colStats);
		// Make sure hive RPC successes
		Assert.assertTrue(msc.updateTableColumnStatistics(statsObj));

		// Get from the meta and compare
		ExternalCatalogTable t = catalog.getTable("src");
		Assert.assertEquals(t.tableType(), "hive");
		Assert.assertTrue((new File("/tmp/hive/src")).exists());
		Assert.assertEquals(8, t.schema().getColumns().length);

		TableStats stats = t.stats();
		Assert.assertEquals(8L, stats.rowCount().longValue());
		Assert.assertEquals(8, stats.colStats().size());
		ColumnStats colA = stats.colStats().get("a");
		Assert.assertEquals(1, colA.nullCount().intValue());
		Assert.assertEquals(5, colA.ndv().intValue());
		Assert.assertTrue(colA.max() instanceof Integer);
		Assert.assertEquals(500, colA.max());
		Assert.assertTrue(colA.min() instanceof Integer);
		Assert.assertEquals(100, colA.min());
		ColumnStats colB = stats.colStats().get("b");
		Assert.assertEquals(2, colB.nullCount().intValue());
		Assert.assertEquals(6, colB.ndv().intValue());
		Assert.assertTrue(colB.max() instanceof java.sql.Date);
		Assert.assertEquals(new java.sql.Date(10000 * DateTimeUtils.MILLIS_PER_DAY), colB.max());
		Assert.assertTrue(colB.min() instanceof java.sql.Date);
		Assert.assertEquals(new java.sql.Date(20 * DateTimeUtils.MILLIS_PER_DAY), colB.min());
		ColumnStats colC = stats.colStats().get("c");
		Assert.assertEquals(3, colC.nullCount().intValue());
		Assert.assertEquals(7, colC.ndv().intValue());
		Assert.assertTrue(colC.max() instanceof BigDecimal);
		Assert.assertEquals("123.456", colC.max().toString());
		Assert.assertTrue(colC.min() instanceof BigDecimal);
		Assert.assertEquals("0.123", colC.min().toString());
		ColumnStats colD = stats.colStats().get("d");
		Assert.assertEquals(0, colD.nullCount().intValue());
		Assert.assertEquals(8, colD.ndv().intValue());
		Assert.assertTrue(colD.max() instanceof Time);
		Assert.assertEquals(950400000000L, ((Time) colD.max()).getTime());
		Assert.assertTrue(colD.min() instanceof Time);
		Assert.assertEquals(86400000000L, ((Time) colD.min()).getTime());
		ColumnStats colE = stats.colStats().get("e");
		Assert.assertEquals(1, colE.nullCount().intValue());
		Assert.assertEquals(8, colE.ndv().intValue());
		Assert.assertTrue(colE.max() instanceof Timestamp);
		Assert.assertEquals(950400000000L, ((Timestamp) colE.max()).getTime());
		Assert.assertTrue(colE.min() instanceof Timestamp);
		Assert.assertEquals(86400000000L, ((Timestamp) colE.min()).getTime());
		ColumnStats colF = stats.colStats().get("f");
		Assert.assertEquals(1, colF.nullCount().intValue());
		Assert.assertEquals(8, colF.ndv().intValue());
		Assert.assertTrue(colF.max() instanceof java.sql.Date);
		Assert.assertEquals(new java.sql.Date(950400000000L), colF.max());
		Assert.assertTrue(colF.min() instanceof java.sql.Date);
		Assert.assertEquals(new java.sql.Date(86400000000L), colF.min());
		ColumnStats colG = stats.colStats().get("g");
		Assert.assertEquals(1, colG.nullCount().intValue());
		Assert.assertEquals(8, colG.ndv().intValue());
		Assert.assertTrue(colG.max() instanceof Time);
		Assert.assertEquals(new Time(950400000000L), colG.max());
		Assert.assertTrue(colG.min() instanceof Time);
		Assert.assertEquals(new Time(86400000000L), colG.min());
		ColumnStats colH = stats.colStats().get("h");
		Assert.assertEquals(1, colH.nullCount().intValue());
		Assert.assertEquals(8, colH.ndv().intValue());
		Assert.assertTrue(colH.max() instanceof Timestamp);
		Assert.assertEquals(new Timestamp(950400000000L), colH.max());
		Assert.assertTrue(colH.min() instanceof Timestamp);
		Assert.assertEquals(new Timestamp(86400000000L), colH.min());
	}

	@Test
	public void testCreatePartition() {
		String tableName = "t1";
		catalog.createTable(
				tableName,
				createPartitionedTableInstance(),
				false);
		Assert.assertTrue(catalog.listPartitions(tableName).isEmpty());
		LinkedHashMap newPartitionSpec = new LinkedHashMap<String, String>();
		newPartitionSpec.put("hour", "12");
		newPartitionSpec.put("ds", "2016-02-01");
		ExternalCatalogTablePartition newPartition = new ExternalCatalogTablePartition(
				newPartitionSpec, null, null);
		catalog.createPartition(tableName, newPartition, false);
		List<LinkedHashMap<String, String>> partitionSpecs = catalog.listPartitions(tableName);
		Assert.assertEquals(partitionSpecs.size(), 1);
		Assert.assertEquals(partitionSpecs.get(0), newPartitionSpec);
	}

	@Test
	public void testGetPartition() {
		String tableName = "t1";
		catalog.createTable(
				tableName,
				createPartitionedTableInstance(),
				false);
		LinkedHashMap<String, String> newPartitionSpec = new LinkedHashMap<String, String>();
		newPartitionSpec.put("ds", "2016-02-01");
		newPartitionSpec.put("hour", "12");
		ExternalCatalogTablePartition newPartition =
				new ExternalCatalogTablePartition(newPartitionSpec, null, null);
		catalog.createPartition(tableName, newPartition, false);
		Assert.assertEquals(
				catalog.getPartition(tableName, newPartitionSpec).partitionSpec(),
				newPartition.partitionSpec());
	}

	@Test
	public void testListPartitionSpec() {
		String tableName = "t1";
		catalog.createTable(
				tableName,
				createPartitionedTableInstance(),
				false);
		Assert.assertTrue(catalog.listPartitions(tableName).isEmpty());
		LinkedHashMap<String, String> newPartitionSpec = new LinkedHashMap<String, String>();
		newPartitionSpec.put("ds", "2016-02-01");
		newPartitionSpec.put("hour", "12");
		ExternalCatalogTablePartition newPartition =
				new ExternalCatalogTablePartition(newPartitionSpec, null, null);
		catalog.createPartition(tableName, newPartition, false);
		List<LinkedHashMap<String, String>> partitionSpecs = catalog.listPartitions(tableName);
		Assert.assertEquals(partitionSpecs.size(), 1);
		Assert.assertEquals(partitionSpecs.get(0), newPartitionSpec);
	}

	@Test
	public void testAlterPartition() {
		String tableName = "t1";
		catalog.createTable(
				tableName,
				createPartitionedTableInstance(),
				false);
		LinkedHashMap<String, String> newPartitionSpec = new LinkedHashMap<String, String>();
		newPartitionSpec.put("ds", "2016-02-01");
		newPartitionSpec.put("hour", "12");
		Map<String, String> properties = new HashMap<>();
		properties.put("location", "/tmp/ds=2016-02-01/hour=12");
		ExternalCatalogTablePartition newPartition = new ExternalCatalogTablePartition(
				newPartitionSpec,
				properties,
				null);
		catalog.createPartition(tableName, newPartition, false);

		Map<String, String> properties1 = new HashMap<>();
		properties1.put("location", "/tmp1/ds=2016-02-01/hour=12");
		ExternalCatalogTablePartition updatedPartition = new ExternalCatalogTablePartition(
				newPartitionSpec,
				properties1,
				null);
		catalog.alterPartition(tableName, updatedPartition, false);
		ExternalCatalogTablePartition currentPartition = catalog.getPartition(tableName, newPartitionSpec);
		Assert.assertEquals(currentPartition.partitionSpec(), updatedPartition.partitionSpec());
		Assert.assertNotEquals(currentPartition, newPartition);
	}

	private ExternalCatalogTable createPartitionedTableInstance()  {

		String[] colNames = {"first", "second"};
		InternalType[] dataTypes = {DataTypes.STRING, DataTypes.INT};

		TableSchema schema = new TableSchema(colNames, dataTypes);
		LinkedHashSet<String> partitions = new LinkedHashSet<>();
		partitions.add("ds");
		partitions.add("hour");
		return new ExternalCatalogTable(
				"hive",
				schema,
				null,
				null,
				"",
				partitions,
				true,
				System.currentTimeMillis(),
				System.currentTimeMillis());
	}

	@After
	public void drop() throws TException {
		IMetaStoreClient msc = ((HiveExternalCatalog) catalog).getMSC();
		msc.dropTable("default", "src");
		msc.dropTable("default", "t1");
	}

	@AfterClass
	public static void clean() throws IOException {
		FileUtils.deleteFileOrDirectory(new File("metastore_db"));
	}
}
