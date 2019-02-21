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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import scala.collection.Seq;

import static org.junit.Assert.assertEquals;

/**
 * Test.
 */

@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveTableSourceTest {

	@HiveSQL(files = {})
	private HiveShell hiveShell;

	@Before
	public void setupSourceDatabaseAndData() {
		hiveShell.execute("CREATE DATABASE source_db");
		hiveShell.execute(new StringBuilder()
							.append("CREATE TABLE source_db.test_table (")
							.append("year STRING, value INT")
							.append(")")
							.toString());
		/*
		 * Insert some source data
		 */
		hiveShell.insertInto("source_db", "test_table")
				.withColumns("year", "value")
				.addRow("2014", 3)
				.addRow("2014", 4)
				.addRow("2015", 2)
				.addRow("2015", 5)
				.commit();
	}

	@Test
	public void testScanWithHcataLog() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://localhost:20101"));
		tEnv.setDefaultDatabase("myHive", "source_db");
		Seq<Row> rowSeqs = tEnv.sqlQuery("select * from test_table").collect();
		Row[] rows = new Row[rowSeqs.size()];
		rowSeqs.copyToArray(rows);
		assertEquals(4, rows.length);
		assertEquals("2014", rows[0].getField(0));
		assertEquals("2014", rows[1].getField(0));
		assertEquals("2015", rows[2].getField(0));
		assertEquals("2015", rows[3].getField(0));
		assertEquals(3, rows[0].getField(1));
		assertEquals(4, rows[1].getField(1));
		assertEquals(2, rows[2].getField(1));
		assertEquals(5, rows[3].getField(1));
	}

	/**
	 * todo: complete test to read from partition table.
	 * @throws Exception
	 */
	@Ignore
	public void testScanPartitionTable() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://"));
		tEnv.setDefaultDatabase("myHive", "default");
		tEnv.sqlQuery("select * from pt_area_products").print();
	}

	/**
	 * todo: complete test to read from partition table to verify pruning source function.
	 * @throws Exception
	 */
	@Ignore
	public void testScanPartitionTableWithPartitionPrune() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://"));
		tEnv.setDefaultDatabase("myHive", "default");
		tEnv.sqlQuery("select * from pt_area_products where ds = '2018-12-25'").print();
	}
}
