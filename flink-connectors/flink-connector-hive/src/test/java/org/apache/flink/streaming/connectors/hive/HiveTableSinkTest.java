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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveTableSinkTest {

	@HiveSQL(files = {})
	private HiveShell hiveShell;

	@Before
	public void setupSourceDatabaseAndData() {
		hiveShell.execute("CREATE DATABASE dst_db");
		hiveShell.execute(new StringBuilder()
								.append("CREATE TABLE dst_db.abc_test (")
								.append("a INT, b INT, c STRING")
								.append(")")
								.toString());
	}

	private Table getSmall3TupleDataSet(BatchTableEnvironment env) {
		List<Tuple3<Integer, Integer, String>> data = new ArrayList();
		data.add(new Tuple3<>(1, 10, "Hi"));
		data.add(new Tuple3<>(2, 20, "Hello"));
		data.add(new Tuple3<>(3, 30, "Hello world!"));
		return env.fromCollection(data,
								TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>() {}),
								"a,b,c");
	}

	/**
	 * Temporarily ignore this case for.
	 * @throws Exception
	 */
	@Test
	public void testInsertIntoTable() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM, 1);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1);
		Table table = getSmall3TupleDataSet(tEnv);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://localhost:20101"));
		tEnv.setDefaultDatabase("myHive", "dst_db");
		table.insertInto("abc_test");
		tEnv.execute();
		List<String> res = hiveShell.executeQuery("select * from dst_db.abc_test");
		assertEquals("1\t10\tHi", res.get(0));
		assertEquals("2\t20\tHello", res.get(1));
		assertEquals("3\t30\tHello world!", res.get(2));
	}
}
