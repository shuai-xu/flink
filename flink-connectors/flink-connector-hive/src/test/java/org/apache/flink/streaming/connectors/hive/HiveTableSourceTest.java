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
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableProperties;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Test.
 * todo(terry.wg@alibaba-inc.com): complete e2e test case and test joining, projection and other case.
 */
@Ignore
public class HiveTableSourceTest {

	@Test
	public void testProgram() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_SINK_PARALLELISM(), 1);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 1);

		HiveTableFactory hiveTableFactory = new HiveTableFactory();
		RichTableSchema richTableSchema = new RichTableSchema(
				new String[]{"name", "value"}, new InternalType[]{StringType.INSTANCE, DoubleType.INSTANCE});
		TableProperties tableProperties = new TableProperties();
		tableProperties.setString("hive.metastore.uris", "thrift://localhost:9083");
		tableProperties.setString("tableName".toLowerCase(), "default.test");
		TableSource tableSource = hiveTableFactory.createTableSource("hive_test_source", richTableSchema,
																	tableProperties);
		tEnv.registerTableSource("s", tableSource);
		Table result = tEnv.sqlQuery("select * from s");
		result.print();
//		List<Row> results = scala.collection.JavaConversions.seqAsJavaList(result.collect());

	}

	@Test
	public void testProgram1() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_SINK_PARALLELISM(), 1);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 1);

		HiveTableFactory hiveTableFactory = new HiveTableFactory();
		RichTableSchema richTableSchema = new RichTableSchema(
				new String[]{"name", "value", "age"}, new InternalType[]{StringType.INSTANCE, DoubleType.INSTANCE,
				IntType.INSTANCE});
		TableProperties tableProperties = new TableProperties();
		tableProperties.setString("hive.metastore.uris", "thrift://localhost:9083");
		tableProperties.setString("tableName".toLowerCase(), "default.test1");
		TableSource tableSource = hiveTableFactory.createTableSource("hive_test_source", richTableSchema,
																	tableProperties);
		tEnv.registerTableSource("s", tableSource);
		Table result = tEnv.sqlQuery("select * from s");
		result.print();
//		List<Row> results = scala.collection.JavaConversions.seqAsJavaList(result.collect());

	}

	@Test
	public void testProgram2() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_SINK_PARALLELISM(), 1);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 1);

		HiveTableFactory hiveTableFactory = new HiveTableFactory();
		RichTableSchema richTableSchema = new RichTableSchema(
				new String[]{"name", "value", "age"}, new InternalType[]{StringType.INSTANCE, DoubleType.INSTANCE,
				IntType.INSTANCE});
		TableProperties tableProperties = new TableProperties();
		tableProperties.setString("hive.metastore.uris", "thrift://localhost:9083");
		tableProperties.setString("tableName".toLowerCase(), "default.test1_orc");
		TableSource tableSource = hiveTableFactory.createTableSource("hive_test_source", richTableSchema,
																	tableProperties);
		tEnv.registerTableSource("s", tableSource);
		Table result = tEnv.sqlQuery("select * from s");
		result.print();
//		List<Row> results = scala.collection.JavaConversions.seqAsJavaList(result.collect());

	}

	@Test
	public void testScanWithHcataLog() throws Exception {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);
		env.setParallelism(1);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_SINK_PARALLELISM(), 1);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 1);
		tEnv.registerCatalog("myHive", new HiveCatalog("myHive", "thrift://localhost:9083"));
		tEnv.setDefaultDatabase("myHive", "default");
//		Table table = tEnv.scan("myHive", "default", "test");
//		table.printSchema();
//		table.print();
		tEnv.sqlQuery("select * from test").print();
	}
}
