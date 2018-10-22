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

package com.alibaba.blink.launcher.tool;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.IntType;
import org.apache.flink.table.util.StreamTableTestUtil;
import org.apache.flink.table.util.TableTestBase;
import org.apache.flink.types.Row;

import com.alibaba.blink.launcher.factory.CollectionTableFactory;
import com.alibaba.blink.launcher.util.JobBuildHelper;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class JobBuildHelperTest extends TableTestBase {
	@Test
	public void testJobQueryWithDefaultColumn() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = new BatchTableEnvironment(env, new TableConfig());
		LinkedList<Row> data = new LinkedList<>();
		data.add(row(1, 2));
		data.add(row(1, 3));
		CollectionTableFactory.initData(
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "b"}),
			data);
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			tableEnv,
			"create table test with(" +
				"	type = 'collection');\n" +
				"create table test2(" +
				"a int," +
				"c int) with (" +
				"	type = 'collection');" +
				"insert into test2 select a, a+2 from test;\n");
		tableEnv.execute();

		LinkedList<Row> expected = new LinkedList<>();
		expected.add(row(1, 3));
		expected.add(row(1, 3));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	@Test
	public void testJobQueryWithComputedColumn() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = new BatchTableEnvironment(env, new TableConfig());
		LinkedList<Row> data = new LinkedList<>();
		data.add(row(1, 2));
		data.add(row(1, 3));
		CollectionTableFactory.initData(
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "b"}),
			data);
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			tableEnv,
			"create table test(" +
				"a int," +
				"b int," +
				"c as a + 1) with(" +
				"	type = 'collection');\n" +
				"create table test2(" +
				"a int," +
				"c int) with (" +
				"	type = 'collection');" +
				"insert into test2 select a,c from test;\n");
		tableEnv.execute();

		LinkedList<Row> expected = new LinkedList<>();
		expected.add(row(1, 2));
		expected.add(row(1, 2));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	/**
	 * Test proc time definition by verifying sql plan.
	 */
	@Test
	public void testJobQueryWithProcTime() throws Exception {
		StreamTableTestUtil util = streamTestUtil();
		LinkedList<Row> data = new LinkedList<>();
		data.add(row(1, 2));
		data.add(row(1, 3));
		CollectionTableFactory.initData(
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "b"}),
			data);
		CollectionTableFactory.emitIntervalMs = 2000;
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			util.tableEnv(),
			"create table test(" +
				"a int," +
				"b int," +
				"c as proctime()) with(" +
				"	type = 'collection');\n" +
				"create view test2 as" +
				" select a,sum(b) from test group by TUMBLE(c, interval '1' SECOND), a;\n");

		util.verifyPlan("select * from test2");
	}

	@Test
	public void testJobQueryWithUserDefinedProcTime() throws Exception {
		StreamTableTestUtil util = streamTestUtil();
		LinkedList<Row> data = new LinkedList<>();
		data.add(row(1, 2));
		data.add(row(1, 3));
		CollectionTableFactory.initData(
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "b"}),
			data);
		CollectionTableFactory.emitIntervalMs = 2000;
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			util.tableEnv(),
			"create table test(" +
				"a int," +
				"b int" +
				") with(" +
				"	type = 'collection');\n" +
				"create view test_with_rowtime as select " +
				"a," +
				"b," +
				"proctime() as c from test;\n" +
				"create view test2 as " +
				"select a, sum(b) from test_with_rowtime group by TUMBLE(c, interval '1' SECOND), a;\n");

		util.verifyPlan("select * from test2");
	}

	@Test
	public void testJobQueryWithDDLRowTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		LinkedList<Row> data = new LinkedList<>();
		data.add(row(1, 2000));
		data.add(row(1, 4000));
		data.add(row(1, 6000));
		CollectionTableFactory.initData(
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "b"}),
			data);
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			tableEnv,
			"create table test(" +
				"a int," +
				"b int," +
				"e as proctime()," +
				"c as to_timestamp(b)," +
				"watermark d for c as withOffset(c, 1000)) with(" +
				"	type = 'collection');\n" +
				"create table test2(" +
				"a int," +
				"c int) with (" +
				"	type = 'collection');" +
				"create view test3 as select a, b+1 as b, c from test;\n" +
				"insert into test2 select a,sum(b) from test3 group by TUMBLE(c, interval '5' " +
				"SECOND), a;\n");
		tableEnv.execute();
		// finial watermark is 5000, so window(0, 5000) is fired.
		LinkedList<Row> expected = new LinkedList<>();
		expected.add(row(1, 6002));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	/**
	 *
	 */
	public static class Parser extends TableFunction<Row> {

		public void eval(String content) {
			String[] fields = content.split(",", 2);
			collect(row(Integer.valueOf(fields[0]), Integer.valueOf(fields[1])));
		}

		@Override
		public DataType getResultType(Object[] arguments, Class[] argTypes) {
			return DataTypes.createRowType(
				new DataType[]{IntType.INSTANCE, IntType.INSTANCE},
				new String[]{"a", "f0"});
		}
	}

	/**
	 * Parser generates timestamp field.
	 */
	public static class ParserWithTimestamp extends TableFunction<Row> {

		public void eval(String content) {
			String[] fields = content.split(",", 2);
			collect(row(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]),
				new Timestamp(Integer.valueOf(fields[1]))));
		}

		@Override
		public DataType getResultType(Object[] arguments, Class[] argTypes) {
			return DataTypes.createRowType(
				new DataType[]{DataTypes.INT, DataTypes.INT, DataTypes.TIMESTAMP},
				new String[]{"a", "f0", "c"});
		}
	}

	@Test
	public void testJobQueryWithParser() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		LinkedList<String> data = new LinkedList<>();
		data.add("1,2000");
		data.add("3,4000");
		data.add("3,4000");
		data.add("5,6000");
		LinkedList<String> parameters = new LinkedList<>();
		parameters.add("f0");
		TableSourceParser parser = new TableSourceParser(new Parser(), parameters);
		CollectionTableFactory.initData(
			BasicTypeInfo.STRING_TYPE_INFO,
			parser,
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "f0"}),
			data);
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			tableEnv,
			"create table test(" +
				"a int," +
				"f0 int," +
				"e as proctime()," +
				"c as to_timestamp(f0)," +
				"watermark d for c as withOffset(c, 1000)) with(" +
				"	type = 'collection');\n" +
				"create table test2(" +
				"a int," +
				"c int) with (" +
				"	type = 'collection');" +
				"create view test3 as select a, f0+1 as b, c from test;\n" +
				"insert into test2 select a,sum(b) from test3 group by TUMBLE(c, interval '5' " +
				"SECOND), a;\n");
		tableEnv.execute();
		// final watermark is 5000, so window(0, 5000) is fired. we have two group: a=1 and a=3
		LinkedList<Row> expected = new LinkedList<>();
		expected.add(row(1, 2001));
		expected.add(row(3, 8002));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	@Test
	public void testJobQueryWithParserAndWaterMark() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		LinkedList<String> data  = new LinkedList<>();
		data.add("1,2000");
		data.add("3,4000");
		data.add("3,4000");
		data.add("5,6000");
		LinkedList<String> parameters = new LinkedList<>();
		parameters.add("f0");
		TableSourceParser parser = new TableSourceParser(new ParserWithTimestamp(), parameters);
		CollectionTableFactory.initData(
			BasicTypeInfo.STRING_TYPE_INFO,
			parser,
			new RowTypeInfo(
				new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
				new String[]{"a", "f0"}),
			data);
		JobBuildHelper.buildSqlJobByString(
			false,
			getClass().getClassLoader(),
			new Properties(),
			tableEnv,
			"create table test(" +
				"watermark d for c as withOffset(c, 1000)) with(" +
				"	type = 'collection');\n" +
				"create table test2(" +
				"a int," +
				"c int) with (" +
				"	type = 'collection');" +
				"create view test3 as select a, f0+1 as b, c from test;\n" +
				"insert into test2 select a,sum(b) from test3 group by TUMBLE(c, interval '5' " +
				"SECOND), a;\n");
		tableEnv.execute();
		// final watermark is 5000, so window(0, 5000) is fired. we have two group: a=1 and a=3
		LinkedList<Row> expected  = new LinkedList<>();
		expected.add(row(1, 2001));
		expected.add(row(3, 8002));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	static Row row(Object... args) {
		Row row = new Row(args.length);
		for (int i = 0; i < args.length; i++) {
			row.setField(i, args[i]);
		}
		return row;
	}
}
