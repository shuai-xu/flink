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

package com.alibaba.blink.launcher;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.FiniteValueInterval;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.plan.stats.ValueInterval;
import org.apache.flink.table.util.TableTestBase;
import org.apache.flink.types.Row;

import com.alibaba.blink.launcher.factory.CollectionTableFactory;
import com.alibaba.blink.launcher.util.JobBuildHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

/**
 * Test Analyze Table Command.
 */
@RunWith(value = Parameterized.class)
public class AnalyzeTableTest extends TableTestBase {

	private TableConfig conf;
	private StreamExecutionEnvironment env;
	private BatchTableEnvironment tEnv;

	private static final Double DEFAULT_ROWCOUNT = 1E8;

	private static final String DDL =
			"create table test with(" +
			"	type = 'collection');\n" +
			"create table test2(" +
			"a int," +
			"b double," +
			"c varchar) with (" +
			"	type = 'collection');";

	private static final String QUERY = "insert into test2 select a, b, c from test where b > 2 and a < 3;";

	private String testcaseName;
	private String sql;
	private TableStats tableStats;

	public AnalyzeTableTest(String testcaseName, String sql, TableStats tableStats) {
		this.testcaseName = testcaseName;
		this.sql = sql;
		this.tableStats = tableStats;
	}

	@Before
	public void prepare() {
		conf = new TableConfig();
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_SORT_BUFFER_MEM(), 4);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_HASH_JOIN_TABLE_MEM(), 4);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM(), 4);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_SOURCE_MEM(), 4);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_SINK_MEM(), 4);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_DEFAULT_MEM(), 4);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM(), 1);
		conf.getParameters().setInteger(
				TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(), 1);
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = TableEnvironment.getBatchTableEnvironment(env, conf);
	}

	@Parameterized.Parameters(name = "sql{index}: {0}")
	public static Collection<Object[]> data() {
		ColumnStats col1Stats = new ColumnStats(2L, 0L, 4D, 4, 2, 1);
		ColumnStats col2Stats = new ColumnStats(2L, 1L, 8D, 8, 3.1D, 2.1D);
		ColumnStats col3Stats = new ColumnStats(3L, 0L, 3D, 5, "a3", "a1");
		TableStats simpleTableStats = new TableStats(3L, new HashMap());

		Map<String, ColumnStats> completeColumnStats = new HashMap<>();
		completeColumnStats.put("a", col1Stats);
		completeColumnStats.put("b", col2Stats);
		completeColumnStats.put("c", col3Stats);
		TableStats completeTableStats = new TableStats(3L, completeColumnStats);

		Map<String, ColumnStats> partialColumnStats = new HashMap<>();
		partialColumnStats.put("a", col1Stats);
		partialColumnStats.put("c", col3Stats);
		TableStats partialTableStats = new TableStats(3L, partialColumnStats);

		Object[][] sqlAndSqlResults = new Object[][]{{
				"testWithoutAnalyzeTable",
				DDL + QUERY,
				null
		}, {
				"testAnalyzeTable",
				DDL + "ANALYZE TABLE test COMPUTE STATISTICS; \n" + QUERY,
				simpleTableStats
		}, {
				"testAnalyzeTableWithAllColumns",
				DDL + "ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS; \n" + QUERY,
				completeTableStats
		}, {
				"testAnalyzeTableWithPartialColumns",
				DDL + "ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS a, c; \n" + QUERY,
				partialTableStats
		}};
		return Arrays.asList(sqlAndSqlResults);
	}

	@Test
	public void test() throws Exception {
		LinkedList<Row> data = new LinkedList<>();
		data.add(row(1, 2.1d, "a1"));
		data.add(row(1, 3.1d, "a2222"));
		data.add(row(2, null, "a3"));
		TypeInformation[] typeInformations = new TypeInformation[]{
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
		};
		CollectionTableFactory.initData(
				new RowTypeInfo(
						typeInformations,
						new String[]{"a", "b", "c"}),
				data);
		JobBuildHelper.buildSqlJobByString(
				false,
				getClass().getClassLoader(),
				new Properties(),
				tEnv,
				sql
		);
		tEnv.execute();

		FlinkRelMetadataQuery mq = FlinkRelMetadataQuery.instance();
		RelNode ts = tEnv.scan("test").getRelNode();

		Double expectRowCnt = tableStats == null ? DEFAULT_ROWCOUNT : tableStats.rowCount();
		Assert.assertEquals(expectRowCnt, mq.getRowCount(ts));

		validateColStats("a", 0, mq, ts, typeInformations);
		validateColStats("b", 1, mq, ts, typeInformations);
		validateColStats("c", 2, mq, ts, typeInformations);

	}

	private void validateColStats(String colName, int colIdx,  FlinkRelMetadataQuery mq, RelNode ts, TypeInformation[] typeInformations) {
		ColumnStats expectColStats = tableStats == null ? null : tableStats.colStats().get(colName);
		Double actualNdvOfCol = mq.getDistinctRowCount(ts, ImmutableBitSet.of(colIdx), null);
		Double actualNullCntOfCol = mq.getColumnNullCount(ts, colIdx);
		ValueInterval actualIntervalOfCol = mq.getColumnInterval(ts, colIdx);
		Double actualSizeOfCol = mq.getAverageColumnSizes(ts).get(colIdx);

		if (expectColStats == null) {
			Assert.assertNull(actualNdvOfCol);
			Assert.assertNull(actualNullCntOfCol);
			Assert.assertNull(actualIntervalOfCol);
			Double defaultColSize = null;
			if (typeInformations[colIdx].equals(BasicTypeInfo.INT_TYPE_INFO)) {
				defaultColSize = 4D;
			} else if (typeInformations[colIdx].equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
				defaultColSize = 8D;
			} else if (typeInformations[colIdx].equals(BasicTypeInfo.STRING_TYPE_INFO)) {
				defaultColSize = 12D;
			} else {
				throw new IllegalArgumentException("Unknown type " + typeInformations[colIdx].toString());
			}
			Assert.assertEquals(defaultColSize, actualSizeOfCol);
		} else {
			Assert.assertEquals(expectColStats.ndv(), Long.valueOf(actualNdvOfCol.longValue()));
			Assert.assertEquals(expectColStats.nullCount(), Long.valueOf(actualNullCntOfCol.longValue()));
			Assert.assertEquals(expectColStats.max(), ((FiniteValueInterval) actualIntervalOfCol).upper());
			Assert.assertEquals(expectColStats.min(), ((FiniteValueInterval) actualIntervalOfCol).lower());
			Assert.assertEquals(expectColStats.avgLen(), actualSizeOfCol);
		}
	}

	static Row row(Object... args) {
		Row row = new Row(args.length);
		for (int i = 0; i < args.length; i++) {
			row.setField(i, args[i]);
		}
		return row;
	}
}
