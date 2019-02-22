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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.utils.SqlJobUtil;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlJobUtilITCase {

	@Test
	public void testValidatedQueryStreamMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment streamEnv = TableEnvironment.getTableEnvironment(env);;

		List<Row> data = new ArrayList<>();
		data.add(Row.of(1, 1L, "Hi"));
		data.add(Row.of(2, 2L, "Hello"));
		data.add(Row.of(3, 2L, "Hello world"));

		TypeInformation<?>[] types = {
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};
		String[] names = {"a", "b", "c"};

		RowTypeInfo typeInfo = new RowTypeInfo(types, names);

		DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

		Table in = streamEnv.fromDataStream(ds, "a,b,c");
		streamEnv.registerTable("MyTable", in);

		String subQuery = "SELECT a FROM MyTable";

		String expected = "SELECT `MyTable`.`a`\nFROM `builtin`.`default`.`MyTable` AS `MyTable`";

		assertEquals(expected, SqlJobUtil.getValidatedSqlQuery(streamEnv, subQuery));
	}

	@Test
	public void testValidatedQueryBatchMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment batchEnv = TableEnvironment.getBatchTableEnvironment(env);

		List<Row> data = new ArrayList<>();
		data.add(Row.of(1, 1L, "Hi"));
		data.add(Row.of(2, 2L, "Hello"));
		data.add(Row.of(3, 2L, "Hello world"));

		TypeInformation<?>[] types = {
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO};
		String[] names = {"a", "b", "c"};

		RowTypeInfo typeInfo = new RowTypeInfo(types, names);

		DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

		Table in = batchEnv.fromBoundedStream(ds, "a,b,c");
		batchEnv.registerTable("MyTable", in);

		String subQuery = "SELECT a FROM MyTable";

		String expected = "SELECT `MyTable`.`a`\nFROM `builtin`.`default`.`MyTable` AS `MyTable`";

		assertEquals(expected, SqlJobUtil.getValidatedSqlQuery(batchEnv, subQuery));
	}
}
