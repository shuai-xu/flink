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

package org.apache.flink.sql.parser.node;

import org.apache.flink.sql.parser.plan.SqlParseException;
import org.apache.flink.sql.parser.util.SqlContextUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test convertion from SQL to tree nodes.
 */
public class SqlToTreeConverterTest {

	@Test
	public void testConvertAggregateWithoutGroupBy() throws SqlParseException {
		String sql = "CREATE TABLE MyTable (\n" +
					"  a VARCHAR,\n" +
					"  b VARCHAR,\n" +
					"  c VARCHAR) WITH (\n" +
					"  type = 'csv',\n" +
					"  path = '/path/to/file');\n" +
					"SELECT a, b, MAX(c) FROM MyTable;\n";
		String json = SqlContextUtils.getSqlTreeJson(sql);
		String expected =
			"{\n" +
			"  \"nodes\" : [ {\n" +
			"    \"id\" : 1,\n" +
			"    \"type\" : \"SOURCE\",\n" +
			"    \"name\" : \"MyTable\",\n" +
			"    \"pos\" : {\n" +
			"      \"line\" : 1,\n" +
			"      \"column\" : 1\n" +
			"    }\n" +
			"  }, {\n" +
			"    \"id\" : 2,\n" +
			"    \"type\" : \"GROUP\",\n" +
			"    \"pos\" : {\n" +
			"      \"line\" : 7,\n" +
			"      \"column\" : 14\n" +
			"    }\n" +
			"  } ],\n" +
			"  \"links\" : [ {\n" +
			"    \"source\" : 1,\n" +
			"    \"target\" : 2\n" +
			"  } ]\n" +
			"}";
		assertEquals(expected, json);
	}

}
