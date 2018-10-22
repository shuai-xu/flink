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
import org.apache.flink.types.Row;

/**
 * Common info for test describe table or column.
 */
public interface DescribeTableTestBase {

	String DDL = "CREATE TABLE test(a int, b double, c varchar) with (type = 'collection');\n";

	String ANALYZE_SQL = "ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS; \n";

	Row[] INIT_ROWS = new Row[]{
			Row.of(1, 2D, "a1"),
			Row.of(1, 3.1D, "a2222"),
			Row.of(2, null, "a3")};

	RowTypeInfo ROW_TYPE_INFO = new RowTypeInfo(
			new TypeInformation[]{
					BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.DOUBLE_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO},
					new String[]{"a", "b", "c"});

	Object[][] SQL_RESULTS_WITHOUT_ANALYZE = new Object[][]{{
			DDL + "DESCRIBE test",
			new String[][]{
					{"column_name", "column_type", "is_nullable"},
					{"a", "INTEGER", "YES"},
					{"b", "DOUBLE", "YES"},
					{"c", "VARCHAR", "YES"}},
			",", "\n", "\""
	}, {
			DDL + "DESCRIBE EXTENDED test",
			new String[][]{
					{"column_name", "column_type", "is_nullable"},
					{"a", "INTEGER", "YES"},
					{"b", "DOUBLE", "YES"},
					{"c", "VARCHAR", "YES"},
					{"", "", ""},
					{"# Detailed Table Information", "", ""},
					{"table_name", "test", ""},
					{"row_count", "NULL", ""}},
			",", "\n", "\""
	}, {
			DDL + "DESCRIBE FORMATTED test",
			new String[][]{
					{"column_name", "column_type", "is_nullable"},
					{"a", "INTEGER", "YES"},
					{"b", "DOUBLE", "YES"},
					{"c", "VARCHAR", "YES"},
					{"", "", ""},
					{"# Detailed Table Information", "", ""},
					{"table_name", "test", ""},
					{"row_count", "NULL", ""}},
			",", "\n", "\""
	}, {
			DDL + "DESCRIBE test a",
			new String[][]{
					{"info_name", "info_value"},
					{"column_name", "a"},
					{"column_type", "INTEGER"},
					{"is_nullable", "YES"}},
			",", "\n", "\""
	}, {
			DDL + "DESCRIBE EXTENDED test a",
			new String[][]{
					{"info_name", "info_value"},
					{"column_name", "a"},
					{"column_type", "INTEGER"},
					{"is_nullable", "YES"},
					{"ndv", "NULL"},
					{"null_count", "NULL"},
					{"avg_len", "NULL"},
					{"max_len", "NULL"},
					{"max", "NULL"},
					{"min", "NULL"}},
			",", "\n", "\""
	}, {
			DDL + "DESCRIBE FORMATTED test a",
			new String[][]{
					{"info_name", "info_value"},
					{"column_name", "a"},
					{"column_type", "INTEGER"},
					{"is_nullable", "YES"},
					{"ndv", "NULL"},
					{"null_count", "NULL"},
					{"avg_len", "NULL"},
					{"max_len", "NULL"},
					{"max", "NULL"},
					{"min", "NULL"}},
			",", "\n", "\""
	}};

	Object[][] SQL_RESULTS_WITH_ANALYZE = new Object[][]{{
			DDL + ANALYZE_SQL + "DESCRIBE test",
			new String[][]{
					{"column_name", "column_type", "is_nullable"},
					{"a", "INTEGER", "YES"},
					{"b", "DOUBLE", "YES"},
					{"c", "VARCHAR", "YES"}},
			",", "\n", "\""
	}, {
			DDL + ANALYZE_SQL + "DESCRIBE EXTENDED test",
			new String[][]{
					{"column_name", "column_type", "is_nullable"},
					{"a", "INTEGER", "YES"},
					{"b", "DOUBLE", "YES"},
					{"c", "VARCHAR", "YES"},
					{"", "", ""},
					{"# Detailed Table Information", "", ""},
					{"table_name", "test", ""},
					{"row_count", "3", ""}},
			",", "\n", "\""
	}, {
			DDL + ANALYZE_SQL + "DESCRIBE FORMATTED test",
			new String[][]{
					{"column_name", "column_type", "is_nullable"},
					{"a", "INTEGER", "YES"},
					{"b", "DOUBLE", "YES"},
					{"c", "VARCHAR", "YES"},
					{"", "", ""},
					{"# Detailed Table Information", "", ""},
					{"table_name", "test", ""},
					{"row_count", "3", ""}},
			",", "\n", "\""
	}, {
			DDL + ANALYZE_SQL + "DESCRIBE test a",
			new String[][]{
					{"info_name", "info_value"},
					{"column_name", "a"},
					{"column_type", "INTEGER"},
					{"is_nullable", "YES"}},
			",", "\n", "\""
	}, {
			DDL + ANALYZE_SQL + "DESCRIBE EXTENDED test a",
			new String[][]{
					{"info_name", "info_value"},
					{"column_name", "a"},
					{"column_type", "INTEGER"},
					{"is_nullable", "YES"},
					{"ndv", "2"},
					{"null_count", "0"},
					{"avg_len", "4.0"},
					{"max_len", "4"},
					{"max", "2"},
					{"min", "1"}},
			",", "\n", "\""
	}, {
			DDL + ANALYZE_SQL + "DESCRIBE FORMATTED test a",
			new String[][]{
					{"info_name", "info_value"},
					{"column_name", "a"},
					{"column_type", "INTEGER"},
					{"is_nullable", "YES"},
					{"ndv", "2"},
					{"null_count", "0"},
					{"avg_len", "4.0"},
					{"max_len", "4"},
					{"max", "2"},
					{"min", "1"}},
			",", "\n", "\""
	}};

}
