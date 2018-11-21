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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.plan.SqlParseException;

import org.apache.calcite.sql.SqlNode;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class SqlParserTest extends ParserTestBase {

	@Test
	public void testCreateTable() {
		check("CREATE TABLE sls_stream (\n" +
				"  a bigint, \n" +
				"  h varchar header, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  PRIMARY KEY (a, b), \n" +
				"  WATERMARK wk FOR a AS withOffset(b, 1000)\n" +
				") with (\n" +
				"  x = 'y', \n" +
				"  asd = 'data'\n" +
				")\n",
			"CREATE TABLE `sls_stream` (\n" +
				"  `a`  BIGINT,\n" +
				"  `h`  VARCHAR HEADER,\n" +
				"  `g` AS (2 * (`a` + 1)),\n" +
				"  `ts` AS `toTimestamp`(`b`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `b`  VARCHAR,\n" +
				"  `proc` AS `PROCTIME`(),\n" +
				"  PRIMARY KEY (`a`, `b`),\n" +
				"  WATERMARK `wk` FOR `a` AS `withOffset`(`b`, 1000)\n" +
				") WITH (\n" +
				"  `x` = 'y',\n" +
				"  `asd` = 'data'\n" +
				")");
	}

	@Test
	public void testCreateTableWithUk() {
		check("CREATE TABLE sls_stream (\n" +
				"  a bigint, \n" +
				"  h varchar header, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  UNIQUE (a, b), \n" +
				"  WATERMARK wk FOR a AS withOffset(b, 1000)\n" +
				") with (\n" +
				"  x = 'y', \n" +
				"  asd = 'data'\n" +
				")\n",
			"CREATE TABLE `sls_stream` (\n" +
				"  `a`  BIGINT,\n" +
				"  `h`  VARCHAR HEADER,\n" +
				"  `g` AS (2 * (`a` + 1)),\n" +
				"  `ts` AS `toTimestamp`(`b`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `b`  VARCHAR,\n" +
				"  `proc` AS `PROCTIME`(),\n" +
				"  UNIQUE (`a`, `b`),\n" +
				"  WATERMARK `wk` FOR `a` AS `withOffset`(`b`, 1000)\n" +
				") WITH (\n" +
				"  `x` = 'y',\n" +
				"  `asd` = 'data'\n" +
				")");
	}

	@Test
	public void testCreateTableWithIndex() {
		check("CREATE TABLE sls_stream (\n" +
				"  a bigint, \n" +
				"  h varchar header, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  INDEX(h),\n" +
				"  UNIQUE INDEX(a, b),\n" +
				"  WATERMARK wk FOR a AS withOffset(b, 1000)\n" +
				") with (\n" +
				"  x = 'y', \n" +
				"  asd = 'data'\n" +
				")\n",
			"CREATE TABLE `sls_stream` (\n" +
				"  `a`  BIGINT,\n" +
				"  `h`  VARCHAR HEADER,\n" +
				"  `g` AS (2 * (`a` + 1)),\n" +
				"  `ts` AS `toTimestamp`(`b`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `b`  VARCHAR,\n" +
				"  `proc` AS `PROCTIME`(),\n" +
				"  INDEX (`h`),\n" +
				"  UNIQUE INDEX (`a`, `b`),\n" +
				"  WATERMARK `wk` FOR `a` AS `withOffset`(`b`, 1000)\n" +
				") WITH (\n" +
				"  `x` = 'y',\n" +
				"  `asd` = 'data'\n" +
				")");
	}

	@Test
	public void testCreateTableWithPkAndUk() {
		check("CREATE TABLE sls_stream (\n" +
				"  a bigint, \n" +
				"  h varchar header, \n" +
				"  g as 2 * (a + 1), \n" +
				"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
				"  b varchar,\n" +
				"  c varchar,\n" +
				"  proc as PROCTIME(), \n" +
				"  PRIMARY KEY (a),\n" +
				"  UNIQUE (b, c), \n" +
				"  WATERMARK wk FOR a AS withOffset(b, 1000)\n" +
				") with (\n" +
				"  x = 'y', \n" +
				"  asd = 'data'\n" +
				")\n",
			"CREATE TABLE `sls_stream` (\n" +
				"  `a`  BIGINT,\n" +
				"  `h`  VARCHAR HEADER,\n" +
				"  `g` AS (2 * (`a` + 1)),\n" +
				"  `ts` AS `toTimestamp`(`b`, 'yyyy-MM-dd HH:mm:ss'),\n" +
				"  `b`  VARCHAR,\n" +
				"  `c`  VARCHAR,\n" +
				"  `proc` AS `PROCTIME`(),\n" +
				"  PRIMARY KEY (`a`),\n" +
				"  UNIQUE (`b`, `c`),\n" +
				"  WATERMARK `wk` FOR `a` AS `withOffset`(`b`, 1000)\n" +
				") WITH (\n" +
				"  `x` = 'y',\n" +
				"  `asd` = 'data'\n" +
				")");
	}

	@Test
	public void testInvalidComputedColumn() {
		sql("CREATE TABLE sls_stream (\n" +
			"  a bigint, \n" +
			"  b varchar,\n" +
			"  ^toTimestamp^(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
			")\n")
			.fails("(?s).*Encountered \"toTimestamp \\(\" at line 4, column 3.\n"
				+ "Was expecting one of:\n"
				+ "    <IDENTIFIER> \"CHARACTER\" ...\n"
				+ "    <IDENTIFIER> \"CHAR\" ...\n"
				+ ".*");
	}

	@Test
	public void testColumnSqlString() {

		String sql = "CREATE TABLE sls_stream (\n" +
			"  a bigint, \n" +
			"  f as a + 1, \n" +
			"  b varchar,\n" +
			"  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
			"  proc as PROCTIME(),\n" +
			"  c int,\n" +
			"  PRIMARY KEY (a, b) \n" +
			") with (\n" +
			"  x = 'y', \n" +
			"  asd = 'data'\n" +
			")\n";
		String expected = "`a`, (`a` + 1) AS `f`, `b`, `toTimestamp`(`b`, 'yyyy-MM-dd HH:mm:ss') " +
			"AS `ts`, `PROCTIME`() AS `proc`, `c`";
		sql(sql).node(new BaseMatcher<SqlNode>() {
			@Override
			public void describeTo(Description description) {
				description.appendText("test");
			}

			@Override
			public boolean matches(Object item) {
				if (item instanceof SqlCreateTable) {
					SqlCreateTable createTable = (SqlCreateTable) item;
					try {
						createTable.validate();
					} catch (SqlParseException e) {
						fail(e.getMessage());
					}
					assertEquals(expected, createTable.getColumnSqlString());
					return true;
				} else {
					return false;
				}
			}
		});
	}

	@Test
	public void testCreateFunction() {
		check("create function myudf as 'com.alibaba.blink.udf.MyUDF'",
			"CREATE FUNCTION `myudf` AS 'com.alibaba.blink.udf.MyUDF'");
	}

	@Test
	public void testAnalyzeTable() {
		check("ANALYZE TABLE sls_stream COMPUTE STATISTICS",
				"ANALYZE TABLE `sls_stream` COMPUTE STATISTICS");
	}

	@Test
	public void testAnalyzeTableWithColumns() {
		check("ANALYZE TABLE sls_stream COMPUTE STATISTICS FOR COLUMNS a, b, c",
				"ANALYZE TABLE `sls_stream` COMPUTE STATISTICS FOR COLUMNS `a`, `b`, `c`");
	}

	@Test
	public void testAnalyzeTableWithAllColumns() {
		check("ANALYZE TABLE sls_stream COMPUTE STATISTICS FOR COLUMNS",
				"ANALYZE TABLE `sls_stream` COMPUTE STATISTICS FOR COLUMNS");
	}

	@Test
	public void testDescribeTable() {
		check("describe emps", "DESCRIBE TABLE `emps`");
		check("describe s.emps", "DESCRIBE TABLE `s`.`emps`");
		check("describe db.c.s.emps", "DESCRIBE TABLE `db`.`c`.`s`.`emps`");
		check("describe emps col1", "DESCRIBE TABLE `emps` `col1`");
		// table keyword is OK
		check("describe table emps col1", "DESCRIBE TABLE `emps` `col1`");
	}

	@Test
	public void testDescribeExtendedTable() {
		check("describe extended emps",
				"DESCRIBE EXTENDED TABLE `emps`");
		check("describe extended s.emps",
				"DESCRIBE EXTENDED TABLE `s`.`emps`");
		check("describe extended db.c.s.emps",
				"DESCRIBE EXTENDED TABLE `db`.`c`.`s`.`emps`");
		check("describe extended emps col1",
				"DESCRIBE EXTENDED TABLE `emps` `col1`");
		check("describe extended table emps col1",
				"DESCRIBE EXTENDED TABLE `emps` `col1`");
	}

	@Test
	public void testDescribeFormattedTable() {
		check("describe formatted emps",
				"DESCRIBE FORMATTED TABLE `emps`");
		check("describe formatted s.emps",
				"DESCRIBE FORMATTED TABLE `s`.`emps`");
		check("describe formatted db.c.s.emps",
				"DESCRIBE FORMATTED TABLE `db`.`c`.`s`.`emps`");
		check("describe formatted emps col1",
				"DESCRIBE FORMATTED TABLE `emps` `col1`");
		check("describe formatted table emps col1",
				"DESCRIBE FORMATTED TABLE `emps` `col1`");
	}
}
