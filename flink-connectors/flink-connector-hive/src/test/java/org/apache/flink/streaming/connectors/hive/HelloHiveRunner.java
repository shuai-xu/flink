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

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Copied test cases from HiveRunner to show how to use original hive runner.
 *
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HelloHiveRunner {
	@HiveSQL(files = {})
	private HiveShell shell;

	@Before
	public void setupSourceDatabase() {
		shell.execute("CREATE DATABASE source_db");
		shell.execute(new StringBuilder()
							.append("CREATE TABLE source_db.test_table (")
							.append("year STRING, value INT")
							.append(")")
							.toString());
	}

	@Before
	public void setupTargetDatabase() {
		shell.execute(Paths.get("src/test/resources/helloHiveRunner/create_max.sql"));
	}

	@Test
	public void testMaxValueByYear() {
		/*
		 * Insert some source data
		 */
		shell.insertInto("source_db", "test_table")
			.withColumns("year", "value")
			.addRow("2014", 3)
			.addRow("2014", 4)
			.addRow("2015", 2)
			.addRow("2015", 5)
			.commit();

		/*
		 * Execute the query
		 */
		shell.execute(Paths.get("src/test/resources/helloHiveRunner/calculate_max.sql"));

		/*
		 * Verify the result
		 */
		List<Object[]> result = shell.executeStatement("select * from my_schema.result");

		assertEquals(2, result.size());
		assertArrayEquals(new Object[]{"2014", 4}, result.get(0));
		assertArrayEquals(new Object[]{"2015", 5}, result.get(1));
	}
}
