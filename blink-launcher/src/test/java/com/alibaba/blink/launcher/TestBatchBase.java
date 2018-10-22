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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import com.alibaba.blink.launcher.util.JobBuildHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * Base class for test job in batch mode.
 */
public abstract class TestBatchBase extends TestJobBase {

	protected TestBatchBase(
		String sql, String[][] sqlResult, String fieldDelim, String recordDelim, String quoteCharacter) {
		super(sql, sqlResult, fieldDelim, recordDelim, quoteCharacter);
	}

	public TableEnvironment initTableEnvironment() {
		TableConfig conf = new TableConfig();
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
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		return TableEnvironment.getBatchTableEnvironment(env, conf);
	}

	@Test
	public void assertSqlResultEquals() throws Exception {
		File tmpFile = File.createTempFile("Sink", ".tmp");
		String path = tmpFile.toURI().toString();
		tmpFile.delete();
		Properties userParam = new Properties();
		JobBuildHelper.buildSqlJobByString(false,
				currentClassLoader,
				userParam,
				null,
				tEnv,
				sql,
				path,
				DEFAULT_PREVIEW_CSV_NUM_FILES,
				DEFAULT_PREVIEW_LIMIT,
				fieldDelim,
				recordDelim,
				quoteCharacter,
				false,
				false);
		tEnv.execute();
		Assert.assertTrue(tmpFile.exists());
		Scanner scanner = new Scanner(new File(tmpFile, "/result1.csv"));
		List<String> actual = new ArrayList<>();
		while (scanner.hasNext()) {
			actual.add(scanner.nextLine());
		}

		List<String> expect = new ArrayList<>();
		for (String[] expectLine : sqlResult) {
			expect.add(Joiner.on(fieldDelim).join(expectLine));
		}

		Collections.sort(actual);
		Collections.sort(expect);
		Assert.assertEquals(expect, actual);

		scanner.close();
		tmpFile.deleteOnExit();
	}

}
