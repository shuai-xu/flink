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
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import com.alibaba.blink.launcher.util.JobBuildHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * Base class for test job in stream mode.
 */
public abstract class TestStreamBase extends TestJobBase {

	protected boolean withRetract;

	protected TestStreamBase(
			String sql,
			String[][] sqlResult,
			String fieldDelim,
			String recordDelim,
			String quoteCharacter,
			boolean withRetract) {
		super(sql, sqlResult, fieldDelim, recordDelim, quoteCharacter);
		this.withRetract = withRetract;
	}

	public TableEnvironment initTableEnvironment() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		StreamQueryConfig queryConfig = new StreamQueryConfig();
		queryConfig.enableValuesSourceInput();
		tEnv.setQueryConfig(queryConfig);
		return tEnv;
	}

	@Test
	public void assertSqlResultEquals() throws Exception {
		File tmpFile = File.createTempFile("Sink", ".tmp");
		String path = tmpFile.toURI().toString();
		tmpFile.delete();
		Properties userParam = new Properties();
		JobBuildHelper.buildSqlJobByString(true,
				currentClassLoader,
				userParam,
				null,
				tEnv,
				sql,
				path,
				DEFAULT_PREVIEW_CSV_NUM_FILES,
				0, // limit does not take effect here, it only work for batch job
				fieldDelim,
				recordDelim,
				quoteCharacter,
				withRetract,
				withRetract); // use retract sink and merge result
		tEnv.execute();
		Assert.assertTrue(tmpFile.exists());

		List<String> expectedList = new LinkedList<>();
		List<String> actualList = new LinkedList<>();
		Scanner scanner = new Scanner(new File(tmpFile, "/result1.csv"));
		for (String[] expectLine : sqlResult) {
			Assert.assertTrue(scanner.hasNext());
			actualList.add(scanner.nextLine());
			expectedList.add(Joiner.on(fieldDelim).join(expectLine));
		}
		Assert.assertFalse(scanner.hasNext());

		Collections.sort(expectedList);
		Collections.sort(actualList);
		for (int i = 0; i < expectedList.size(); i++) {
			Assert.assertEquals(expectedList.get(i), actualList.get(i));
		}

		scanner.close();
		tmpFile.deleteOnExit();
	}
}
