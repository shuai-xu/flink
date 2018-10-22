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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import com.alibaba.blink.launcher.factory.CollectionTableFactory;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

/**
 * Basic class to test job.
 */
public abstract class TestJobBase {

	public static final int DEFAULT_PREVIEW_CSV_NUM_FILES = 1;
	public static final int DEFAULT_PREVIEW_LIMIT = 10;

	protected TableEnvironment tEnv;
	protected ClassLoader currentClassLoader;

	protected String sql;
	protected String[][] sqlResult;
	protected String fieldDelim;
	protected String recordDelim;
	protected String quoteCharacter;

	protected Row[] initRows;
	protected RowTypeInfo initRowsTypeInfo;

	protected TestJobBase(
		String sql, String[][] sqlResult, String fieldDelim, String recordDelim, String quoteCharacter) {
		this.sql = sql;
		this.sqlResult = sqlResult;
		this.fieldDelim = fieldDelim;
		this.recordDelim = recordDelim;
		this.quoteCharacter = quoteCharacter;
	}

	@Before
	public void prepare() {
		currentClassLoader = JobLauncher.class.getClassLoader();
		tEnv = initTableEnvironment();
		List<Row> data = Arrays.asList(initRows);
		CollectionTableFactory.initData(initRowsTypeInfo, data);
	}

	public abstract TableEnvironment initTableEnvironment();

}
