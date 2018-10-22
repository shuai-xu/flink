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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test Describe Table Or Column Command in Stream mode.
 */
@RunWith(value = Parameterized.class)
public class DescribeTableStreamTest extends TestStreamBase implements DescribeTableTestBase {

	public DescribeTableStreamTest(
		String sql, String[][] sqlResult, String fieldDelim, String recordDelim, String quoteCharacter) {
		super(sql, sqlResult, fieldDelim, recordDelim, quoteCharacter, false);
		this.initRows = INIT_ROWS;
		this.initRowsTypeInfo = ROW_TYPE_INFO;
	}

	@Parameterized.Parameters(name = "sql{index}: {0}")
	public static Collection<Object[]> data() {
		// ANALYZE SQL cannot run in stream mode now.
		return Arrays.asList(SQL_RESULTS_WITHOUT_ANALYZE);
	}

}
