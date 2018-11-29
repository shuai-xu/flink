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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.utils.EnvironmentUtil;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * Test for Environment.
 */
public class EnvironmentTest {

	@Test
	public void testParsingCatalog() throws IOException {
		Environment env = EnvironmentUtil.getDefaultTestEnvironment();

		assertEquals(new HashSet<>(Arrays.asList("myhive", "myinmemory")), env.getCatalogs().keySet());
		assertEquals(
			new HashMap<String, String>() {{
				put("catalog.connector.hive.metastore.uris", "thrift://host1:10000,thrift://host2:10000");
				put("catalog.connector.hive.metastore.username", "flink");
				put("catalog.type", "hive");
			}},
			env.getCatalogs().get("myhive").getProperties());
	}
}
