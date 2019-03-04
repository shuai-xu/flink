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

package org.apache.flink.table.temptable.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.ServiceInstance;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for {@link TableServiceUtil}.
 */
public class TableServiceUtilTest {

	@Test
	public void testAddServiceInstance() {
		ServiceInstance s1 = new ServiceInstance(0, "127.0.0.1", 20000);
		ServiceInstance s2 = new ServiceInstance(1, "127.0.0.1", 30000);
		Map<Integer, ServiceInstance> map = new HashMap<>();
		map.put(0, s1);
		map.put(1, s2);

		Configuration configuration = new Configuration();
		TableServiceUtil.injectTableServiceInstances(map, configuration);

		Map<Integer, ServiceInstance> deSerialized = TableServiceUtil.buildTableServiceInstance(configuration);

		Assert.assertTrue(deSerialized.size() == map.size());
		for (Map.Entry<Integer, ServiceInstance> entry : map.entrySet()) {
			Assert.assertEquals(entry.getValue(), deSerialized.get(entry.getKey()));
		}
	}
}
