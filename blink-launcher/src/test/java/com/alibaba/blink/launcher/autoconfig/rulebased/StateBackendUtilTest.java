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

package com.alibaba.blink.launcher.autoconfig.rulebased;

import com.alibaba.blink.launcher.ConfConstants;
import org.junit.Test;

import java.util.Properties;

import static com.alibaba.blink.launcher.autoconfig.rulebased.ResourceSettings.DEFAULT_STATE_MEM_INCREASE_RATIO;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link StateBackendUtil}.
 */
public class StateBackendUtilTest {

	@Test
	public void testGetTaskNativeMemory() {
		Properties properties = new Properties();
		assertEquals(
				0,
				StateBackendUtil.getOperatorNativeMemory(properties, false));

		int nativeMemSize = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB + ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;

		properties.setProperty(ConfConstants.STATE_BACKEND_TYPE, "rocksdb");
		assertEquals(nativeMemSize, StateBackendUtil.getOperatorNativeMemory(properties, true));

		properties.setProperty(ConfConstants.STATE_BACKEND_TYPE, "niagara");
		assertEquals((int) (DEFAULT_STATE_MEM_INCREASE_RATIO * nativeMemSize), StateBackendUtil.getOperatorNativeMemory(properties, true));
	}
}
