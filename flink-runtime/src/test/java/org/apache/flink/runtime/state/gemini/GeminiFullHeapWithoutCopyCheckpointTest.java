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

package org.apache.flink.runtime.state.gemini;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalStateCheckpointTestBase;
import org.apache.flink.runtime.state.LocalRecoveryConfig;

/**
 * Unit tests to validate that internal states can be correctly saved and
 * restored in {@link GeminiInternalStateBackend} with the configuration of full heap
 * and no value copy.
 */
public class GeminiFullHeapWithoutCopyCheckpointTest extends InternalStateCheckpointTestBase {

	@Override
	protected AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) {
		Configuration configuration = getStateBackendConfiguration();
		return new GeminiInternalStateBackend(numberOfGroups, groups, userClassLoader, localRecoveryConfig, configuration, null);
	}

	private Configuration getStateBackendConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setString(GeminiConfiguration.GEMINI_SNAPSHOT_TYPE, "FULL");
		configuration.setString(GeminiConfiguration.GEMINI_MEMORY_TYPE, "HEAP");
		configuration.setString(GeminiConfiguration.GEMINI_COPY_VALUE, "false");

		return configuration;
	}

}
