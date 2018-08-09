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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateAccessTestBase;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertNotEquals;

/**
 * Unit tests to validate that internal states can be correctly accessed in
 * {@link GeminiInternalStateBackend}.
 */
@RunWith(Parameterized.class)
public class GeminiInternalStateAccessTest extends InternalStateAccessTestBase {

	@Parameterized.Parameters(name = "Configuration: snapshotType={0}, memoryType={1}, copyValue={2}")
	public static Collection<String[]> parameters() {
		return Arrays.asList(new String[][] {
			{"FULL", "HEAP", "false"},
			{"FULL", "HEAP", "true"}
		});
	}

	@Parameterized.Parameter
	public String snapshotType;

	@Parameterized.Parameter(1)
	public String memoryType;

	@Parameterized.Parameter(2)
	public String copyValue;

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
		configuration.setString(GeminiConfiguration.GEMINI_SNAPSHOT_TYPE, snapshotType);
		configuration.setString(GeminiConfiguration.GEMINI_MEMORY_TYPE, memoryType);
		configuration.setString(GeminiConfiguration.GEMINI_COPY_VALUE, copyValue);

		return configuration;
	}

	@Test
	public void testCopyValue() {
		if (copyValue == "false") {
			return;
		}

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test_copy_value")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", LongSerializer.INSTANCE)
				.getDescriptor();
		InternalState state = backend.getInternalState(descriptor);

		Set<Row> keySet = new HashSet<>();
		Random random = new Random();
		for (int i = 0; i < 1000; i++) {
			Row rowKey = Row.of(random.nextInt());
			long value = random.nextLong();
			Row rowValue = Row.of(value);
			state.put(rowKey, rowValue);
			keySet.add(rowKey);

			rowValue.setField(0, value + 1);
			Row oldValue = state.get(rowKey);
			assertNotEquals(rowValue, oldValue);
		}

		for (Row rowKey : keySet) {
			Row rowValue = state.get(rowKey);
			long value = (Long) rowValue.getField(0);
			rowValue.setField(0, value + 1);

			Row oldValue = state.get(rowKey);
			assertNotEquals(rowValue, oldValue);
		}
	}

}
