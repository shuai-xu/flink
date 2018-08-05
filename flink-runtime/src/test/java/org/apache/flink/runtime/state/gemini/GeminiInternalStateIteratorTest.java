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

import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.runtime.state.InternalStateIteratorTestBase;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests to validate that internal states can be correctly accessed in
 * {@link GeminiInternalStateBackend}.
 */
@RunWith(Parameterized.class)
public class GeminiInternalStateIteratorTest extends InternalStateIteratorTestBase {

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
		ClassLoader userClassLoader) {
		Configuration configuration = getStateBackendConfiguration();
		return new GeminiInternalStateBackend(numberOfGroups, groups, userClassLoader, configuration);
	}

	private Configuration getStateBackendConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setString(GeminiConfiguration.GEMINI_SNAPSHOT_TYPE, snapshotType);
		configuration.setString(GeminiConfiguration.GEMINI_MEMORY_TYPE, memoryType);
		configuration.setString(GeminiConfiguration.GEMINI_COPY_VALUE, copyValue);

		return configuration;
	}

	@Test
	public void testIteratorSetValue() {
		Random random = new Random();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addKeyColumn("key3", IntSerializer.INSTANCE)
				.addValueColumn("value", FloatSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		// Generates test data
		Map<Row, Row> pairs = new HashMap<>();
		for (int k1 = 0; k1 < 10; ++k1) {
			for (int k2 = 0; k2 <= k1; ++k2) {
				for (int k3 = 0; k3 <= k2; ++k3) {

					Row key = Row.of(k1, k2, k3);
					Row value = Row.of(random.nextFloat());

					pairs.put(key, value);

					state.put(key, value);
				}
			}
		}

		// Validates that the iterators can correctly iterate over the pairs.
		{
			int numActualPairs = 0;
			Iterator<Pair<Row, Row>> iterator = state.iterator();

			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row expectedValue = pairs.get(pair.getKey());
				Row actualValue = pair.getValue();
				assertEquals(expectedValue, actualValue);

				numActualPairs++;
			}
			assertEquals(pairs.size(), numActualPairs);
		}

		// Change values
		{

			Iterator<Pair<Row, Row>> iterator = state.iterator();
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();

				Row newValue = Row.of(random.nextFloat());
				Row oldValue = pair.getValue();

				Row returnedValue = pair.setValue(newValue);
				pairs.put(pair.getKey(), newValue);

				assertEquals(returnedValue, oldValue);
				assertEquals(newValue, state.get(pair.getKey()));
			}
		}

		// Re-validates that the iterators can correctly iterate over all the pairs.
		{
			int numActualPairs = 0;
			Iterator<Pair<Row, Row>> iterator = state.iterator();

			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row expectedValue = pairs.get(pair.getKey());
				Row actualValue = pair.getValue();
				assertEquals(expectedValue, actualValue);

				numActualPairs++;
			}
			assertEquals(pairs.size(), numActualPairs);
		}

		// set null value
		{
			Iterator<Pair<Row, Row>> iterator = state.iterator();
			Pair<Row, Row> pair = iterator.next();
			try {
				pair.setValue(null);
				fail("Should throw NullPointerException");
			} catch (Exception e) {
				assertTrue(e instanceof NullPointerException);
			}
		}
	}

	@Test
	public void testCopyValue() {
		if (copyValue == "false") {
			return;
		}

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test_copy_value")
				.addKeyColumn("key", IntSerializer.INSTANCE, new NaturalComparator<>())
				.addValueColumn("value", LongSerializer.INSTANCE)
				.getDescriptor();
		InternalState state = backend.getInternalState(descriptor);

		Set<Integer> keySet = new HashSet<>();
		Random random = new Random();
		for (int i = 0; i < 1000; i++) {
			int key = random.nextInt();
			Row rowKey = Row.of(key);
			Row rowValue = Row.of(random.nextLong());
			state.put(rowKey, rowValue);
			keySet.add(key);
		}

		// prefixIterator
		{
			Iterator<Pair<Row, Row>> iterator = state.prefixIterator(null);
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row rowKey = pair.getKey();
				Row rowValue = pair.getValue();
				long value = (Long) rowValue.getField(0);

				rowValue.setField(0, value + 1);
				Row oldValue = state.get(rowKey);
				assertNotEquals(rowValue, oldValue);
			}
		}

		// subIterator
		{
			Iterator<Pair<Row, Row>> iterator = state.subIterator(null, null, null);
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row rowKey = pair.getKey();
				Row rowValue = pair.getValue();
				long value = (Long) rowValue.getField(0);

				rowValue.setField(0, value + 1);
				Row oldValue = state.get(rowKey);
				assertNotEquals(rowValue, oldValue);
			}
		}

		// first pair, last pair
		{
			int keyNumber = keySet.size();
			while (keyNumber > 0) {
				Pair<Row, Row> firstPair = state.firstPair(null);
				Row firstRowValue = firstPair.getValue();
				long firstValue = (Long) firstRowValue.getField(0);

				firstRowValue.setField(0, firstValue + 1);
				Pair<Row, Row> firstPair1 = state.firstPair(null);
				assertNotEquals(firstRowValue, firstPair1.getValue());

				firstPair.setValue(firstRowValue);
				firstRowValue.setField(0, firstValue + 2);
				Pair<Row, Row> firstPair2 = state.firstPair(null);
				assertNotEquals(firstRowValue, firstPair2.getValue());

				Pair<Row, Row> lastPair = state.lastPair(null);
				Row lastRowValue = lastPair.getValue();
				long lastValue = (Long) lastRowValue.getField(0);

				lastRowValue.setField(0, lastValue + 1);
				Pair<Row, Row> lastPair1 = state.lastPair(null);
				assertNotEquals(lastRowValue, lastPair1.getValue());

				lastPair.setValue(lastRowValue);
				lastRowValue.setField(0, lastValue + 2);
				Pair<Row, Row> lastPair2 = state.lastPair(null);
				assertNotEquals(lastRowValue, lastPair2.getValue());

				state.remove(firstPair.getKey());
				state.remove(lastPair.getKey());
				keyNumber -= 2;
			}
		}
	}

}
