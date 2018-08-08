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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.runtime.state.InternalStateIteratorTestBase;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests to validate that internal states iterator can be correctly accessed in
 * {@link HeapInternalStateBackend}.
 */
public class HeapInternalStateIteratorTest extends InternalStateIteratorTestBase {

	@Override
	protected AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) {
		return new HeapInternalStateBackend(numberOfGroups, groups, userClassLoader, localRecoveryConfig);
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
}
