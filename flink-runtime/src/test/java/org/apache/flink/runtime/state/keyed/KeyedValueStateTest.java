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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupRangePartitioner;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link KeyedValueState}.
 */
public class KeyedValueStateTest {

	protected AbstractInternalStateBackend backend;

	@Before
	public void openStateBackend() throws Exception {
		backend = new HeapInternalStateBackend(
			10,
			getGroupsForSubtask(10, 1, 0),
			ClassLoader.getSystemClassLoader(),
			TestLocalRecoveryConfig.disabled(),
			null
		);

		backend.restore(null);
	}

	@After
	public void closeStateBackend() {
		if (backend != null) {
			backend.dispose();
		}
	}

	@Test
	public void testKeyAccess() {

		KeyedValueStateDescriptor<Integer, Float> descriptor =
			new KeyedValueStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		KeyedValueState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Float> pairs = new HashMap<>();
		pairs.put(0, null);
		for (int i = 1; i < 10; ++i) {
			float value = random.nextFloat();
			pairs.put(i, value);
		}

		// Validates that no pair exists in the state when the state is empty.
		for (Integer key : pairs.keySet()) {
			assertFalse(state.contains(key));
			assertNull(state.get(key));
			assertEquals(Float.MIN_VALUE, state.getOrDefault(key, Float.MIN_VALUE), 0);
		}

		// Adds some pairs into the state and validates that their values can
		// be correctly retrieved.
		for (Map.Entry<Integer, Float> pair : pairs.entrySet()) {
			state.put(pair.getKey(), pair.getValue());
		}

		assertFalse(state.contains(null));
		assertNull(state.get(null));

		for (Map.Entry<Integer, Float> pair : pairs.entrySet()) {
			assertTrue(state.contains(pair.getKey()));

			Float expectedValue = pair.getValue();
			assertEquals(expectedValue, state.get(pair.getKey()));
			assertEquals(expectedValue, state.getOrDefault(pair.getKey(), Float.MIN_VALUE));
		}

		// Removes some pairs from the state and validates that the pairs do
		// not exist in the state any more.
		Set<Integer> removedKeys = new HashSet<>();

		int index = 0;
		for (Integer key : pairs.keySet()) {
			if (key == 0 || index % key == 0) {
				removedKeys.add(key);
			}

			index++;
		}

		removedKeys.add(null);
		removedKeys.add(11111);

		for (Integer removedKey : removedKeys) {
			state.remove(removedKey);
		}

		for (Map.Entry<Integer, Float> pair : pairs.entrySet()) {
			Float value = state.get(pair.getKey());
			if (removedKeys.contains(pair.getKey())) {
				assertNull(value);
			} else {
				assertEquals(pair.getValue(), value);
			}
		}

		pairs.keySet().removeAll(removedKeys);

		// Adds more pairs into the state and validates that the values of the
		// pairs can be correctly retrieved.

		Map<Integer, Float> addedPairs = new HashMap<>();
		for (int i = 5; i < 15; ++i) {
			float value = random.nextFloat();
			addedPairs.put(i, value);
		}

		pairs.putAll(addedPairs);
		state.putAll(addedPairs);

		for (Map.Entry<Integer, Float> pair : pairs.entrySet()) {
			Float expectedValue = pair.getValue();
			Float actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves the values of some pairs and validates the correctness of
		// these values.
		Map<Integer, Float> retrievedPairs = state.getAll(null);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		Set<Integer> retrievedKeys = new HashSet<>();
		retrievedKeys.add(null);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedKeys.add(11111);
		retrievedPairs = state.getAll(retrievedKeys);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		index = 0;
		for (Integer key : pairs.keySet()) {
			if (index % 4 == 0) {
				retrievedKeys.add(key);
			}
			index++;
		}
		retrievedPairs = state.getAll(retrievedKeys);
		for (Integer retrievedKey : retrievedKeys) {
			Float expecedValue = pairs.get(retrievedKey);
			Float actualValue = retrievedPairs.get(retrievedKey);
			assertEquals(expecedValue, actualValue);
		}

		// Removes some pairs from the state and validates that they do not
		// exist in the state any more.
		removedKeys.clear();

		index = 0;
		for (Integer key : pairs.keySet()) {
			if (index % 5 == 0) {
				removedKeys.add(key);
			}
			index++;
		}

		removedKeys.add(111);
		removedKeys.add(null);

		state.removeAll(removedKeys);
		for (Integer removedKey : removedKeys) {
			assertFalse(state.contains(removedKey));
		}

		// Removes all pairs from the state and validates that no pair exists
		// in the state.
		state.removeAll(pairs.keySet());
		for (Map.Entry<Integer, Float> pair : pairs.entrySet()) {
			assertFalse(state.contains(pair.getKey()));
		}
	}

	@Test
	public void testGetAllRemoveAll() {
		KeyedValueStateDescriptor<Integer, Float> descriptor =
			new KeyedValueStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		KeyedValueState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Float> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			float value = random.nextFloat();
			pairs.put(i, value);
			state.put(i, value);
		}

		Map<Integer, Float> allState = state.getAll();
		assertEquals(pairs, allState);

		state.removeAll();
		allState = state.getAll();
		assertTrue(allState.isEmpty());
	}

	@Test
	public void testKeys() {
		KeyedValueStateDescriptor<Integer, Float> descriptor =
			new KeyedValueStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		KeyedValueState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Float> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			float value = random.nextFloat();
			pairs.put(i, value);
			state.put(i, value);
		}

		Set<Integer> expectedKeys = pairs.keySet();
		Iterable<Integer> keyIterable = state.keys();
		Set<Integer> acturalKeys = new HashSet<>();
		for (Integer k : keyIterable) {
			acturalKeys.add(k);
		}
		assertEquals(expectedKeys, acturalKeys);

		Iterator<Integer> iter1 = keyIterable.iterator();
		Iterator<Integer> iter2 = keyIterable.iterator();

		Set<Integer> keySet1 = new HashSet<>();
		Set<Integer> keySet2 = new HashSet<>();
		while (iter1.hasNext() && iter2.hasNext()) {
			Integer k1 = iter1.next();
			Integer k2 = iter2.next();
			assertEquals(k1, k2);

			keySet1.add(k1);
			keySet2.add(k2);
		}

		assertEquals(expectedKeys, keySet1);
		assertEquals(expectedKeys, keySet2);
	}

	private GroupSet getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		GroupRange groups = new GroupRange(0, maxParallelism);
		return GroupRangePartitioner.getPartitionRange(groups, parallelism, subtaskIndex);
	}
}
