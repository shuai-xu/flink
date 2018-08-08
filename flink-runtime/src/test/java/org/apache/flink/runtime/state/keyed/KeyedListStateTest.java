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

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupRangePartitioner;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link KeyedListState}.
 */
public class KeyedListStateTest {

	protected AbstractInternalStateBackend backend;

	@Before
	public void openStateBackend() throws Exception {
		backend = new HeapInternalStateBackend(
			10,
			getGroupsForSubtask(10, 1, 0),
			ClassLoader.getSystemClassLoader(),
			TestLocalRecoveryConfig.disabled()
		);

		backend.restore(null);
	}

	@After
	public void closeStateBackend() {
		if (backend != null) {
			backend.close();
		}
	}

	@Test
	public void testKeyAccess() {
		KeyedListStateDescriptor<Integer, Float> descriptor =
			new KeyedListStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		KeyedListState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, List<Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numElements = random.nextInt(19) + 1;

			List<Float> elements = new ArrayList<>(numElements);
			for (int j = 0; j < numElements; ++j) {
				Float element = random.nextFloat();
				elements.add(element);
			}

			pairs.put(i, elements);
		}

		// Validates that no list exists in the state when the state is empty.
		for (Integer key : pairs.keySet()) {
			assertFalse(state.contains(key));
			assertNull(state.get(key));
			assertEquals(Collections.emptyList(), state.getOrDefault(key, Collections.emptyList()));
		}

		// Adds some lists into the state and validates that they can be
		// correctly retrieved.
		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			List<Float> elements = pair.getValue();
			state.addAll(key, elements);
		}

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			assertTrue(state.contains(pair.getKey()));

			List<Float> expectedValue = pair.getValue();
			assertEquals(expectedValue, state.get(pair.getKey()));
			assertEquals(expectedValue, state.getOrDefault(pair.getKey(), Collections.emptyList()));
		}

		assertFalse(state.contains(null));
		assertNull(state.get(null));

		// Removes some lists from the state and validates that they do not
		// exist in the state any more.

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

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			List<Float> value = state.get(pair.getKey());
			if (removedKeys.contains(pair.getKey())) {
				assertNull(value);
			} else {
				assertEquals(pair.getValue(), value);
			}
		}

		pairs.keySet().removeAll(removedKeys);

		// Adds more elements into the state and validates that the values of the
		// pairs can be correctly retrieved.

		Map<Integer, List<Float>> addedKeyMap = new HashMap<>();

		for (int i = 5; i < 15; ++i) {
			int numAddedElements = random.nextInt(19) + 1;
			List<Float> addedElements = new ArrayList<>();
			for (int j = 0; j < numAddedElements; ++j) {
				addedElements.add(random.nextFloat());
			}

			addedKeyMap.put(i, addedElements);

			List<Float> elements = pairs.get(i);
			if (elements == null) {
				elements = new ArrayList<>();
				pairs.put(i, elements);
			}
			elements.addAll(addedElements);
		}

		state.addAll(addedKeyMap);

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			List<Float> expectedValue = pair.getValue();
			List<Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves the values of some pairs and validates the correctness of
		// these values.
		Set<Integer> retrievedKeys = null;
		Map<Integer, List<Float>> retrievedPairs = state.getAll(retrievedKeys);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedKeys = new HashSet<>();
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
			List<Float> expectedValue = pairs.get(retrievedKey);
			List<Float> actualValue = retrievedPairs.get(retrievedKey);
			assertEquals(expectedValue, actualValue);
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

		removedKeys.add(11111);
		removedKeys.add(null);

		state.removeAll(removedKeys);
		for (Integer removedKey : removedKeys) {
			assertFalse(state.contains(removedKey));
		}

		// Removes all pairs from the state and validates that no pair exists
		// in the state.
		state.removeAll(pairs.keySet());
		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			assertFalse(state.contains(pair.getKey()));
		}

		Integer noExistKey = 11111;
		Float value = 2.1F;
		state.remove(noExistKey, value);
	}

	@Test
	public void testElementAccess() {
		KeyedListStateDescriptor<Integer, Float> descriptor =
			new KeyedListStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		KeyedListState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, List<Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numElements = random.nextInt(19) + 1;

			List<Float> elements = new ArrayList<>(numElements);
			for (int j = 0; j < numElements; ++j) {
				Float element = random.nextFloat();

				int numDuplicates = random.nextInt(3) + 1;
				for (int k = 0; k < numDuplicates; ++k) {
					elements.add(element);
				}
			}

			pairs.put(i, elements);
		}

		// Adds some elements into the state and validates that they can be
		// correctly retrieved.
		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			List<Float> elements = pair.getValue();
			for (Float element : elements) {
				state.add(key, element);
			}
		}

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			assertTrue(state.contains(pair.getKey()));

			List<Float> expectedValue = pair.getValue();
			List<Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			List<Float> elements = pair.getValue();

			List<Float> removedElements = new ArrayList<>();
			int index = 0;
			for (Float element : elements) {
				if (index % 3 == 0) {
					assertTrue(state.remove(key, element));
					removedElements.add(element);
				}
				index++;
			}

			for (Float removedElement : removedElements) {
				elements.remove(removedElement);
			}

			if (elements.isEmpty()) {
				pair.setValue(null);
			}
		}

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			List<Float> expectedValue = pair.getValue();
			List<Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			List<Float> elements = pair.getValue();
			if (elements == null) {
				continue;
			}

			List<Float> removedElements = new ArrayList<>();
			int index = 0;
			for (Float element : elements) {
				if (index % 4 == 0) {
					removedElements.add(element);
				}
				index++;
			}

			assertTrue(state.removeAll(key, removedElements));

			elements.removeAll(removedElements);
			if (elements.isEmpty()) {
				pair.setValue(null);
			}
		}

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			List<Float> expectedValue = pair.getValue();
			List<Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		Map<Integer, List<Float>> removedMap = new HashMap<>();

		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			List<Float> elements = pair.getValue();
			if (elements == null) {
				continue;
			}

			List<Float> removedElements = new ArrayList<>();
			int index = 0;
			for (Float element : elements) {
				if (index % 5 == 0) {
					removedElements.add(element);
				}
				index++;
			}

			elements.removeAll(removedElements);
			if (elements.isEmpty()) {
				pair.setValue(null);
			}

			removedMap.put(key, removedElements);
		}

		removedMap.put(null, Collections.emptyList());

		state.removeAll(removedMap);
		for (Map.Entry<Integer, List<Float>> pair : pairs.entrySet()) {
			List<Float> expectedValue = pair.getValue();
			List<Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}
	}

	@Test
	public void testGetAllRemoveAll() {
		KeyedListStateDescriptor<Integer, Float> descriptor =
			new KeyedListStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		KeyedListState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Map<Integer, List<Float>> pairs = new HashMap<>();
		int keyNumber = 10;
		int leastElementPerKey = 1;
		populateState(pairs, state, keyNumber, leastElementPerKey);

		Map<Integer, List<Float>> allState = state.getAll();
		assertEquals(allState, pairs);

		state.removeAll();
		allState = state.getAll();
		assertTrue(allState.isEmpty());
	}

	@Test
	public void testKeys() {
		KeyedListStateDescriptor<Integer, Float> descriptor =
			new KeyedListStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		KeyedListState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Map<Integer, List<Float>> pairs = new HashMap<>();
		int keyNumber = 10;
		int leastElementPerKey = 1;
		populateState(pairs, state, keyNumber, leastElementPerKey);

		Iterable<Integer> keyIterable = state.keys();
		Set<Integer> actualKeys = new HashSet<>();
		for (Integer k : keyIterable) {
			actualKeys.add(k);
		}
		assertEquals(actualKeys, pairs.keySet());

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
		assertEquals(keySet1, pairs.keySet());
		assertEquals(keySet2, pairs.keySet());
	}

	@Test
	public void testPoolPeek() {
		KeyedListStateDescriptor<Integer, Float> descriptor =
			new KeyedListStateDescriptor<>("test", IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		KeyedListState<Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Map<Integer, List<Float>> pairs = new HashMap<>();
		int keyNumber = 10;
		int leastElementPerKey = 2;
		populateState(pairs, state, keyNumber, leastElementPerKey);

		// test state#peek.
		for (int i = 0; i < keyNumber; ++i) {
			Float expectedNumber = pairs.get(i).get(0);
			Float actualNumber = state.peek(i);
			assertEquals(expectedNumber, actualNumber);

			Float secondActualNumber = state.peek(i);
			// assert peek did not remove the element.
			assertEquals(expectedNumber, secondActualNumber);
		}
		assertNull(state.peek(keyNumber));

		// test state#poll.
		for (int i = 0; i < keyNumber; ++i) {
			Float expectedNumber = pairs.get(i).get(0);
			Float firstActualNumber = state.poll(i);
			assertEquals(expectedNumber, firstActualNumber);

			expectedNumber = pairs.get(i).get(1);
			Float secondActualNumber = state.peek(i);
			assertEquals(expectedNumber, secondActualNumber);
		}
		assertNull(state.poll(keyNumber));
	}

	@Test
	public void testPutPutAll() {
		KeyedListStateDescriptor<Integer, Long> descriptor =
			new KeyedListStateDescriptor<>("test", IntSerializer.INSTANCE,
				LongSerializer.INSTANCE);

		KeyedListState<Integer, Long> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		// state#put
		state.put(3, 4L);

		List<Long> actualList = state.get(3);
		assertEquals(1, actualList.size());
		assertTrue(4 == actualList.get(0));

		// state#putAll
		List<Long> addValue = new ArrayList<>();
		addValue.add(1L);
		addValue.add(2L);
		addValue.add(3L);
		state.putAll(3, addValue);

		actualList = state.get(3);
		assertEquals(addValue.size(), actualList.size());
		for (Long item : actualList) {
			assertTrue(addValue.contains(item));
		}

		// state#putAll
		Map<Integer, List<Long>> tooAdd = new HashedMap();

		for (int i = 0; i < 10; ++i) {
			List<Long> listValue = new ArrayList<>();
			for (long j = i; j < 10; ++j) {
				listValue.add(j);
			}
			tooAdd.put(i, listValue);
		}
		state.putAll(tooAdd);

		for (int i = 0; i < 10; ++i) {
			actualList = state.get(i);
			assertEquals(actualList.size() + i, 10);

			for (long j = i; j < 10; ++j) {
				assertTrue(actualList.contains(j));
			}
		}
	}

	private void populateState(Map<Integer, List<Float>> pairs,
							   KeyedListState<Integer, Float> state,
							   int keyNumber,
							   int leastElementPerKey) {
		Random random = new Random();
		for (int i = 0; i < keyNumber; ++i) {
			int numElements = random.nextInt(19) + leastElementPerKey;

			List<Float> elements = new ArrayList<>(numElements);
			for (int j = 0; j < numElements; ++j) {
				Float element = random.nextFloat();
				elements.add(element);
			}

			pairs.put(i, elements);
			state.addAll(i, elements);
		}
	}

	private GroupSet getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		GroupRange groups = new GroupRange(0, maxParallelism);
		return GroupRangePartitioner.getPartitionRange(groups, parallelism, subtaskIndex);
	}
}

