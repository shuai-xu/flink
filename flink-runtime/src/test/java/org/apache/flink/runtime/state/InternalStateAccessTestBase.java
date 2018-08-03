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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.ListMerger;
import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link InternalState}.
 */
public abstract class InternalStateAccessTestBase {

	protected InternalStateBackend backend;

	/**
	 * Creates a new state backend for testing.
	 *
	 * @return A new state backend for testing.
	 */
	protected abstract InternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader) throws Exception;

	@Before
	public void openStateBackend() throws Exception {
		GroupSet groups = getGroupsForSubtask(10, 1, 0);
		backend = createStateBackend(10, groups, ClassLoader.getSystemClassLoader());
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
		Random random = new Random();

		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addKeyColumn("key3", IntSerializer.INSTANCE)
				.addValueColumn("value1", FloatSerializer.INSTANCE)
				.addValueColumn("value2", StringSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		Map<Row, Row> pairs = new HashMap<>(10);
		for (int i = 0; i < 10; ++i) {
			int key1 = i;
			int key2 = random.nextInt(1000);
			int key3 = random.nextInt(1000);
			float value1 = random.nextFloat();
			String value2 = key1 + "-" + key2 + "-" + key3 + ": " + value1;

			pairs.put(Row.of(key1, key2, key3), Row.of(value1, value2));
		}

		// Validates that no pairs exist in the state before performing any
		// addition operation.
		for (Row key : pairs.keySet()) {
			assertNull(state.get(key));
		}

		// Adds the pairs into the state and validates that the values can be
		// correctly retrieved.
		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			state.put(pair.getKey(), pair.getValue());
		}

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			Row expectedValue = pair.getValue();
			Row value = state.get(pair.getKey());
			assertEquals(expectedValue, value);
		}

		Row nullValue = state.get(null);
		assertNull(nullValue);

		try {
			state.put(Row.of(1, 2, 3), null);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			HashMap<Row, Row> nullValuePairs = new HashMap<>();
			nullValuePairs.put(Row.of(1, 2, 3), null);
			state.putAll(nullValuePairs);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		// Removes some of the pairs from the state and validates that the
		// removed pairs do not exist in the state.
		Set<Row> removedKeys = new HashSet<>();

		int index = 0;
		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			if ((index++) % 3 == 0) {
				removedKeys.add(pair.getKey());
			}
		}

		removedKeys.add(Row.of(159, 546, 807));
		removedKeys.add(Row.of(45, 811, 217));

		pairs.keySet().removeAll(removedKeys);
		for (Row removedKey : removedKeys) {
			state.remove(removedKey);
		}

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			Row value = state.get(pair.getKey());
			if (removedKeys.contains(pair.getKey())) {
				assertNull(value);
			} else {
				assertEquals(pair.getValue(), value);
			}
		}

		pairs.keySet().removeAll(removedKeys);

		// Adds more pairs into the state and validates that the values of the
		// pairs can be correctly retrieved.
		Map<Row, Row> addedPairs = new HashMap<>(3);
		for (int i = 10; i < 13; ++i) {
			int key1 = i + 10;
			int key2 = random.nextInt(1000);
			int key3 = random.nextInt(1000);
			float value1 = random.nextFloat();
			String value2 = key1 + "-" + key2 + "-" + key3 + ": " + value1;

			addedPairs.put(Row.of(key1, key2, key3), Row.of(value1, value2));
		}

		pairs.putAll(addedPairs);
		state.putAll(addedPairs);

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			Row value = state.get(pair.getKey());
			assertEquals(pair.getValue(), value);
		}

		// Retrieves the values of some pairs and validates that they are equal
		// to the expected values.
		Set<Row> retrievedKeys = new HashSet<>();

		Map<Row, Row> retrievedPairs = state.getAll(null);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedKeys.add(Row.of(12345, 123, 197));
		retrievedPairs = state.getAll(retrievedKeys);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		index = 0;
		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			if ((index++) % 5 == 0) {
				retrievedKeys.add(pair.getKey());
			}
		}

		retrievedPairs = state.getAll(retrievedKeys);
		for (Row key : retrievedKeys) {
			Row expectedValue = pairs.get(key);
			Row actualValue = retrievedPairs.get(key);
			assertEquals(expectedValue, actualValue);
		}

		// Removes some pairs from the state and validates that removed pairs
		// do not exist in the state.
		removedKeys.clear();

		index = 0;
		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			if (index % 4 == 0) {
				removedKeys.add(pair.getKey());
			}

			index++;
		}

		removedKeys.add(Row.of(201, 25, 950));
		removedKeys.add(Row.of(914, 482, 178));

		state.removeAll(removedKeys);

		for (Row removedKey : removedKeys) {
			Row value = state.get(removedKey);
			assertNull(value);
		}

		// Removes all pairs from the state and validates that the state is
		// empty.
		state.removeAll(pairs.keySet());

		for (Row key : pairs.keySet()) {
			Row value = state.get(key);
			assertNull(value);
		}
	}

	@Test
	public void testKeyMerge() {
		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE)
				.addValueColumn("value1",
					new ListSerializer(IntSerializer.INSTANCE),
					new ListMerger<>())
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		// merge null key
		try {
			List<Integer> value = new ArrayList<>();
			value.add(1);
			state.merge(null, Row.of(value));
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		// merge null value
		try {
			state.merge(Row.of(1), null);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}


		for (int i = 0; i < 100; i++) {
			List<Integer> value = new ArrayList<>();
			value.add(i);
			state.put(Row.of(i), Row.of(value));
		}

		for (int i = 0; i < 100; i++) {
			Row internalValue = state.get(Row.of(i));
			assertNotNull(internalValue);
			List<Integer> value = new ArrayList<>();
			value.add(i);
			assertEquals(internalValue, Row.of(value));
		}

		for (int i = 0; i < 100; i++) {
			List<Integer> value = new ArrayList<>();
			value.add(i + 1);
			state.merge(Row.of(i), Row.of(value));
		}

		for (int i = 0; i < 100; i++) {
			Row actualValue = state.get(Row.of(i));
			assertNotNull(actualValue);
			ArrayList<Integer> value = new ArrayList<>();
			value.add(i);
			value.add(i + 1);
			assertEquals(actualValue, Row.of(value));
		}

		// If the state previously did not contains a row for the given key,
		// associate the given value with the key.
		for (int i = 100; i < 200; i++) {
			Row internalKey = Row.of(i);
			assertNull(state.get(internalKey));
			List<Integer> value = new ArrayList<>();
			value.add(i);
			state.merge(internalKey, Row.of(value));
		}

		for (int i = 100; i < 200; i++) {
			Row internalValue = state.get(Row.of(i));
			assertNotNull(internalValue);
			List<Integer> value = new ArrayList<>();
			value.add(i);
			assertEquals(internalValue, Row.of(value));
		}

		Map<Row, Row> pairsToMerge = new HashMap<>();
		for (int i = 100; i < 200; i++) {
			List<Integer> value = new ArrayList<>();
			value.add(i + 1);
			pairsToMerge.put(Row.of(i), Row.of(value));
		}

		state.mergeAll(pairsToMerge);

		for (int i = 100; i < 200; i++) {
			Row actualValue = state.get(Row.of(i));
			assertNotNull(actualValue);
			ArrayList<Integer> value = new ArrayList<>();
			value.add(i);
			value.add(i + 1);
			assertEquals(actualValue, Row.of(value));
		}

		// null key in mergeAll
		Map<Row, Row> nullKeyPairsToMerge = new HashMap();
		nullKeyPairsToMerge.put(null, Row.of(new ArrayList<Integer>()));
		try {
			state.mergeAll(nullKeyPairsToMerge);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		// null value in mergeAll
		Map<Row, Row> nullValuePairsToMerge = new HashMap();
		nullValuePairsToMerge.put(Row.of(1), null);
		try {
			state.mergeAll(nullValuePairsToMerge);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}
	}

	@Test
	public void testBoundAccess() {
		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE, new BytewiseComparator<>(IntSerializer.INSTANCE))
				.addKeyColumn("key2", IntSerializer.INSTANCE, new BytewiseComparator<>(IntSerializer.INSTANCE))
				.addKeyColumn("key3", IntSerializer.INSTANCE, new BytewiseComparator<>(IntSerializer.INSTANCE))
				.addValueColumn("value1", FloatSerializer.INSTANCE)
				.addValueColumn("value2", StringSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		// Adds pairs into the state and validates that all head iterators can
		// correctly iterate over all the pairs under given range.
		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1Map = populateState(state);

		Integer firstKey = k1Map.firstKey();
		Pair<Row, Row> firstPair = state.firstPair(null);
		assertEquals(firstKey, firstPair.getKey().getField(0));

		for (int k1 = 0; k1 < 20; k1++) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);

			Integer expectedFirstK2 = k2Map == null ? null : k2Map.firstKey();
			Pair<Row, Row> actualFirstPair = state.firstPair(Row.of(k1));
			Integer actualFirstK2 = actualFirstPair == null ? null : (Integer) actualFirstPair.getKey().getField(1);
			assertEquals(expectedFirstK2, actualFirstK2);

			Integer expectedLastK2 = k2Map == null ? null : k2Map.lastKey();
			Pair<Row, Row> actualLastPair = state.lastPair(Row.of(k1));
			Integer actualLastK2 = actualLastPair == null ? null : (Integer) actualLastPair.getKey().getField(1);
			assertEquals(expectedLastK2, actualLastK2);
		}

		for (int k1 = 0; k1 < 20; k1++) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);

			for (int k2 = 0; k2 <= k1; ++k2) {
				SortedMap<Integer, Row> k3Map = k2Map == null ? null : k2Map.get(k2);

				Integer expectedFirstKey = k3Map == null ? null : k3Map.firstKey();
				Pair<Row, Row> actualFirstPair = state.firstPair(Row.of(k1, k2));
				Integer actualFirstKey = actualFirstPair == null ? null : (Integer) actualFirstPair.getKey().getField(2);
				assertEquals(expectedFirstKey, actualFirstKey);

				Integer expectedLastKey = k3Map == null ? null : k3Map.lastKey();
				Pair<Row, Row> actualLastPair = state.lastPair(Row.of(k1, k2));
				Integer actualLastKey = actualLastPair == null ? null : (Integer) actualLastPair.getKey().getField(2);
				assertEquals(expectedLastKey, actualLastKey);
			}
		}
	}

	//--------------------------------------------------------------------------

	/**
	 * A help method to get the group for the subtask.
	 *
	 * @param maxParallelism The maximum parallelism of the subtask.
	 * @param parallelism The parallelism of the subtask.
	 * @param subtaskIndex The index of the subtask.
	 *
	 * @return The group for the subtask.
	 */
	private GroupSet getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		GroupRange groups = new GroupRange(0, maxParallelism);
		return GroupRangePartitioner.getPartitionRange(groups, parallelism, subtaskIndex);
	}

	/**
	 * A help method to populate a backend with some background states.
	 */
	private void prepareStateBackend() {

		Random random = new Random();

		InternalStateDescriptor descriptor1 =
			new InternalStateDescriptorBuilder("__test1")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value1", LongSerializer.INSTANCE)
				.addValueColumn("value2", StringSerializer.INSTANCE)
				.getDescriptor();
		InternalState state1 = backend.getInternalState(descriptor1);

		for (int i = 0; i < 100; ++i) {
			int key = random.nextInt(10000);
			long value1 = random.nextLong();
			String value2 = Long.toString(System.currentTimeMillis());
			state1.put(Row.of(key), Row.of(value1, value2));
		}

		InternalStateDescriptor descriptor2 =
			new InternalStateDescriptorBuilder("__test2")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", StringSerializer.INSTANCE)
				.getDescriptor();
		InternalState state2 = backend.getInternalState(descriptor2);

		for (int i = 0; i < 100; ++i) {
			int key = random.nextInt(10000);
			String value = Long.toString(System.currentTimeMillis());
			state2.put(Row.of(key), Row.of(value));
		}
	}

	/**
	 * Generates test data and fills it in the given state.
	 *
	 * @param state The state to be filled.
	 * @return The generated data for the given state.
	 */
	private static SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> populateState(
		InternalState state
	) {

		Random random = new Random();

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1Map = new TreeMap<>();

		for (int k1 = 0; k1 < 20; ++k1) {
			for (int k2 = 0; k2 < k1; ++k2) {
				for (int k3 = 0; k3 < k2; ++k3) {
					Row key = Row.of(k1, k2, k3);
					Row value = Row.of(random.nextFloat(), "value");

					SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.computeIfAbsent(k1, k -> new TreeMap<>());
					SortedMap<Integer, Row> k3Map = k2Map.computeIfAbsent(k2, k -> new TreeMap<>());
					k3Map.put(k3, value);

					state.put(key, value);
				}
			}
		}

		return k1Map;
	}
}
