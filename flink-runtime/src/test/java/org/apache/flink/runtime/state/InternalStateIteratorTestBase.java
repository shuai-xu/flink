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

import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link InternalState}.
 */
public abstract class InternalStateIteratorTestBase {

	protected AbstractInternalStateBackend backend;

	/**
	 * Creates a new state backend for testing.
	 *
	 * @return A new state backend for testing.
	 */
	protected abstract AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) throws IOException;

	@Before
	public void openStateBackend() throws Exception {
		GroupSet groups = getGroupsForSubtask(10, 1, 0);
		backend = createStateBackend(10, groups, ClassLoader.getSystemClassLoader(), TestLocalRecoveryConfig.disabled());
	}

	@After
	public void closeStateBackend() {
		if (backend != null) {
			backend.dispose();
		}
	}

	@Test
	public void testInvalidIteratorArguments() throws IOException {
		AbstractInternalStateBackend backend = createStateBackend(
			10,
			getGroupsForSubtask(10, 2, 0),
			ClassLoader.getSystemClassLoader(),
			TestLocalRecoveryConfig.disabled());

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE, new NaturalComparator<>())
				.addKeyColumn("key3", IntSerializer.INSTANCE)
				.addValueColumn("value", FloatSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);

		// Validates the illegal arguments for prefix iterators.
		try {
			state.prefixIterator(Row.of(0, 0, 0));
			fail("Should throw exceptions because the number of the prefix " +
				"keys is equal to the number of key columns in the state.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// Validates the illegal arguments for head iterators.
		try {
			state.headIterator(Row.of(0, 0, 0), 0);
			fail("Should throw exceptions because the number of the prefix " +
				"keys is equal to the number of key columns in the state.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			state.headIterator(null, 0);
			fail("Should throw exceptions because the column to scan is not " +
				"ordered.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// Validates the illegal arguments for tail iterators.
		try {
			state.tailIterator(Row.of(0, 0, 0), 0);
			fail("Should throw exceptions because the number of the prefix " +
				"keys is equal to the number of key columns in the state.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			state.tailIterator(null, 0);
			fail("Should throw exceptions because the column to scan is not " +
				"ordered.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// Validates the illegal arguments for sub iterators.
		try {
			state.subIterator(Row.of(0, 0, 0), 0, 0);
			fail("Should throw exceptions because the number of the prefix " +
				"keys is equal to the number of key columns in the state.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			state.subIterator(null, 0, 0);
			fail("Should throw exceptions because the column to scan is not " +
				"ordered.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}
	}

	@Test
	public void testIterator() {
		Random random = new Random();

		prepareStateBackend();

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

		// Validates that all iterators are empty when the state is empty.
		Iterator<Pair<Row, Row>> emptyIterator = state.iterator();
		assertNotNull(emptyIterator);
		assertFalse(emptyIterator.hasNext());

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

		// Validates that the iterators can correctly iterate over the pairs in the state.
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

		// Removes some pairs from the state via iterators and validates that
		// the removed pairs do no exist in the state any more.
		{
			Iterator<Pair<Row, Row>> iterator = state.iterator();

			int index = 0;
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();

				if (index % 3 == 0) {
					iterator.remove();
					pairs.put(pair.getKey(), null);
				}

				index++;
			}

			for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
				Row expectedValue = pair.getValue();
				Row actualValue = state.get(pair.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}

		// Removes all the pairs from the state via iterators and validates that
		// the state is empty.
		{
			Iterator<Pair<Row, Row>> iterator = state.iterator();

			while (iterator.hasNext()) {
				iterator.next();
				iterator.remove();
			}

			for (Row key : pairs.keySet()) {
				Row value = state.get(key);
				assertNull(value);
			}
		}

		{
			Iterator<Pair<Row, Row>> iterator = state.iterator();
			assertFalse(iterator.hasNext());

			try {
				iterator.next();
				fail("Should throw exceptions because the iterator does not " +
					"point to any pair.");
			} catch (Exception e) {
				assertTrue(e instanceof NoSuchElementException);
			}
		}
	}

	@Test
	public void testPrefixIterator() {
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

		// Validates that all prefix iterators do not point at any pair when the
		// state is empty.
		{
			Iterator<Pair<Row, Row>> iterator = state.prefixIterator(null);
			assertNotNull(iterator);
			assertFalse(iterator.hasNext());
		}

		for (int k1 = 0; k1 < 20; k1++) {
			Iterator<Pair<Row, Row>> iterator = state.prefixIterator(Row.of(k1));
			assertNotNull(iterator);
			assertFalse(iterator.hasNext());
		}

		for (int k1 = 0; k1 < 20; ++k1) {
			for (int k2 = 0; k2 <= k1; ++k2) {
				Iterator<Pair<Row, Row>> iterator = state.prefixIterator(Row.of(k1, k2));
				assertNotNull(iterator);
				assertFalse(iterator.hasNext());
			}
		}

		// Adds pairs into the state and validates that all prefix iterators can
		// correctly iterate over all the pairs under given prefix keys.
		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1Map = populateState(state);

		Iterator<Pair<Row, Row>> k1Iterator = state.prefixIterator(null);
		validateFirstOrderIterator(k1Map, k1Iterator, false);

		for (int k1 = 0; k1 < 20; k1++) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);
			Iterator<Pair<Row, Row>> prefixIterator = state.prefixIterator(Row.of(k1));
			validateSecondOrderIterator(k1, k2Map, prefixIterator, false);
		}

		for (int k1 = 0; k1 < 20; k1++) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);

			for (int k2 = 0; k2 <= k1; k2++) {
				SortedMap<Integer, Row> k3Map = k2Map == null ? null : k2Map.get(k2);
				Iterator<Pair<Row, Row>> prefixIterator = state.prefixIterator(Row.of(k1, k2));
				validateThirdOrderIterator(k1, k2, k3Map, prefixIterator, false);
			}
		}

		// Removes some pairs from the state via first-order iterators.
		{
			for (int k1 = 0; k1 < 20; ++k1) {
				Iterator<Pair<Row, Row>> iterator = state.prefixIterator(Row.of(k1));

				int index = 0;
				while (iterator.hasNext()) {
					Row internalKey = iterator.next().getKey();
					if (k1 == 0 || index % k1 == 0) {
						iterator.remove();

						int k2 = (int) internalKey.getField(1);
						int k3 = (int) internalKey.getField(2);

						Map<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);
						Map<Integer, Row> k3Map = k2Map.get(k2);

						k3Map.remove(k3);
						if (k3Map.isEmpty()) {
							k2Map.remove(k2);
						}

						if (k2Map.isEmpty()) {
							k1Map.remove(k1);
						}
					}
					index++;
				}
			}

			Iterator<Pair<Row, Row>> iterator = state.iterator();
			validateFirstOrderIterator(k1Map, iterator, false);
		}

		// Removes all pairs from the state via second-order iterators.
		{
			for (int k1 = 0; k1 < 20; ++k1) {
				for (int k2 = 0; k2 <= k1; ++k2) {
					Iterator<Pair<Row, Row>> iterator = state.prefixIterator(Row.of(k1, k2));

					while (iterator.hasNext()) {
						iterator.next();
						iterator.remove();
					}
				}
			}

			Iterator<Pair<Row, Row>> iterator = state.iterator();
			validateFirstOrderIterator(null, iterator, false);
		}
	}

	@Test
	public void testBoundAccess() {
		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addKeyColumn("key3", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
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

	@Test
	public void testHeadIterator() {
		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addKeyColumn("key3", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addValueColumn("value1", FloatSerializer.INSTANCE)
				.addValueColumn("value2", StringSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		// Validates that all head iterators do not point at any pair when the
		// state is empty.
		for (int k1 = 0; k1 < 20; ++k1) {
			Iterator<Pair<Row, Row>> iterator1 = state.headIterator(null, 0);
			assertNotNull(iterator1);
			assertFalse(iterator1.hasNext());

			Iterator<Pair<Row, Row>> iterator2 = state.headIterator(null, 10);
			assertNotNull(iterator2);
			assertFalse(iterator2.hasNext());

			Iterator<Pair<Row, Row>> iterator3 = state.headIterator(null, 20);
			assertNotNull(iterator3);
			assertFalse(iterator3.hasNext());
		}

		for (int k1 = 0; k1 < 20; ++k1) {
			for (int k2 = 0; k2 <= k1; ++k2) {
				for (int k3 = 0; k3 <= k2; ++k3) {
					Iterator<Pair<Row, Row>> iterator1 = state.headIterator(Row.of(k1, k2), 0);
					assertNotNull(iterator1);
					assertFalse(iterator1.hasNext());

					Iterator<Pair<Row, Row>> iterator2 = state.headIterator(Row.of(k1, k2), k2 / 2);
					assertNotNull(iterator2);
					assertFalse(iterator2.hasNext());

					Iterator<Pair<Row, Row>> iterator3 = state.headIterator(Row.of(k1, k2), k2);
					assertNotNull(iterator3);
					assertFalse(iterator3.hasNext());
				}
			}
		}

		// Adds pairs into the state and validates that all head iterators can
		// correctly iterate over all the pairs under given range.
		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1Map = populateState(state);

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1HeadMap1 = k1Map.headMap(0);
		Iterator<Pair<Row, Row>> headIterator1 = state.headIterator(null, 0);
		validateFirstOrderIterator(k1HeadMap1, headIterator1, true);

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1HeadMap2 = k1Map.headMap(10);
		Iterator<Pair<Row, Row>> headIterator2 = state.headIterator(null, 10);
		validateFirstOrderIterator(k1HeadMap2, headIterator2, true);

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1HeadMap3 = k1Map.headMap(20);
		Iterator<Pair<Row, Row>> headIterator3 = state.headIterator(null, 20);
		validateFirstOrderIterator(k1HeadMap3, headIterator3, true);

		for (int k1 = 0; k1 < 20; k1++) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);

			for (int k2 = 0; k2 <= k1; k2++) {
				SortedMap<Integer, Row> k3Map = k2Map == null ? null : k2Map.get(k2);

				int bound1 = 0;
				SortedMap<Integer, Row> thirdOrderPairs1 =
					k3Map == null ? null : k3Map.headMap(bound1);
				Iterator<Pair<Row, Row>> thirdOrderIterator1 = state.headIterator(Row.of(k1, k2), bound1);
				validateThirdOrderIterator(k1, k2, thirdOrderPairs1, thirdOrderIterator1, true);

				int bound2 = k2 / 2;
				SortedMap<Integer, Row> thirdOrderPairs2 =
					k3Map == null ? null : k3Map.headMap(bound2);
				Iterator<Pair<Row, Row>> thirdOrderIterator2 = state.headIterator(Row.of(k1, k2), bound2);
				validateThirdOrderIterator(k1, k2, thirdOrderPairs2, thirdOrderIterator2, true);

				int bound3 = k2;
				SortedMap<Integer, Row> thirdOrderPairs3 =
					k3Map == null ? null : k3Map.headMap(bound3);
				Iterator<Pair<Row, Row>> thirdOrderIterator3 = state.headIterator(Row.of(k1, k2), bound3);
				validateThirdOrderIterator(k1, k2, thirdOrderPairs3, thirdOrderIterator3, true);
			}
		}
	}

	@Test
	public void testTailIterator() {
		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addKeyColumn("key3", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addValueColumn("value1", FloatSerializer.INSTANCE)
				.addValueColumn("value2", StringSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		// Validates that all head iterators do not point at any pair when the
		// state is empty.
		for (int k1 = 0; k1 < 20; ++k1) {
			Iterator<Pair<Row, Row>> iterator1 = state.tailIterator(null, 0);
			assertNotNull(iterator1);
			assertFalse(iterator1.hasNext());

			Iterator<Pair<Row, Row>> iterator2 = state.tailIterator(null, 10);
			assertNotNull(iterator2);
			assertFalse(iterator2.hasNext());

			Iterator<Pair<Row, Row>> iterator3 = state.tailIterator(null, 20);
			assertNotNull(iterator3);
			assertFalse(iterator3.hasNext());
		}

		for (int k1 = 0; k1 < 20; ++k1) {
			for (int k2 = 0; k2 <= k1; ++k2) {
				for (int k3 = 0; k3 <= k2; ++k3) {
					Iterator<Pair<Row, Row>> iterator1 = state.tailIterator(Row.of(k1, k2), 0);
					assertNotNull(iterator1);
					assertFalse(iterator1.hasNext());

					Iterator<Pair<Row, Row>> iterator2 = state.tailIterator(Row.of(k1, k2), k2 / 2);
					assertNotNull(iterator2);
					assertFalse(iterator2.hasNext());

					Iterator<Pair<Row, Row>> iterator3 = state.tailIterator(Row.of(k1, k2), k2);
					assertNotNull(iterator3);
					assertFalse(iterator3.hasNext());
				}
			}
		}

		// Adds pairs into the state and validates that all head iterators can
		// correctly iterate over all the pairs under given range.
		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1Map = populateState(state);

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1HeadMap1 = k1Map.tailMap(0);
		Iterator<Pair<Row, Row>> headIterator1 = state.tailIterator(null, 0);
		validateFirstOrderIterator(k1HeadMap1, headIterator1, true);

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1HeadMap2 = k1Map.tailMap(10);
		Iterator<Pair<Row, Row>> headIterator2 = state.tailIterator(null, 10);
		validateFirstOrderIterator(k1HeadMap2, headIterator2, true);

		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1HeadMap3 = k1Map.tailMap(20);
		Iterator<Pair<Row, Row>> headIterator3 = state.tailIterator(null, 20);
		validateFirstOrderIterator(k1HeadMap3, headIterator3, true);

		for (int k1 = 0; k1 < 20; k1++) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map = k1Map.get(k1);

			for (int k2 = 0; k2 <= k1; k2++) {
				SortedMap<Integer, Row> k3Map = k2Map == null ? null : k2Map.get(k2);

				int bound1 = 0;
				SortedMap<Integer, Row> thirdOrderPairs1 =
					k3Map == null ? null : k3Map.tailMap(bound1);
				Iterator<Pair<Row, Row>> thirdOrderIterator1 = state.tailIterator(Row.of(k1, k2), bound1);
				validateThirdOrderIterator(k1, k2, thirdOrderPairs1, thirdOrderIterator1, true);

				int bound2 = k2 / 2;
				SortedMap<Integer, Row> thirdOrderPairs2 =
					k3Map == null ? null : k3Map.tailMap(bound2);
				Iterator<Pair<Row, Row>> thirdOrderIterator2 = state.tailIterator(Row.of(k1, k2), bound2);
				validateThirdOrderIterator(k1, k2, thirdOrderPairs2, thirdOrderIterator2, true);

				int bound3 = k2;
				SortedMap<Integer, Row> thirdOrderPairs3 =
					k3Map == null ? null : k3Map.tailMap(bound3);
				Iterator<Pair<Row, Row>> thirdOrderIterator3 = state.tailIterator(Row.of(k1, k2), bound3);
				validateThirdOrderIterator(k1, k2, thirdOrderPairs3, thirdOrderIterator3, true);
			}
		}
	}

	@Test
	public void testSubIterator() {
		prepareStateBackend();

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addKeyColumn("key3", IntSerializer.INSTANCE, BytewiseComparator.INT_INSTANCE)
				.addValueColumn("value1", FloatSerializer.INSTANCE)
				.addValueColumn("value2", StringSerializer.INSTANCE)
				.getDescriptor();

		InternalState state = backend.getInternalState(descriptor);
		assertNotNull(state);
		assertEquals(descriptor, state.getDescriptor());

		// Validates that all head iterators do not point at any pair when the
		// state is empty.
		for (int k1 = 0; k1 < 20; ++k1) {
			Iterator<Pair<Row, Row>> iterator1 = state.subIterator(null, 0, 20);
			assertNotNull(iterator1);
			assertFalse(iterator1.hasNext());

			Iterator<Pair<Row, Row>> iterator2 = state.subIterator(null, 5, 15);
			assertNotNull(iterator2);
			assertFalse(iterator2.hasNext());

			Iterator<Pair<Row, Row>> iterator3 = state.subIterator(null, 10, 10);
			assertNotNull(iterator3);
			assertFalse(iterator3.hasNext());
		}

		for (int k1 = 0; k1 < 20; ++k1) {
			for (int k2 = 0; k2 <= k1; ++k2) {
				for (int k3 = 0; k3 <= k2; ++k3) {
					int lowerBound1 = 0;
					int upperBound1 = k2;
					Iterator<Pair<Row, Row>> iterator1 =
						state.subIterator(Row.of(k1, k2), lowerBound1, upperBound1);
					assertNotNull(iterator1);
					assertFalse(iterator1.hasNext());

					int lowerBound2 = k2 / 4;
					int upperBound2 = k2 * 3 / 4;
					Iterator<Pair<Row, Row>> iterator2 =
						state.subIterator(Row.of(k1, k2), lowerBound2, upperBound2);
					assertNotNull(iterator2);
					assertFalse(iterator2.hasNext());

					int bound3 = k2 / 2;
					Iterator<Pair<Row, Row>> iterator3 =
						state.subIterator(Row.of(k1, k2), bound3, bound3);
					assertNotNull(iterator3);
					assertFalse(iterator3.hasNext());
				}
			}
		}

		// Adds pairs into the state and validates that all head iterators can
		// correctly iterate over all the pairs under given range.
		SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> k1Map =
			populateState(state);

		{
			int lowerBound1 = 0;
			int upperBound1 = 20;
			SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> subK1Map1 =
				k1Map == null ? null : k1Map.subMap(lowerBound1, upperBound1);
			Iterator<Pair<Row, Row>> subIterator1 =
				state.subIterator(null, lowerBound1, upperBound1);
			validateFirstOrderIterator(subK1Map1, subIterator1, true);

			int lowerBound2 = 5;
			int upperBound2 = 15;
			SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> subK1Map2 =
				k1Map == null ? null : k1Map.subMap(lowerBound2, upperBound2);
			Iterator<Pair<Row, Row>> subIterator2 =
				state.subIterator(null, lowerBound2, upperBound2);
			validateFirstOrderIterator(subK1Map2, subIterator2, true);

			int bound3 = 10;
			SortedMap<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> subK1Map3 =
				k1Map == null ? null : k1Map.subMap(10, 10);
			Iterator<Pair<Row, Row>> subIterator3 =
				state.subIterator(null, bound3, bound3);
			validateFirstOrderIterator(subK1Map3, subIterator3, true);
		}

		for (int k1 = 0; k1 < 20; ++k1) {
			SortedMap<Integer, SortedMap<Integer, Row>> k2Map =
				k1Map == null ? null : k1Map.get(k1);

			for (int k2 = 0; k2 <= k1; ++k2) {
				SortedMap<Integer, Row> k3Map =
					k2Map == null ? null : k2Map.get(k2);

				int lowerBound1 = 0;
				int upperBound1 = k2;
				SortedMap<Integer, Row> subK3Map1 =
					k3Map == null ? null : k3Map.subMap(lowerBound1, upperBound1);
				Iterator<Pair<Row, Row>> subIterator1 =
					state.subIterator(Row.of(k1, k2), lowerBound1, upperBound1);
				validateThirdOrderIterator(k1, k2, subK3Map1, subIterator1, true);

				int lowerBound2 = k2 / 4;
				int upperBound2 = k2 * 3 / 4;
				SortedMap<Integer, Row> subK3Map2 =
					k3Map == null ? null : k3Map.subMap(lowerBound2, upperBound2);
				Iterator<Pair<Row, Row>> subIterator2 =
					state.subIterator(Row.of(k1, k2), lowerBound2, upperBound2);
				validateThirdOrderIterator(k1, k2, subK3Map2, subIterator2, true);

				int bound3 = k2 / 2;
				SortedMap<Integer, Row> subK3Map3 =
					k3Map == null ? null : k3Map.subMap(bound3, bound3);
				Iterator<Pair<Row, Row>> subIterator3 =
					state.subIterator(Row.of(k1, k2), bound3, bound3);
				validateThirdOrderIterator(k1, k2, subK3Map3, subIterator3, true);
			}
		}
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

	/**
	 * A helper method to validate that the iterator can correctly iterate over
	 * all the pairs in the given map.
	 *
	 * @param firstOrderPairs The pairs expected to be iterated over.
	 * @param iterator The iterator to be validated.
	 * @param isOrdered True if needed to validate the iterating ordering of the
	 *                  iterator.
	 */
	private static void validateFirstOrderIterator(
		Map<Integer, SortedMap<Integer, SortedMap<Integer, Row>>> firstOrderPairs,
		Iterator<Pair<Row, Row>> iterator,
		boolean isOrdered
	) {
		assertNotNull(iterator);

		if (firstOrderPairs == null || firstOrderPairs.isEmpty()) {
			assertFalse(iterator.hasNext());
			return;
		}

		int numActualPairs = 0;
		int lastK1 = Integer.MIN_VALUE;
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();

			Row actualKey = pair.getKey();

			int actualK1 = (int) actualKey.getField(0);
			Map<Integer, SortedMap<Integer, Row>> secondOrderPairs = firstOrderPairs.get(actualK1);
			assertNotNull(secondOrderPairs);

			if (isOrdered) {
				assertTrue(actualK1 >= lastK1);
				lastK1 = actualK1;
			}

			int actualK2 = (int) actualKey.getField(1);
			Map<Integer, Row> thirdOrderPairs = secondOrderPairs.get(actualK2);
			assertNotNull(thirdOrderPairs);

			int actualK3 = (int) actualKey.getField(2);
			Row actualValue = pair.getValue();
			Row expectedValue = thirdOrderPairs.get(actualK3);
			assertEquals(expectedValue, actualValue);

			numActualPairs++;
		}

		int numExpectedPairs = 0;
		for (Map<Integer, SortedMap<Integer, Row>> secondOrderPairs : firstOrderPairs.values()) {
			for (Map<Integer, Row> thirdOrderPairs: secondOrderPairs.values()) {
				numExpectedPairs += thirdOrderPairs.size();
			}
		}
		assertEquals(numExpectedPairs, numActualPairs);
	}

	/**
	 * A helper method to validate that the iterator can correctly iterate over
	 * all the pairs in the given map.
	 *
	 * @param expectedK1 The expected value of the first key column in the pairs
	 *                   iterated by the iterator.
	 * @param secondOrderPairs The pairs expected to be iterated over.
	 * @param iterator The iterator to be validated.
	 * @param isOrdered True if needed to validate the iterating ordering of the
	 *                  iterator.
	 */
	private static void validateSecondOrderIterator(
		int expectedK1,
		Map<Integer, SortedMap<Integer, Row>> secondOrderPairs,
		Iterator<Pair<Row, Row>> iterator,
		boolean isOrdered
	) {
		assertNotNull(iterator);

		if (secondOrderPairs == null || secondOrderPairs.isEmpty()) {
			assertFalse(iterator.hasNext());
			return;
		}

		int numActualPairs = 0;
		int lastK2 = Integer.MIN_VALUE;
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();

			Row actualKey = pair.getKey();

			int actualK1 = (int) actualKey.getField(0);
			assertEquals(expectedK1, actualK1);

			int actualK2 = (int) actualKey.getField(1);
			Map<Integer, Row> thirdOrderPairs = secondOrderPairs.get(actualK2);
			assertNotNull(thirdOrderPairs);

			if (isOrdered) {
				assertTrue(actualK2 >= lastK2);
				lastK2 = actualK2;
			}

			int actualK3 = (int) actualKey.getField(2);
			Row actualValue = pair.getValue();
			Row expectedValue = thirdOrderPairs.get(actualK3);
			assertEquals(expectedValue, actualValue);

			numActualPairs++;
		}

		int numExpectedPairs = 0;
		for (Map<Integer, Row> thirdOrderPairs : secondOrderPairs.values()) {
			numExpectedPairs += thirdOrderPairs.size();
		}
		assertEquals(numExpectedPairs, numActualPairs);
	}

	/**
	 * A helper method to validate that the iterator can correctly iterate over
	 * all the pairs in the given map.
	 *
	 * @param expectedK1 The expected value of the first key column in the pairs
	 *                   iterated by the iterator.
	 * @param expectedK2 The expected value of the second key column in the
	 *                   pairs iterated by the iterator.
	 * @param thirdOrderPairs The pairs expected to be iterated over.
	 * @param iterator The iterator to be validated.
	 * @param isOrdered True if needed to validate the iterating ordering of the
	 *                  iterator.
	 */
	private static void validateThirdOrderIterator(
		int expectedK1,
		int expectedK2,
		Map<Integer, Row> thirdOrderPairs,
		Iterator<Pair<Row, Row>> iterator,
		boolean isOrdered
	) {
		assertNotNull(iterator);

		if (thirdOrderPairs == null || thirdOrderPairs.isEmpty()) {
			assertFalse(iterator.hasNext());
			return;
		}

		int numActualPairs = 0;
		int lastK3 = Integer.MIN_VALUE;
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();

			Row actualKey = pair.getKey();

			int actualK1 = (int) actualKey.getField(0);
			assertEquals(expectedK1, actualK1);

			int actualK2 = (int) actualKey.getField(1);
			assertEquals(expectedK2, actualK2);

			int actualK3 = (int) actualKey.getField(2);
			Row actualValue = pair.getValue();
			Row expectedValue = thirdOrderPairs.get(actualK3);
			assertEquals(expectedValue, actualValue);

			if (isOrdered) {
				assertTrue(actualK3 >= lastK3);
				lastK3 = actualK3;
			}

			numActualPairs++;
		}

		assertEquals(thirdOrderPairs.size(), numActualPairs);
	}
}
