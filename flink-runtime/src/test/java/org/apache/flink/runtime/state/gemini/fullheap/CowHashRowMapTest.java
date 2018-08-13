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

package org.apache.flink.runtime.state.gemini.fullheap;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.runtime.state.gemini.RowMap;
import org.apache.flink.runtime.state.gemini.RowMapSnapshot;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CowHashRowMap}.
 */
public class CowHashRowMapTest extends TestLogger {

	@Test
	public void testGetAddRemoveOperations() {

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", IntSerializer.INSTANCE)
				.getDescriptor();

		CowHashRowMap rowMap = new CowHashRowMap(
			mock(AbstractInternalStateBackend.class), descriptor);

		Row key1 = Row.of(1, 1);
		Row key2 = Row.of(2, 2);
		Row key3 = Row.of(3, 3);

		Row value1 = Row.of(4, 4);
		Row value2 = Row.of(5, 5);
		Row value3 = Row.of(6, 6);

		Assert.assertEquals(0, rowMap.size());

		Assert.assertNull(rowMap.get(key1));
		rowMap.put(key1, value1);
		Assert.assertEquals(value1, rowMap.get(key1));
		Assert.assertEquals(1, rowMap.size());

		Assert.assertNull(rowMap.get(key2));
		rowMap.put(key2, value2);
		Assert.assertEquals(value2, rowMap.get(key2));
		Assert.assertEquals(2, rowMap.size());

		Assert.assertNull(rowMap.get(key3));
		rowMap.put(key3, value3);
		Assert.assertEquals(value3, rowMap.get(key3));
		Assert.assertEquals(3, rowMap.size());

		rowMap.put(key1, value2);
		Assert.assertEquals(value2, rowMap.get(key1));
		Assert.assertEquals(3, rowMap.size());

		rowMap.put(key2, value3);
		Assert.assertEquals(value3, rowMap.get(key2));
		Assert.assertEquals(3, rowMap.size());

		rowMap.put(key3, value1);
		Assert.assertEquals(value1, rowMap.get(key3));
		Assert.assertEquals(3, rowMap.size());

		rowMap.remove(key1);
		Assert.assertNull(rowMap.get(key1));
		Assert.assertEquals(2, rowMap.size());

		rowMap.remove(key2);
		Assert.assertNull(rowMap.get(key2));
		Assert.assertEquals(1, rowMap.size());

		rowMap.remove(key3);
		Assert.assertNull(rowMap.get(key3));
		Assert.assertEquals(0, rowMap.size());
	}

	@Test
	public void testIncrementalRehash() {

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", IntSerializer.INSTANCE)
				.getDescriptor();

		CowHashRowMap rowMap = new CowHashRowMap(
			mock(AbstractInternalStateBackend.class), descriptor);

		int insert = 0;
		int remove = 0;
		while (!rowMap.isRehashing()) {
			rowMap.put(Row.of(insert), Row.of(insert, insert));
			++insert;
			if (insert % 8 == 0) {
				rowMap.remove(Row.of(remove));
				++remove;
			}
		}
		Assert.assertEquals(insert - remove, rowMap.size());

		while (rowMap.isRehashing()) {
			rowMap.put(Row.of(insert), Row.of(insert, insert));
			++insert;
			if (insert % 8 == 0) {
				rowMap.remove(Row.of(remove));
				++remove;
			}
		}
		Assert.assertEquals(insert - remove, rowMap.size());

		for (int i = 0; i < insert; ++i) {
			if (i < remove) {
				Assert.assertNull(rowMap.get(Row.of(i)));
			} else {
				Assert.assertEquals(Row.of(i, i), rowMap.get(Row.of(i)));
			}
		}
	}

	@Test
	public void testIteratorWhenRehashing() {

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", IntSerializer.INSTANCE)
				.getDescriptor();

		CowHashRowMap rowMap = new CowHashRowMap(
			mock(AbstractInternalStateBackend.class), descriptor);
		HashMap<Row, Row> referenceMap = new HashMap<>();

		{
			// test empty iterator
			Iterator<Pair<Row, Row>> iterator = rowMap.getIterator(null);

			assertFalse(iterator.hasNext());

			try {
				iterator.next();
				fail("Should throw exception with empty iterator");
			} catch (Exception e) {
				assertTrue(e instanceof NoSuchElementException);
			}

			try {
				iterator.remove();
				fail("Should throw exception with empty iterator");
			} catch (Exception e) {
				assertTrue(e instanceof IllegalStateException);
			}
		}

		// add data
		Random random = new Random();
		int i = 0;
		while (!rowMap.isRehashing()) {
			Row key = Row.of(i++);
			Row value = Row.of(random.nextInt(10000));
			rowMap.put(key, value);
			referenceMap.put(key, value);
		}
		for (int j = i + 100; i < j; i++) {
			Row key = Row.of(i);
			Row value = Row.of(random.nextInt(10000));
			rowMap.put(key, value);
			referenceMap.put(key, value);
		}

		{
			Iterator<Pair<Row, Row>> iterator = rowMap.getIterator(null);

			int numKVs = 0;
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row key = pair.getKey();
				Row actualValue = pair.getValue();
				Row expectedValue = referenceMap.get(key);
				assertEquals(actualValue, expectedValue);
				++numKVs;
			}
			assertEquals(numKVs, referenceMap.size());
		}

		{
			// 1. remove() must be called after next()
			// 2. remove() executed successfully
			Iterator<Pair<Row, Row>> iterator = rowMap.getIterator(null);

			int totalKVs = referenceMap.size();
			int numKVs = 0;
			HashMap<Row, Row> removedMap = new HashMap<>();
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row key = pair.getKey();
				Row actualValue = pair.getValue();
				Row expectedValue = referenceMap.get(key);
				assertEquals(actualValue, expectedValue);

				if (numKVs % 10 == 1) {
					iterator.remove();
					removedMap.put(key, actualValue);
					referenceMap.remove(key);
					if (numKVs % 20 == 1) {
						try {
							iterator.remove();
							fail("Should throw exception when remove() is called successively.");
						} catch (Exception e) {
							assertTrue(e instanceof IllegalStateException);
						}
					}
				}

				++numKVs;
			}

			assertEquals(numKVs, totalKVs);
			assertEquals(referenceMap.size(), rowMap.size());
			for (Row key : removedMap.keySet()) {
				assertNull(rowMap.get(key));
			}

			for (Map.Entry<Row, Row> entry : referenceMap.entrySet()) {
				Row key = entry.getKey();
				Row expectedValue = entry.getValue();
				Row actualValue = rowMap.get(key);
				assertEquals(actualValue, expectedValue);
			}
		}

		{
			Iterator<Pair<Row, Row>> iterator = rowMap.getIterator(null);
			while (iterator.hasNext()) {
				iterator.next();
				iterator.remove();
			}
			assertTrue(rowMap.size() == 0);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCopyOnWriteWithRandomModifications() throws Exception {

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", IntSerializer.INSTANCE)
				.getDescriptor();

		CowHashRowMap rowMap = new CowHashRowMap(
			mock(AbstractInternalStateBackend.class), descriptor);
		HashMap<Row, Row> referenceMap = new HashMap<>();

		final Random random = new Random(42);

		// holds snapshots from the map under test, and at most 2 snapshots exist at the same time.
		CowHashRowMapSnapshot[] snapshots = new CowHashRowMapSnapshot[2];

		// holds a reference snapshot from our reference map that we compare against
		HashMap<Row, Row>[] references = new HashMap[2];

		int snapshotCounter = 0;

		for (int i = 0; i < 10000000; ++i) {

			Row key = Row.of(random.nextInt(1000));
			Row value = Row.of(random.nextInt(1000000));
			int op = random.nextInt(3);

			if (op < 2) {
				rowMap.put(key, value);
				referenceMap.put(key, value);
			} else {
				rowMap.remove(key);
				referenceMap.remove(key);
			}

			if (i > 0 && i % 1000 == 0) {
				Assert.assertTrue(checkRowMap(rowMap, referenceMap));

				int idx = snapshotCounter & 1;
				if (snapshots[idx] != null) {
					Assert.assertTrue(checkSnapshot(snapshots[idx], references[idx]));
					snapshots[idx].releaseSnapshot();
				}

				snapshots[idx] = (CowHashRowMapSnapshot) rowMap.createSnapshot();
				references[idx] = new HashMap<>(referenceMap);
				++snapshotCounter;
			}
		}
	}

	@Test
	public void testSnapshotWithMultipleGroups() {
		// partitioner based the first Integer column.
		Partitioner<Row> partitioner = new IntRowPartitioner();

		AbstractInternalStateBackend stateBackend = mock(AbstractInternalStateBackend.class);

		InternalStateDescriptor descriptor = new InternalStateDescriptorBuilder("global")
			.addKeyColumn("key", IntSerializer.INSTANCE)
			.addValueColumn("value", IntSerializer.INSTANCE)
			.setPartitioner(new IntRowPartitioner())
			.getDescriptor();

		// total groups is 10,
		int totalGroup = 10;

		// row map stores keys in group 0-9
		GroupRange group1 = new GroupRange(0, 10);
		when(stateBackend.getNumGroups()).thenReturn(totalGroup);
		when(stateBackend.getGroups()).thenReturn(group1);
		CowHashRowMap rowMap1 = new CowHashRowMap(stateBackend, descriptor);
		validateGroupSnapshot(totalGroup, group1, rowMap1, partitioner);

		// row map stores no keys in group 0
		GroupRange group2 = new GroupRange(0, 1);
		when(stateBackend.getNumGroups()).thenReturn(totalGroup);
		when(stateBackend.getGroups()).thenReturn(group2);
		CowHashRowMap rowMap2 = new CowHashRowMap(stateBackend, descriptor);
		validateGroupSnapshot(totalGroup, group2, rowMap2, partitioner);

		// row map stores no keys in group 4-7
		GroupRange group3 = new GroupRange(4, 7);
		when(stateBackend.getNumGroups()).thenReturn(totalGroup);
		when(stateBackend.getGroups()).thenReturn(group3);
		CowHashRowMap rowMap3 = new CowHashRowMap(stateBackend, descriptor);
		validateGroupSnapshot(totalGroup, group3, rowMap3, partitioner);
	}

	@Test
	public void testIteratorWhenSnapshot() {
		// partitioner based the first Integer column.
		Partitioner<Row> partitioner = new IntRowPartitioner();
		// total groups is 10,
		int totalGroup = 10;

		AbstractInternalStateBackend stateBackend = mock(AbstractInternalStateBackend.class);
		// row map stores keys in group 0-9
		GroupRange group = new GroupRange(0, 10);
		when(stateBackend.getNumGroups()).thenReturn(totalGroup);
		when(stateBackend.getGroups()).thenReturn(group);

		InternalStateDescriptor descriptor =
			new InternalStateDescriptorBuilder("test")
				.addKeyColumn("key1", IntSerializer.INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addValueColumn("value", IntSerializer.INSTANCE)
				.getDescriptor();

		CowHashRowMap rowMap = new CowHashRowMap(stateBackend, descriptor);

		Map<Row, Row> referenceMap = new HashMap<>();

		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < 1000; i++) {
			Row key = Row.of(random.nextInt(), random.nextInt());
			Row value = Row.of(random.nextInt());
			rowMap.put(key, value);
			referenceMap.put(key, value);
		}
		assertEquals(referenceMap.size(), rowMap.size());

		checkRowMap(rowMap, referenceMap);

		CowHashRowMapSnapshot snapshot1 = (CowHashRowMapSnapshot) rowMap.createSnapshot();

		Iterator<Pair<Row, Row>>iterator = rowMap.getIterator(null);
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
		assertTrue(rowMap.isEmpty());
		for (Map.Entry<Row, Row> entry : referenceMap.entrySet()) {
			assertNull(rowMap.get(entry.getKey()));
		}
		assertFalse(rowMap.getIterator(null).hasNext());
		checkSnapshot(snapshot1, referenceMap);
		snapshot1.releaseSnapshot();

		referenceMap.clear();
		for (int i = 0; i < 10000; i++) {
			Row key = Row.of(random.nextInt(), random.nextInt());
			Row value = Row.of(random.nextInt());
			rowMap.put(key, value);
			referenceMap.put(key, value);
		}
		assertEquals(referenceMap.size(), rowMap.size());

		snapshot1 = (CowHashRowMapSnapshot) rowMap.createSnapshot();
		Map<Row, Row> refSnapshot1 = new HashMap<>(referenceMap);

		iterator = rowMap.getIterator(null);
		Set<Row> removedKey = new HashSet<>();
		Set<Row> setValueKey = new HashSet<>();
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			Row key = pair.getKey();

			boolean op = random.nextBoolean();
			if (op) {
				removedKey.add(key);
				referenceMap.remove(key);
				iterator.remove();
			} else {
				setValueKey.add(key);
				Row newValue = Row.of(random.nextInt());
				pair.setValue(newValue);
				referenceMap.put(key, newValue);
			}
		}

		checkRowMap(rowMap, referenceMap);
		for (Row key : removedKey) {
			assertNull(rowMap.get(key));
		}

		for (Row key : setValueKey) {
			assertEquals(rowMap.get(key), referenceMap.get(key));
		}
		checkSnapshot(snapshot1, refSnapshot1);

		for (int i = 0; i < 10000; i++) {
			Row key = Row.of(random.nextInt(), random.nextInt());
			Row value = Row.of(random.nextInt());
			rowMap.put(key, value);
			referenceMap.put(key, value);
		}
		assertEquals(referenceMap.size(), rowMap.size());
		checkRowMap(rowMap, referenceMap);

		CowHashRowMapSnapshot snapshot2 = (CowHashRowMapSnapshot) rowMap.createSnapshot();
		Map<Row, Row> refSnapshot2 = new HashMap<>(referenceMap);

		iterator = rowMap.getIterator(null);
		removedKey = new HashSet<>();
		setValueKey = new HashSet<>();
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			Row key = pair.getKey();

			boolean op = random.nextBoolean();
			if (op) {
				removedKey.add(key);
				referenceMap.remove(key);
				iterator.remove();
			} else {
				setValueKey.add(key);
				Row newValue = Row.of(random.nextInt());
				pair.setValue(newValue);
				referenceMap.put(key, newValue);
			}
		}
		checkRowMap(rowMap, referenceMap);

		for (Row key : removedKey) {
			assertNull(rowMap.get(key));
		}

		for (Row key : setValueKey) {
			assertEquals(rowMap.get(key), referenceMap.get(key));
		}
		checkSnapshot(snapshot1, refSnapshot1);
		checkSnapshot(snapshot2, refSnapshot2);

		rowMap.put(Row.of(1, 1), Row.of(2));
		iterator = rowMap.getIterator(null);
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			iterator.remove();
			try {
				pair.setValue(Row.of(1));
				fail("Should throw IllegalStateException");
			} catch (Exception e) {
				assertTrue(e instanceof IllegalStateException);
			}
		}

		snapshot1.releaseSnapshot();
		snapshot2.releaseSnapshot();
	}

	private boolean checkRowMap(CowHashRowMap rowMap, Map<Row, Row > reference) {
		if (rowMap.size() != reference.size()) {
			return false;
		}

		Iterator<Pair<Row, Row>> rowMapIter = rowMap.getIterator(null);
		while (rowMapIter.hasNext()) {
			Pair<Row, Row> pair = rowMapIter.next();
			Row referenceValue = reference.get(pair.getKey());
			if (!Objects.equals(pair.getValue(), referenceValue)) {
				return false;
			}
		}

		return true;
	}

	private boolean checkSnapshot(CowHashRowMapSnapshot snapshot, Map<Row, Row> reference) {
		if (snapshot.size() != reference.size()) {
			return false;
		}

		Map<Row, Row> data = convert(snapshot.getSnapshotTable(), snapshot.size());
		for (Map.Entry<Row, Row> entry : data.entrySet()) {
			Row value = entry.getValue();
			Row expectedValue = reference.get(entry.getKey());
			if (!Objects.equals(value, expectedValue)) {
				return false;
			}
		}

		return true;
	}

	private Map<Row, Row> convert(CowHashRowMap.CowHashRowMapEntry[] snapshot, int snapshotSize) {

		Map<Row, Row> result = new HashMap();
		for (CowHashRowMap.CowHashRowMapEntry entry : snapshot) {
			while (null != entry) {
				result.put(entry.getKey(), entry.getValue());
				entry = entry.next;
			}
		}
		Assert.assertEquals(result.size(), snapshotSize);
		return result;
	}

	private void validateGroupSnapshot(
		int totalGroup,
		GroupRange groups,
		RowMap rowMap,
		Partitioner partitioner
	) {
		RowMapSnapshot emptySnpashot = rowMap.createSnapshot();

		for (int i = 0; i < 10; i++) {
			assertFalse(emptySnpashot.groupIterator(i).hasNext());
		}

		emptySnpashot.releaseSnapshot();

		Random rand = new Random(System.currentTimeMillis());
		// group -> key row -> value row
		Map<Integer, Map<Row, Row>> reference = new HashMap<>();
		for (int i = 0; i < 100000; i++) {
			// must be non-negative.
			int key = rand.nextInt(1 << 30);
			int value = rand.nextInt();
			int partiton = partitioner.partition(Row.of(key), totalGroup);
			if (!groups.contains(partiton)) {
				continue;
			}
			Map<Row, Row> map = reference.get(partiton);
			if (map == null) {
				map = new HashMap<>();
				reference.put(partiton, map);
			}
			map.put(Row.of(key), Row.of(value));
			rowMap.put(Row.of(key), Row.of(value));
		}

		RowMapSnapshot snapshot = rowMap.createSnapshot();

		assertFalse(snapshot.groupIterator(totalGroup + 1).hasNext());

		for (int group : reference.keySet()) {
			Map<Row, Row> expectedGroup = reference.get(group);
			Iterator<Pair<Row, Row>> iterator = snapshot.groupIterator(group);
			Set<Row> visitedKey = new HashSet<>();
			while (iterator.hasNext()) {
				Pair<Row, Row> pair = iterator.next();
				Row expectedValue = expectedGroup.get(pair.getKey());
				assertNotNull(expectedValue);
				assertEquals(expectedValue, pair.getValue());
				assertFalse(visitedKey.contains(pair.getKey()));
				visitedKey.add(pair.getKey());
			}
			assertEquals(visitedKey.size(), expectedGroup.size());
		}

		snapshot.releaseSnapshot();
	}

	private static class IntRowPartitioner implements Partitioner<Row> {

		@Override
		public int partition(Row row, int numPartitions) {
			// the column must be non-negative.
			return ((int) row.getField(0)) % numPartitions;
		}
	}
}
