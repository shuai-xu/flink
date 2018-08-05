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

package org.apache.flink.contrib.streaming.state;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RocksDBStatePrefixIterator} and {@link RocksDBStateRangeIterator}.
 */
public class RocksDBStateIteratorTest {

	private int groups = 128;

	private static DBOptions dbOptions;

	private static ColumnFamilyOptions columnOptions;

	@BeforeClass
	public static void setupOptions() {
		dbOptions = PredefinedOptions.DEFAULT.createDBOptions().setCreateIfMissing(true);
		columnOptions = PredefinedOptions.DEFAULT.createColumnOptions();
	}

	@AfterClass
	public static void disposeOptions() {
		if (dbOptions != null) {
			dbOptions.close();
		}
		if (columnOptions != null) {
			columnOptions.close();
		}
	}

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testConstructIterator() throws IOException, RocksDBException {
		try (RocksDBInstance dbInstance = new RocksDBInstance(
			dbOptions,
			columnOptions,
			temporaryFolder.newFolder().getAbsoluteFile())) {

			try {
				createRangeIterator(dbInstance, null, null);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			try {
				createRangeIterator(dbInstance, new byte[3], null);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			try {
				createRangeIterator(dbInstance, null, new byte[3]);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			try {
				createRangeIterator(dbInstance, new byte[5], new byte[3]);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			createRangeIterator(dbInstance, new byte[3], new byte[5]);

			createPrefixIterator(dbInstance, null);
		}
	}

	@Test
	public void testIteratorAPIs() throws IOException, RocksDBException {
		try (RocksDBInstance dbInstance = new RocksDBInstance(
			dbOptions,
			columnOptions,
			temporaryFolder.newFolder().getAbsoluteFile())) {

			loadRandomData(dbInstance);

			// test invalid prefix iterator.
			{
				byte[] invalidPrefixKey = new byte[]{(byte) (groups + 1)};
				Iterator<RocksDBEntry> prefixIterator = createPrefixIterator(dbInstance, invalidPrefixKey);
				testInvalidIteratorAPIs(prefixIterator);
			}

			// test valid prefix iterator.
			{
				Iterator<RocksDBEntry> prefixIterator = createPrefixIterator(dbInstance, null);
				testValidIteratorAPIs(prefixIterator);
			}

			// test invalid range iterator.
			{
				byte[] invalidStartKey = new byte[]{(byte) (groups + 1)};
				byte[] invalidEndKey = new byte[]{(byte) (groups + 4)};
				Iterator<RocksDBEntry> rangeIterator = createRangeIterator(dbInstance, invalidStartKey, invalidEndKey);
				testInvalidIteratorAPIs(rangeIterator);
			}

			// test valid range iterator.
			{
				byte[] validStartKey = new byte[]{(byte) 0};
				byte[] validEndKey = new byte[]{(byte) groups};
				Iterator<RocksDBEntry> rangeIterator = createRangeIterator(dbInstance, validStartKey, validEndKey);
				testValidIteratorAPIs(rangeIterator);
			}
		}
	}

	@Test
	public void testIteratorAccess() throws IOException, RocksDBException {
		try (RocksDBInstance dbInstance = new RocksDBInstance(
			dbOptions,
			columnOptions,
			temporaryFolder.newFolder().getAbsoluteFile())) {

			SortedMap<byte[], byte[]> expectedResult = loadRandomData(dbInstance);

			// test prefix iterator access.
			{
				int dataCount = 0;
				for (int i = 0; i < groups; i++) {
					byte[] fromKey = new byte[]{(byte) i};
					byte[] toKey = new byte[]{(byte) (i + 1)};

					Iterator<RocksDBEntry> prefixIterator = createPrefixIterator(dbInstance, fromKey);

					SortedMap<byte[], byte[]> subMap = expectedResult.subMap(fromKey, toKey);
					Iterator<Map.Entry<byte[], byte[]>> expectedIterator = subMap.entrySet().iterator();
					validateIterators(prefixIterator, expectedIterator);
					dataCount += subMap.size();
				}
				assertEquals(expectedResult.size(), dataCount);
			}

			// test range iterator access.
			{
				int dataCount = 0;
				int maxRangeStep = 5;
				int startGroup = 0;
				do {
					int toGroup = Math.min(startGroup + ThreadLocalRandom.current().nextInt(maxRangeStep) + 1, groups) + 1;

					byte[] startKey = new byte[]{(byte) startGroup};
					byte[] toKey = new byte[]{(byte) toGroup};

					Iterator<RocksDBEntry> rangeIterator = createRangeIterator(dbInstance, startKey, toKey);

					SortedMap<byte[], byte[]> subMap = expectedResult.subMap(startKey, toKey);
					Iterator<Map.Entry<byte[], byte[]>> expectedIterator = subMap.entrySet().iterator();
					validateIterators(rangeIterator, expectedIterator);
					dataCount += subMap.size();
					startGroup = toGroup;
				} while (startGroup < groups);
				assertEquals(expectedResult.size(), dataCount);
			}

			// test range iterator over all groups.
			{
				byte[] startKey = new byte[]{(byte) 0};
				byte[] toKey = new byte[]{(byte) groups};
				Iterator<RocksDBEntry> dbIterator = createRangeIterator(dbInstance, startKey, toKey);
				Iterator<Map.Entry<byte[], byte[]>> expectedIterator = expectedResult.entrySet().iterator();
				validateIteratorsWithRemoval(dbIterator, expectedIterator, true);
			}

			// test iterator over all groups with some elements already removed.
			{
				byte[] startKey = new byte[]{(byte) 0};
				byte[] toKey = new byte[]{(byte) groups};
				validateIterators(createRangeIterator(dbInstance, startKey, toKey), expectedResult.entrySet().iterator());
				validateIterators(createPrefixIterator(dbInstance, null), expectedResult.entrySet().iterator());
			}
		}
	}

	//--------------------------------------------------------------------------

	private SortedMap<byte[], byte[]> loadRandomData(RocksDBInstance dbInstance) {
		int maxSizeForEachGroup = 32;
		int valueBytesLength = 32;

		SortedMap<byte[], byte[]> result = new TreeMap<>(new Comparator<byte[]>() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				return RocksDBInstance.compare(o1, o2);
			}
		});

		int keyBytesLength = 16;
		byte[] keyBytes = new byte[keyBytesLength];
		byte[] valueBytes = new byte[valueBytesLength];
		byte[] firstGroupBytes = new byte[]{(byte) 0};

		dbInstance.put(firstGroupBytes, valueBytes);
		result.put(firstGroupBytes, valueBytes.clone());

		for (int i = 1; i < groups; i++) {
			int dataSize = ThreadLocalRandom.current().nextInt(maxSizeForEachGroup);

			if (dataSize >= 1) {
				byte[] groupBytes = new byte[]{(byte) i};
				ThreadLocalRandom.current().nextBytes(valueBytes);
				dbInstance.put(groupBytes, valueBytes);
				result.put(groupBytes, valueBytes.clone());
			}

			for (int j = 1; j < dataSize - 1; j++) {
				ThreadLocalRandom.current().nextBytes(keyBytes);
				keyBytes[0] = (byte) i;
				ThreadLocalRandom.current().nextBytes(valueBytes);
				dbInstance.put(keyBytes, valueBytes);
				result.put(keyBytes.clone(), valueBytes.clone());
			}

		}
		return result;
	}

	private Iterator<RocksDBEntry> createPrefixIterator(RocksDBInstance dbInstance, byte[] prefixKey) {
		return new RocksDBStatePrefixIterator<RocksDBEntry>(dbInstance, prefixKey) {
			@Override
			public RocksDBEntry next() {
				return getNextEntry();
			}
		};
	}

	private Iterator<RocksDBEntry> createRangeIterator(RocksDBInstance dbInstance, byte[] startKey, byte[] toKey) {
		return new RocksDBStateRangeIterator<RocksDBEntry>(dbInstance, startKey, toKey) {
			@Override
			public RocksDBEntry next() {
				return getNextEntry();
			}
		};
	}

	private void testInvalidIteratorAPIs(Iterator iterator) {
		assertFalse(iterator.hasNext());
		assertTrue(throwExpectedException(iterator, Iterator::remove) instanceof IllegalStateException);

		assertTrue(throwExpectedException(iterator, Iterator::next) instanceof NoSuchElementException);

		assertTrue(throwExpectedException(iterator, Iterator::remove) instanceof IllegalStateException);
	}

	private void testValidIteratorAPIs(Iterator iterator) {
		assertTrue(iterator.hasNext());
		assertTrue(throwExpectedException(iterator, Iterator::remove) instanceof IllegalStateException);

		assertNotNull(iterator.next());
		iterator.remove();
		assertTrue(throwExpectedException(iterator, Iterator::remove) instanceof IllegalStateException);
	}

	private RuntimeException throwExpectedException(Iterator iterator, Consumer<Iterator> consumer) {
		try {
			consumer.accept(iterator);
			fail();
		} catch (RuntimeException e) {
			return e;
		}
		return new RuntimeException("not catch any expected exceptions.");
	}

	private void validateIterators(Iterator<RocksDBEntry> dbIterator, Iterator<Map.Entry<byte[], byte[]>> expectedIterator) {
			validateIteratorsWithRemoval(dbIterator, expectedIterator, false);
	}

	private void validateIteratorsWithRemoval(Iterator<RocksDBEntry> dbIterator, Iterator<Map.Entry<byte[], byte[]>> expectedIterator, boolean removeElements) {
		while (dbIterator.hasNext()) {
			RocksDBEntry dbEntry = dbIterator.next();
			assertTrue(expectedIterator.hasNext());
			Map.Entry<byte[], byte[]> expectedEntry = expectedIterator.next();
			assertArrayEquals(expectedEntry.getKey(), dbEntry.getDBKey());
			assertArrayEquals(expectedEntry.getValue(), dbEntry.getDBValue());
			if (removeElements && ThreadLocalRandom.current().nextInt() % 3 == 0) {
				dbIterator.remove();
				expectedIterator.remove();
			}
		}
		assertFalse(expectedIterator.hasNext());
	}
}
