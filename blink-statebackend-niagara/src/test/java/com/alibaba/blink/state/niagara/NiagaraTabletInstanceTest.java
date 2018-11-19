/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.state.niagara;

import org.apache.flink.configuration.ConfigConstants;

import com.alibaba.niagara.AbstractNiagaraObject;
import com.alibaba.niagara.NiagaraException;
import com.alibaba.niagara.NiagaraIterator;
import com.alibaba.niagara.NiagaraJNI;
import com.alibaba.niagara.ReadOptions;
import com.alibaba.niagara.TabletOptions;
import com.alibaba.niagara.WriteOptions;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link NiagaraTabletInstance}.
 */
public class NiagaraTabletInstanceTest {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private int groups = 128;

	private static NiagaraDBInstance dbInstance;

	private static NiagaraConfiguration configuration = new NiagaraConfiguration();

	@BeforeClass
	public static void platformCheck() {
		// Check OS & Kernel version before run unit tests
		Assume.assumeTrue(System.getProperty("os.name").startsWith("Linux") && System.getProperty("os.version").contains("alios7"));

		// use working dir to init Niagara
		ensureNiagaraIsLoaded(new File(System.getProperty("user.dir")).getAbsolutePath());
		dbInstance = new NiagaraDBInstance(configuration);
	}

	@AfterClass
	public static void closeNiagara() throws Exception {
		if (dbInstance != null) {
			dbInstance.close();
		}
	}

	@Test
	public void testNativeObjectsClosed() throws IOException, NiagaraException {
		File tabletPath = new File(temporaryFolder.newFolder().toString());
		NiagaraTabletInstance tabletInstance = new NiagaraTabletInstance(
			dbInstance.getNiagaraJNI(),
			tabletPath,
			tabletPath.toString(),
			configuration
		);

		TabletOptions tabletOptions = (TabletOptions) Whitebox.getInternalState(tabletInstance, "tabletOptions");
		WriteOptions writeOptions = (WriteOptions) Whitebox.getInternalState(tabletInstance, "writeOptions");
		ReadOptions readOptions = (ReadOptions) Whitebox.getInternalState(tabletInstance, "readOptions");
		List<AbstractNiagaraObject> niagaraObjects = Arrays.asList(tabletOptions, writeOptions, readOptions);

		for (AbstractNiagaraObject niagaraObject : niagaraObjects) {
			assertNotNull(niagaraObject);
			assertTrue(niagaraObject.isOwningHandle());
		}

		tabletInstance.close();
		for (AbstractNiagaraObject niagaraObject : niagaraObjects) {
			// The C++ object has been destroyed.
			assertFalse(niagaraObject.isOwningHandle());
		}
	}

	@Test
	public void testBasicOperations() throws IOException, NiagaraException {
		File tabletPath = new File(temporaryFolder.newFolder().toString());
		try (NiagaraTabletInstance tabletInstance = new NiagaraTabletInstance(
			dbInstance.getNiagaraJNI(),
			tabletPath,
			tabletPath.toString(),
			configuration)) {

			byte[] keyBytes = new byte[10];
			byte[] valueBytes = new byte[20];
			ThreadLocalRandom.current().nextBytes(keyBytes);
			ThreadLocalRandom.current().nextBytes(valueBytes);

			assertNull(tabletInstance.get(keyBytes));
			tabletInstance.put(keyBytes, valueBytes);
			assertArrayEquals(valueBytes, tabletInstance.get(keyBytes));
			tabletInstance.merge(keyBytes, valueBytes);
			ByteBuffer newValueBytes = ByteBuffer.allocate(valueBytes.length * 2 + 1);
			newValueBytes.put(valueBytes);
			newValueBytes.put(",".getBytes(ConfigConstants.DEFAULT_CHARSET));
			newValueBytes.put(valueBytes);
			assertArrayEquals(newValueBytes.array(), tabletInstance.get(keyBytes));

			String snapshotPath = temporaryFolder.newFolder().getAbsolutePath() + "/snapshot";
			tabletInstance.snapshot(snapshotPath);
			File file = new File(snapshotPath);
			assertTrue(file.exists() && file.isDirectory());
			String[] files = file.list();
			assertNotNull(files);
			assertTrue(validateSstFilesExist(files));

			NiagaraIterator iterator = tabletInstance.iterator();
			try {
				iterator.seekToFirst();
				assertArrayEquals(keyBytes, iterator.key());
				assertArrayEquals(newValueBytes.array(), iterator.value());
			} finally {
				iterator.close();
			}
			tabletInstance.delete(keyBytes);
			assertNull(tabletInstance.get(keyBytes));
		}

	}

	@Test
	public void testConstructIterator() throws IOException, NiagaraException {
		File tabletPath = new File(temporaryFolder.newFolder().toString());
		try (NiagaraTabletInstance tabletInstance = new NiagaraTabletInstance(
			dbInstance.getNiagaraJNI(),
			tabletPath,
			tabletPath.toString(),
			configuration)) {

			try {
				createRangeIterator(tabletInstance, null, null);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			try {
				createRangeIterator(tabletInstance, new byte[3], null);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			try {
				createRangeIterator(tabletInstance, null, new byte[3]);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			try {
				createRangeIterator(tabletInstance, new byte[5], new byte[3]);
				fail();
			} catch (IllegalArgumentException e) {
				// ignored.
			}

			createRangeIterator(tabletInstance, new byte[3], new byte[5]);

		}
	}

	@Test
	public void testIteratorAPIs() throws IOException, NiagaraException {
		File tabletPath = new File(temporaryFolder.newFolder().toString());
		try (NiagaraTabletInstance tabletInstance = new NiagaraTabletInstance(
			dbInstance.getNiagaraJNI(),
			tabletPath,
			tabletPath.toString(),
			configuration)) {

			loadRandomData(tabletInstance);

			// test invalid range iterator.
			{
				byte[] invalidStartKey = new byte[]{(byte) (groups + 1)};
				byte[] invalidEndKey = new byte[]{(byte) (groups + 4)};
				Iterator<NiagaraEntry> rangeIterator = createRangeIterator(tabletInstance, invalidStartKey, invalidEndKey);
				testInvalidIteratorAPIs(rangeIterator);
			}

			// test valid range iterator.
			{
				byte[] validStartKey = new byte[]{(byte) 0};
				byte[] validEndKey = new byte[]{(byte) groups};
				Iterator<NiagaraEntry> rangeIterator = createRangeIterator(tabletInstance, validStartKey, validEndKey);
				testValidIteratorAPIs(rangeIterator);
			}
		}
	}

	@Test
	public void testIteratorAccess() throws IOException, NiagaraException {
		File tabletPath = new File(temporaryFolder.newFolder().toString());
		try (NiagaraTabletInstance tabletInstance = new NiagaraTabletInstance(
			dbInstance.getNiagaraJNI(),
			tabletPath,
			tabletPath.toString(),
			configuration)) {

			SortedMap<byte[], byte[]> expectedResult = loadRandomData(tabletInstance);

			// test range iterator access.
			{
				int dataCount = 0;
				int maxRangeStep = 5;
				int startGroup = 0;
				do {
					int toGroup = Math.min(startGroup + ThreadLocalRandom.current().nextInt(maxRangeStep) + 1, groups) + 1;

					byte[] startKey = new byte[]{(byte) startGroup};
					byte[] toKey = new byte[]{(byte) toGroup};

					Iterator<NiagaraEntry> rangeIterator = createRangeIterator(tabletInstance, startKey, toKey);

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
				Iterator<NiagaraEntry> dbIterator = createRangeIterator(tabletInstance, startKey, toKey);
				Iterator<Map.Entry<byte[], byte[]>> expectedIterator = expectedResult.entrySet().iterator();
				validateIteratorsWithRemoval(dbIterator, expectedIterator, true);
			}

			// test iterator over all groups with some elements already removed.
			{
				byte[] startKey = new byte[]{(byte) 0};
				byte[] toKey = new byte[]{(byte) groups};
				validateIterators(createRangeIterator(tabletInstance, startKey, toKey), expectedResult.entrySet().iterator());
			}
		}
	}

	private boolean validateSstFilesExist(String[] files) {
		for (String s : files) {
			if (s.endsWith("sst")) {
				return true;
			}
		}
		return false;
	}

	//--------------------------------------------------------------------------

	private SortedMap<byte[], byte[]> loadRandomData(NiagaraTabletInstance dbInstance) {
		int maxSizeForEachGroup = 32;
		int valueBytesLength = 32;

		SortedMap<byte[], byte[]> result = new TreeMap<>(new Comparator<byte[]>() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				return NiagaraTabletInstance.compare(o1, o2);
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

	private Iterator<NiagaraEntry> createRangeIterator(NiagaraTabletInstance dbInstance, byte[] startKey, byte[] toKey) {
		return new NiagaraStateRangeIterator<NiagaraEntry>(dbInstance, startKey, toKey) {
			@Override
			public NiagaraEntry next() {
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

	private void validateIterators(Iterator<NiagaraEntry> dbIterator, Iterator<Map.Entry<byte[], byte[]>> expectedIterator) {
		validateIteratorsWithRemoval(dbIterator, expectedIterator, false);
	}

	private void validateIteratorsWithRemoval(Iterator<NiagaraEntry> dbIterator, Iterator<Map.Entry<byte[], byte[]>> expectedIterator, boolean removeElements) {
		while (dbIterator.hasNext()) {
			NiagaraEntry dbEntry = dbIterator.next();
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

	private static void ensureNiagaraIsLoaded(String tempDirectory){
		final File tempDirParent = new File(tempDirectory).getAbsoluteFile();

		Throwable lastException = null;
		for (int attempt = 1; attempt <= 3; attempt++) {
			try {
				File tempDirFile = new File(tempDirParent, "niagara-library-" + UUID.randomUUID().toString());

				// make sure the temp path exists
				// noinspection ResultOfMethodCallIgnored
				tempDirFile.mkdirs();
				tempDirFile.deleteOnExit();

				// this initialization here should validate that the loading succeeded
				NiagaraJNI.loadLibrary(tempDirFile.getAbsolutePath());

				return;
			} catch (Throwable t) {
				lastException = t;

				// try to force Niagara to attempt reloading the library
				try {
					final Field initField = com.alibaba.niagara.NativeLibraryLoader.class.getDeclaredField("initialized");
					initField.setAccessible(true);
					initField.setBoolean(null, false);
				} catch (Throwable tt) {
				}
			}
		}

		throw new RuntimeException("Could not load the native Niagara library", lastException);
	}
}
