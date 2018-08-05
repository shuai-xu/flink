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

import org.apache.flink.configuration.ConfigConstants;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.reflection.Whitebox;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link RocksDBInstance}.
 */
public class RocksDBInstanceTest {

	private int ttlSeconds = -1;

	private static DBOptions dbOptions;

	private static ColumnFamilyOptions columnOptions;

	@BeforeClass
	public static void setupOptions() {
		dbOptions = PredefinedOptions.DEFAULT.createDBOptions().setCreateIfMissing(true);
		columnOptions = PredefinedOptions.DEFAULT.createColumnOptions()
			.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
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
	public void testRocksObjectsClosed() throws IOException, RocksDBException {
		RocksDBInstance dbInstance = new RocksDBInstance(
			dbOptions,
			columnOptions,
			temporaryFolder.newFolder().getAbsoluteFile());

		RocksDB rocksDB = (RocksDB) Whitebox.getInternalState(dbInstance, "db");
		WriteOptions writeOptions = (WriteOptions) Whitebox.getInternalState(dbInstance, "writeOptions");
		ColumnFamilyHandle defaultColumnFamily = (ColumnFamilyHandle) Whitebox.getInternalState(dbInstance, "defaultColumnFamily");
		List<RocksObject> rocksObjects = Arrays.asList(rocksDB, writeOptions, defaultColumnFamily);

		for (RocksObject rocksObject : rocksObjects) {
			assertNotNull(rocksObject);
			assertTrue(rocksObject.isOwningHandle());
		}

		dbInstance.close();
		for (RocksObject rocksObject : rocksObjects) {
			// The C++ object has been destroyed.
			assertFalse(rocksObject.isOwningHandle());
		}
	}

	@Test
	public void testBasicOperations() throws IOException, RocksDBException {
		try (RocksDBInstance dbInstance = new RocksDBInstance(
			dbOptions,
			columnOptions,
			temporaryFolder.newFolder().getAbsoluteFile())) {

			byte[] keyBytes = new byte[10];
			byte[] valueBytes = new byte[20];
			ThreadLocalRandom.current().nextBytes(keyBytes);
			ThreadLocalRandom.current().nextBytes(valueBytes);

			assertNull(dbInstance.get(keyBytes));
			dbInstance.put(keyBytes, valueBytes);
			assertArrayEquals(valueBytes, dbInstance.get(keyBytes));
			dbInstance.merge(keyBytes, valueBytes);
			ByteBuffer newValueBytes = ByteBuffer.allocate(valueBytes.length * 2 + 1);
			newValueBytes.put(valueBytes);
			newValueBytes.put(",".getBytes(ConfigConstants.DEFAULT_CHARSET));
			newValueBytes.put(valueBytes);
			assertArrayEquals(newValueBytes.array(), dbInstance.get(keyBytes));

			String snapshotPath = temporaryFolder.newFolder().getAbsolutePath() + "/snapshot";
			dbInstance.snapshot(snapshotPath);
			File file = new File(snapshotPath);
			assertTrue(file.exists() && file.isDirectory());
			String[] files = file.list();
			assertNotNull(files);
			assertTrue(validateSstFilesExist(files));

			try (RocksIterator iterator = dbInstance.iterator()) {
				iterator.seekToFirst();
				assertArrayEquals(keyBytes, iterator.key());
				assertArrayEquals(newValueBytes.array(), iterator.value());
			}
			dbInstance.delete(keyBytes);
			assertNull(dbInstance.get(keyBytes));
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
}
