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

package org.apache.flink.contrib.streaming.state3;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.reflection.Whitebox;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for RocksDBStorageInstance.
 */
public class RocksDBStorageInstanceTest {
	private static RocksDB db;
	private static DBOptions dbOptions;

	private static ColumnFamilyOptions columnOptions;

	@BeforeClass
	public static void setupOptions() throws Exception {
		dbOptions = PredefinedOptions.DEFAULT.createDBOptions().setCreateIfMissing(true);
		columnOptions = PredefinedOptions.DEFAULT.createColumnOptions()
			.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
		db = RocksDB.open(temporaryFolder.newFolder().getAbsolutePath());

	}

	@AfterClass
	public static void disposeOptions() {
		if (db != null) {
			db.close();
			db = null;
		}
		if (dbOptions != null) {
			dbOptions.close();
		}
		if (columnOptions != null) {
			columnOptions.close();
		}
	}

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testRocksObjectsClosed() throws Exception {
		byte[] nameBytes = "testRocksObjectsClosed".getBytes(ConfigConstants.DEFAULT_CHARSET);
		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);
		ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(columnDescriptor);

		RocksDbStorageInstance storageInstance = new RocksDbStorageInstance(
			db,
			columnFamilyHandle);

		storageInstance.close();

		// all storage instances share the same db instance.
		// will close db instance when state backend is closed.
		assertTrue(db.isOwningHandle());
		assertFalse(columnFamilyHandle.isOwningHandle());
		WriteOptions writeOptions = (WriteOptions) Whitebox.getInternalState(storageInstance, "writeOptions");
		assertFalse(writeOptions.isOwningHandle());
	}

	@Test
	public void testBasicOperations() throws Exception {
		byte[] nameBytes = "testBasicOperations".getBytes(ConfigConstants.DEFAULT_CHARSET);
		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);
		ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(columnDescriptor);

		try (RocksDbStorageInstance storageInstance = new RocksDbStorageInstance(
			db,
			columnFamilyHandle)) {
			byte[] keyBytes = new byte[10];
			byte[] valueBytes = new byte[20];
			ThreadLocalRandom random = ThreadLocalRandom.current();
			random.nextBytes(keyBytes);
			random.nextBytes(valueBytes);

			assertNull(storageInstance.get(keyBytes));
			storageInstance.put(keyBytes, valueBytes);
			assertArrayEquals(valueBytes, storageInstance.get(keyBytes));
			storageInstance.merge(keyBytes, valueBytes);
			ByteBuffer newValueBytes = ByteBuffer.allocate(valueBytes.length * 2 + 1);
			newValueBytes.put(valueBytes);
			newValueBytes.put(",".getBytes(ConfigConstants.DEFAULT_CHARSET));
			newValueBytes.put(valueBytes);
			assertArrayEquals(newValueBytes.array(), storageInstance.get(keyBytes));

			String snapshotPath = temporaryFolder.newFolder().getAbsolutePath() + "/snapshot";
			storageInstance.snapshot(snapshotPath);
			File file = new File(snapshotPath);
			assertTrue(file.exists() && file.isDirectory());
			String[] files = file.list();
			assertNotNull(files);
			assertTrue(validateSstFilesExist(files));

			try (RocksIterator iterator = storageInstance.iterator()) {
				iterator.seekToFirst();
				assertArrayEquals(keyBytes, iterator.key());
				assertArrayEquals(newValueBytes.array(), iterator.value());
			}
			storageInstance.delete(keyBytes);
			assertNull(storageInstance.get(keyBytes));
		}
	}

	@Test
	public void testMultiColumnFamily() throws Exception {
		byte[] nameBytes = "testMultiColumnFamily1".getBytes(ConfigConstants.DEFAULT_CHARSET);
		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);
		ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(columnDescriptor);

		byte[] nameBytes2 = "testMultiColumnFamily2".getBytes(ConfigConstants.DEFAULT_CHARSET);
		ColumnFamilyDescriptor columnDescriptor2 = new ColumnFamilyDescriptor(nameBytes2, columnOptions);
		ColumnFamilyHandle columnFamilyHandle2 = db.createColumnFamily(columnDescriptor2);

		RocksDbStorageInstance storageInstance1 = new RocksDbStorageInstance(db, columnFamilyHandle);
		RocksDbStorageInstance storageInstance2 = new RocksDbStorageInstance(db, columnFamilyHandle2);

		byte[] keyBytes = new byte[10];
		byte[] valueBytes = new byte[20];
		ThreadLocalRandom random = ThreadLocalRandom.current();
		random.nextBytes(keyBytes);
		random.nextBytes(valueBytes);

		storageInstance1.put(keyBytes, valueBytes);
		assertArrayEquals(storageInstance1.get(keyBytes), valueBytes);
		assertNull(storageInstance2.get(keyBytes));

		storageInstance2.put(keyBytes, valueBytes);
		assertArrayEquals(storageInstance2.get(keyBytes), valueBytes);

		storageInstance1.delete(keyBytes);
		assertArrayEquals(storageInstance2.get(keyBytes), valueBytes);
		assertNull(storageInstance1.get(keyBytes));

		storageInstance2.delete(keyBytes);
		assertNull(storageInstance1.get(keyBytes));
		assertNull(storageInstance2.get(keyBytes));

		storageInstance1.close();
		storageInstance2.close();
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

