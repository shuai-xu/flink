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

import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.StorageInstance;
import org.apache.flink.runtime.state3.StorageIterator;
import org.apache.flink.types.Pair;

import org.rocksdb.RocksIterator;

/**
 * An implementation of {@link StateStorage} which stores key-values in {@code RocksDB}.
 */
public class RocksDbStateStorage implements StateStorage<byte[], byte[]> {
	private final RocksDbStorageInstance storageInstance;

	public RocksDbStateStorage(RocksDbStorageInstance storageInstance) {
		this.storageInstance = storageInstance;
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		storageInstance.put(key, value);
	}

	@Override
	public byte[] get(byte[] key) throws Exception {
		return storageInstance.get(key);
	}

	@Override
	public boolean remove(byte[] key) {
		storageInstance.delete(key);
		return true;
	}

	@Override
	public StorageIterator iterator() {
		return new RocksDbStoragePrefixIterator(storageInstance, null);
	}

	@Override
	public StorageIterator prefixIterator(byte[] prefixKey) {
		return new RocksDbStoragePrefixIterator(storageInstance, prefixKey);
	}

	@Override
	public StorageIterator subIterator(byte[] prefixKeyStart, byte[] prefixKeyEnd) {
		return new RocksDbStorageRangeIterator(storageInstance, prefixKeyStart, prefixKeyEnd);
	}

	@Override
	public Pair<byte[], byte[]> firstEntry(byte[] prefixKeys) {

		try (RocksIterator iterator = storageInstance.iterator()) {
			iterator.seek(prefixKeys);
			if (iterator.isValid()) {
				return new RocksDbPair(storageInstance, iterator.key(), iterator.value());
			} else {
				return null;
			}
		}
	}

	@Override
	public Pair<byte[], byte[]> lastEntry(byte[] prefixKeys) {
		try (RocksIterator iterator = storageInstance.iterator()) {
			iterator.seek(prefixKeys);
			if (iterator.isValid()) {
				iterator.prev();
			} else {
				iterator.seekToLast();
			}
			if (iterator.isValid()) {
				return new RocksDbPair(storageInstance, iterator.key(), iterator.value());
			} else {
				return null;
			}
		}
	}

	@Override
	public void merge(byte[] key, byte[] value) {
		storageInstance.merge(key, value);
	}

	@Override
	public boolean lazySerde() {
		return false;
	}

	@Override
	public boolean supportMultiColumnFamilies() {
		return true;
	}

	@Override
	public StorageInstance getStorageInstance() {
		return storageInstance;
	}
}

