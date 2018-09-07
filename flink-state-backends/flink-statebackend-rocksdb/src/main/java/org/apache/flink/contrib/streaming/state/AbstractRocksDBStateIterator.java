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

import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A helper class for iterating over entries backed in rocksDB.
 */
abstract class AbstractRocksDBStateIterator<T> implements Iterator<T> {

	private static final int CACHE_SIZE_LIMIT = 128;

	/** The rocksDB instance where entries are located. */
	private final RocksDBInstance dbInstance;

	/**
	 *  The cached rocksDB entries for this iterator to improve the read performance, the cache size range is [1, 128].
	 *
	 *  <p>When this {@link AbstractRocksDBStateIterator} is created, it would first load 1 element into this
	 *  {@code cacheEntries}, if this iterator has been called {@link Iterator#next()} and not meeting the end-key,
	 *  it would load at most {@code Math.min(last-cache-size * 2, 128)} elements into the {@code cacheEntries} for the next time.
	 */
	private final List<RocksDBEntry> cacheEntries;

	/** The entry pointing to the current position which is last returned by calling {@link #getNextEntry()}. */
	private RocksDBEntry currentEntry;

	private boolean expired;

	private int cacheIndex;

	AbstractRocksDBStateIterator(RocksDBInstance dbInstance) {
		this.dbInstance = dbInstance;
		this.cacheEntries = new ArrayList<>();
		this.expired = false;
		this.cacheIndex = 0;
	}

	/**
	 * Get the start-key bytes, which used to seek for iterator.
	 */
	abstract byte[] getStartDBKey();

	/**
	 * Check whether the key-bytes is the end for this {@link AbstractRocksDBStateIterator}.
	 *
	 * @param dbKey The key-bytes to check.
	 */
	abstract boolean isEndDBKey(byte[] dbKey);

	@Override
	public boolean hasNext() {
		loadCacheEntries();
		return (cacheIndex < cacheEntries.size());
	}

	@Override
	public void remove() {
		if (currentEntry == null || currentEntry.isDeleted()) {
			throw new IllegalStateException("The remove operation must be called after a valid next operation.");
		}

		currentEntry.remove();
	}

	final RocksDBEntry getNextEntry() {
		loadCacheEntries();

		if (cacheIndex == cacheEntries.size()) {
			Preconditions.checkState(expired);
			throw new NoSuchElementException();
		}

		this.currentEntry = cacheEntries.get(cacheIndex);
		cacheIndex++;

		return currentEntry;
	}

	/**
	 * Load rocksDB entries into {@link AbstractRocksDBStateIterator#cacheEntries} using {@link RocksIterator}.
	 */
	private void loadCacheEntries() {
		Preconditions.checkState(cacheIndex <= cacheEntries.size());

		// Load cache entries only when cache is empty and there still exist unread entries
		if (cacheIndex < cacheEntries.size() || expired) {
			return;
		}

		try (RocksIterator iterator = dbInstance.iterator()) {
			/*
			 * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
			 * the currentEntry points to the last returned entry, and at that time, we will start
			 * the iterating from currentEntry if reloading cache is needed.
			 */
			byte[] startRocksKey = (currentEntry == null ? getStartDBKey() : currentEntry.getDBKey());

			cacheEntries.clear();
			cacheIndex = 0;

			if (startRocksKey == null) {
				iterator.seekToFirst();
			} else {
				iterator.seek(startRocksKey);
			}

			/*
			 * If the last returned entry is not deleted, it will be the first
			 * entry in the iterating. Skip it to avoid redundant access in such
			 * cases.
			 */
			if (currentEntry != null && !currentEntry.isDeleted()) {
				iterator.next();
			}

			while (true) {
				if (!iterator.isValid() || isEndDBKey(iterator.key())) {
					expired = true;
					break;
				}

				if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
					break;
				}

				RocksDBEntry entry = new RocksDBEntry(dbInstance, iterator.key(), iterator.value());
				cacheEntries.add(entry);

				iterator.next();
			}
		}
	}
}
