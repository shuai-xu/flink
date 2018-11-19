/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.state.niagara;

import org.apache.flink.util.Preconditions;

import com.alibaba.niagara.NiagaraIterator;
import com.alibaba.niagara.ReadOptions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * State iterator in Niagara.
 *
 * @param <T> The type of iterator value.
 */
public abstract class AbstractNiagaraStateIterator<T> implements Iterator<T> {

	static final int CACHE_SIZE_LIMIT = 128;

	/** The tablet where data resides. */
	private final NiagaraTabletInstance instance;

	/**
	 * True if all entries have been accessed or the iterator has come across an
	 * entry with a different prefix.
	 */
	private boolean expired = false;

	/** An in-memory cache for the entries in the niagara. */
	private ArrayList<NiagaraEntry> cacheEntries = new ArrayList<>();
	private int cacheIndex = 0;

	AbstractNiagaraStateIterator(final NiagaraTabletInstance instance) {
		this.instance = instance;
	}

	abstract byte[] getStartDBKey();

	/**
	 * Returns the endKey bytes which will be used when creating Niagara iterator,
	 * if the readOptions has been set up an endKey, the created iterator must containing all keys smaller than
	 * the endKey or not valid.
	 *
	 * @return The end key bytes which will be used when creating Niagara iterator.
	 */
	abstract byte[] getEndDBKey();

	@Override
	public boolean hasNext() {
		loadCache();
		return (cacheIndex < cacheEntries.size());
	}

	@Override
	public void remove() {
		if (cacheIndex == 0 || cacheIndex > cacheEntries.size()) {
			throw new IllegalStateException("The remove operation must be called after an valid next operation.");
		}

		NiagaraEntry lastEntry = cacheEntries.get(cacheIndex - 1);
		lastEntry.remove();
	}

	protected final NiagaraEntry getNextEntry() {
		loadCache();

		if (cacheIndex == cacheEntries.size()) {
			Preconditions.checkState(expired);
			throw new NoSuchElementException();
		}

		NiagaraEntry entry = cacheEntries.get(cacheIndex);
		cacheIndex++;

		return entry;
	}

	private void loadCache() {
		Preconditions.checkState(cacheIndex <= cacheEntries.size());

		if (cacheIndex < cacheEntries.size() || expired) {
			return;
		}

		ReadOptions readOptions = new ReadOptions();
		readOptions.setEndKey(getEndDBKey());
		NiagaraIterator iterator = instance.iterator(readOptions);

		try {
			// The iteration starts from the start bytes at the first loading (cacheIndex == cacheEntries.size() == 0).
			// The cache reloads when the next entry to return is the last one in the cache (cacheIndex == cacheEntries.size() > 0).
			// At that time, we will start the iterating from the last returned entry.
			// We iterates the backend as a geometric sequence style(common ratio is 2)
			NiagaraEntry lastEntry = cacheEntries.size() == 0 ? null : cacheEntries.get(cacheEntries.size() - 1);

			byte[] startBytes = (lastEntry == null ? getStartDBKey() : lastEntry.getDBKey());

			cacheEntries.clear();
			cacheIndex = 0;

			if (startBytes == null) {
				iterator.seekToFirst();
			} else {
				iterator.seek(startBytes);
			}

			// If the last returned entry is not deleted, it will be the first entry in the
			// iterating. In such cases, skip it to avoid redundant access.
			if (lastEntry != null && !lastEntry.isDeleted()) {
				iterator.next();
				cacheEntries.add(lastEntry);
				cacheIndex = 1;
			}

			while (true) {
				if (!iterator.isValid()) {
					expired = true;
					break;
				}

				if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
					break;
				}

				NiagaraEntry entry = new NiagaraEntry(instance, iterator.key(), iterator.value());
				cacheEntries.add(entry);

				iterator.next();
			}

		} finally {
			iterator.close();
			readOptions.close();
		}
	}
}
