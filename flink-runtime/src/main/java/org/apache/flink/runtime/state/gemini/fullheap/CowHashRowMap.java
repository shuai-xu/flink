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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.gemini.RowMap;
import org.apache.flink.runtime.state.gemini.RowMapSnapshot;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Hash table based implementation of {@link RowMap} with copy-on-write and incremental rehash support. We assume
 * that the value won't be modified outside the map.
 * <p>
 * {@link CowHashRowMap} sacrifices some peak performance and memory efficiency for features like incremental
 * rehashing and asynchronous snapshots through copy-on-write. Copy-on-write tries to minimize the amount of
 * copying by maintaining version meta data for the map structure.
 */
public class CowHashRowMap implements RowMap {

	/**
	 * The minimum capacity. Must be a power of two.
	 */
	private static final int MINIMUM_CAPACITY = 1 << 4;

	/**
	 * The maximum capacity. Must be a power of two.
	 */
	private static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * The load factor.
	 */
	private static final float LOAD_FACTOR = 0.75f;

	/**
	 * Minimum number of entries that one step of incremental rehashing migrates from the old to the new table.
	 */
	private static final int MIN_TRANSFERRED_PER_INCREMENTAL_REHASH = 4;

	/**
	 * Empty entry that we use to bootstrap our {@link CowHashRowMap.CowHashRowMapIterator}.
	 */
	private static final CowHashRowMapEntry ITERATOR_BOOTSTRAP_ENTRY = new CowHashRowMapEntry();

	/**
	 * The state backend this map belongs to.
	 */
	private final AbstractInternalStateBackend stateBackend;

	/**
	 * The descriptor of state this map belongs to.
	 */
	private final InternalStateDescriptor descriptor;

	/**
	 * This is the primary entry array (hash directory) of the table. If no incremental rehash is ongoing, this
	 * is the only used table.
	 */
	private CowHashRowMapEntry[] primaryTable;

	/**
	 * We maintain a secondary entry array while performing an incremental rehash. The purpose is to slowly migrate
	 * entries from the primary table to this resized table array. When all entries are migrated, this becomes the new
	 * primary table.
	 */
	private CowHashRowMapEntry[] rehashTable;

	/**
	 * The number of key-value mappings contained in primaryTable.
	 */
	private int primaryTableSize;

	/**
	 * The number of key-value mappings contained in rehashTable.
	 */
	private int rehashTableSize;

	/**
	 * The next size value at which to resize (capacity * load factor).
	 */
	private int threshold;

	/**
	 * The next index for a step of incremental rehashing in the primary table.
	 */
	private int rehashIndex;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	private int rowMapVersion;

	/**
	 * The highest version of unreleased snapshots.
	 */
	private int highestSnapshotVersion;

	/**
	 * An ordered set of unreleased snapshot versions.
	 */
	private final TreeSet<Integer> snapshotVersions;

	/**
	 * Incremented by "structural modifications" to allow (best effort)
	 * detection of concurrent modification.
	 */
	private int modCount;

	/**
	 * Constructor with the given value serializer.
	 *
	 * @param stateBackend The state backend this map belongs to.
	 * @param descriptor The descriptor of state this map belongs to.
	 */
	CowHashRowMap(
		AbstractInternalStateBackend stateBackend,
		InternalStateDescriptor descriptor) {
		this(stateBackend, descriptor, MINIMUM_CAPACITY);
	}

	/**
	 * Constructor with the given value serializer and initial capacity.
	 *
	 * @param stateBackend The state backend this map belongs to.
	 * @param descriptor The descriptor of state this map belongs to.
	 * @param initialCapacity The initial capacity.
	 */
	CowHashRowMap(
		AbstractInternalStateBackend stateBackend,
		InternalStateDescriptor descriptor,
		int initialCapacity) {

		this.stateBackend = Preconditions.checkNotNull(stateBackend);
		this.descriptor = Preconditions.checkNotNull(descriptor);

		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
		}
		if (initialCapacity < MINIMUM_CAPACITY) {
			initialCapacity = MINIMUM_CAPACITY;
		} else if (initialCapacity > MAXIMUM_CAPACITY) {
			initialCapacity = MAXIMUM_CAPACITY;
		} else {
			initialCapacity = MathUtils.roundUpToPowerOfTwo(initialCapacity);
		}

		this.primaryTable = makeTable(initialCapacity);
		this.rehashTable = null;
		this.primaryTableSize = 0;
		this.rehashTableSize = 0;

		this.threshold = (int) (initialCapacity * LOAD_FACTOR);
		this.rehashIndex = 0;

		this.rowMapVersion = 0;
		this.highestSnapshotVersion = 0;
		this.snapshotVersions = new TreeSet<>();

		this.modCount = 0;
	}

	@Override
	public int size() {
		return primaryTableSize + rehashTableSize;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Row get(Row key) {
		if (key == null) {
			return null;
		}

		CowHashRowMapEntry e = getEntry(key);

		return e == null ? null : e.value;
	}

	@Override
	public Row put(Row key, Row value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);

		incrementalRehash();
		CowHashRowMapEntry e = putEntry(key);
		Row oldValue = e.value;
		e.value = value;
		e.valueVersion = rowMapVersion;

		return oldValue;
	}

	@Override
	public Row merge(Row key, Row value) {
		if (key == null) {
			return null;
		}

		incrementalRehash();

		int hash = hashValue(key);
		CowHashRowMapEntry[] table = selectActiveTable(hash);
		int index = hash & (table.length - 1);

		for (CowHashRowMapEntry e = table[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key)) {
				InternalStateDescriptor localDescriptor = descriptor;
				Row oldValue = e.value;
				if (e.valueVersion < highestSnapshotVersion) {
					if (e.entryVersion < highestSnapshotVersion) {
						e = handleChainedEntryCopyOnWrite(table, index, e);

					}
					// copy the old value
					e.value = localDescriptor.getValueSerializer().copy(oldValue);
				}
				e.value = localDescriptor.getValueMerger().merge(e.value, value);
				e.valueVersion = rowMapVersion;

				return oldValue;
			}
		}

		if (size() >= threshold) {
			doubleCapacity();
		}

		CowHashRowMapEntry e = new CowHashRowMapEntry(
			key,
			value,
			rowMapVersion,
			rowMapVersion,
			hash,
			table[index]);
		table[index] = e;

		if (table == primaryTable) {
			++primaryTableSize;
		} else {
			++rehashTableSize;
		}

		++modCount;

		return null;
	}

	@Override
	public Row remove(Row key) {
		if (key == null) {
			return null;
		}

		incrementalRehash();
		CowHashRowMapEntry e = removeEntry(key);

		return e == null ? null : e.value;
	}

	public AbstractInternalStateBackend getStateBackend() {
		return stateBackend;
	}

	public InternalStateDescriptor getDescriptor() {
		return descriptor;
	}

	// Private implementation details of the API methods ---------------------------------------------------------------

	/**
	 * Returns the entry for the specified key.
	 *
	 * @param key The specified key.
	 * @return The entry for the specified key if present, otherwise {@code null}.
	 */
	private CowHashRowMapEntry getEntry(Row key) {
		int hash = hashValue(key);
		CowHashRowMapEntry[] table = selectActiveTable(hash);
		int index = hash & (table.length - 1);

		for (CowHashRowMapEntry e = table[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key)) {
				return e;
			}
		}

		return null;
	}

	/**
	 * Creates an entry for the specified key if the entry does not exit.
	 * Otherwise we need make a copy-on-write check for the entry.
	 *
	 * @param key The specified key.
	 * @return The entry for the specified key.
	 */
	private CowHashRowMapEntry putEntry(Row key) {
		int hash = hashValue(key);
		CowHashRowMapEntry[] table = selectActiveTable(hash);
		int index = hash & (table.length - 1);

		for (CowHashRowMapEntry e = table[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key)) {
				if (e.entryVersion < highestSnapshotVersion) {
					e = handleChainedEntryCopyOnWrite(table, index, e);
				}

				return e;
			}
		}

		if (size() >= threshold) {
			doubleCapacity();
		}

		CowHashRowMapEntry e = new CowHashRowMapEntry(
			key,
			null,
			rowMapVersion,
			rowMapVersion,
			hash,
			table[index]);
		table[index] = e;

		if (table == primaryTable) {
			++primaryTableSize;
		} else {
			++rehashTableSize;
		}

		++modCount;

		return e;
	}

	/**
	 * Removes the entry for the specified key and make a copy-on-write check if the entry exsits.
	 *
	 * @param key The specified key to remove
	 * @return The entry for the specified key if present, otherwise{@code null}.
	 */
	private CowHashRowMapEntry removeEntry(Row key) {
		int hash = hashValue(key);
		CowHashRowMapEntry[] table = selectActiveTable(hash);
		int index = hash & (table.length - 1);

		for (CowHashRowMapEntry e = table[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key)) {
				if (prev == null) {
					table[index] = e.next;
				} else {
					if (prev.entryVersion < highestSnapshotVersion) {
						prev = handleChainedEntryCopyOnWrite(table, index, prev);
					}
					prev.next = e.next;
				}

				if (table == primaryTable) {
					--primaryTableSize;
				} else {
					--rehashTableSize;
				}

				++modCount;

				return e;
			}
		}

		return null;
	}

	private int hashValue(Row key) {
		return MathUtils.bitMix(key.hashCode());
	}

	private CowHashRowMapEntry[] makeTable(int capacity) {
		if (capacity < 0) {
			throw new IllegalArgumentException("Illegal capacity: " + capacity);
		}

		return new CowHashRowMapEntry[capacity];
	}

	private void doubleCapacity() {
		Preconditions.checkState(!isRehashing(), "There is already a rehash in progress.");

		int oldCapacity = primaryTable.length;
		if (oldCapacity == MAXIMUM_CAPACITY) {
			return;
		}

		rehashTable = makeTable(oldCapacity << 1);
		threshold = (int) ((oldCapacity << 1) * LOAD_FACTOR);
	}

	/**
	 * Performs copy-on-write for entry chains. We iterate the chain, and replace all links
	 * up to the 'untilEntry', which we actually wanted to modify.
	 *
	 * @param table The entry array we perform copty-on-write.
	 * @param index The position of the entry chain in the table.
	 * @param untilEntry The entry we wanted to modify.
	 * @return The copy of untilEnty if copy-on-write is performed, otherwise the untilEnty.
	 */
	private CowHashRowMapEntry handleChainedEntryCopyOnWrite(
		CowHashRowMapEntry[] table,
		int index,
		CowHashRowMapEntry untilEntry) {

		int requiredVersion = highestSnapshotVersion;

		CowHashRowMapEntry current = table[index];
		CowHashRowMapEntry copy;

		if (current.entryVersion < requiredVersion) {
			copy = new CowHashRowMapEntry(current, rowMapVersion);
			table[index] = copy;
		} else {
			// nothing to do, just advance copy to current
			copy = current;
		}

		// we iterate the chain up to 'until entry'
		while (current != untilEntry) {
			//advance current
			current = current.next;

			if (current.entryVersion < requiredVersion) {
				// copy and advance the current's copy
				copy.next = new CowHashRowMapEntry(current, rowMapVersion);
				copy = copy.next;
			} else {
				// nothing to do, just advance copy to current
				copy = current;
			}
		}

		return copy;
	}

	/**
	 * Returns whether this map is rehashing.
	 *
	 * @return {@code true} if this map is rehashing, {@code false} otherwise.
	 */
	@VisibleForTesting
	public boolean isRehashing() {
		return rehashTable != null;
	}

	/**
	 * Selects the sub-table which is responsible for entries with the given hash code.
	 *
	 * @param hash The hash code which we use to decide about the table that is responsible.
	 * @return The sub-table that is responsible for the entry with the given hash code.
	 */
	private CowHashRowMapEntry[] selectActiveTable(int hash) {
		int index = hash & (primaryTable.length - 1);
		return index >= rehashIndex ? primaryTable : rehashTable;
	}

	/**
	 * Runs a number of steps for incremental rehashing.
	 */
	private void incrementalRehash() {
		if (!isRehashing()) {
			return;
		}

		CowHashRowMapEntry[] oldTable = primaryTable;
		CowHashRowMapEntry[] newTable = rehashTable;

		int oldCapacity = oldTable.length;
		int newMask = newTable.length - 1;
		int requiredVersion = highestSnapshotVersion;
		int rhIdx = rehashIndex;
		int transferred = 0;

		// rehashing changes the structure of the map.
		++modCount;

		// we migrate a certain minimum amount of entries from the old to the new table
		while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {

			CowHashRowMapEntry e = oldTable[rhIdx];

			while (e != null) {
				// copy-on-write check for entry
				if (e.entryVersion < requiredVersion) {
					e = new CowHashRowMapEntry(e, rowMapVersion);
				}
				CowHashRowMapEntry n = e.next;
				int pos = e.hash & newMask;
				e.next = newTable[pos];
				newTable[pos] = e;
				e = n;
				++transferred;
			}

			oldTable[rhIdx] = null;
			if (++rhIdx == oldCapacity) {
				//here, the rehash is complete and we release resources and reset fields
				primaryTable = newTable;
				rehashTable = null;
				primaryTableSize += rehashTableSize;
				rehashTableSize = 0;
				rehashIndex = 0;
				return;
			}
		}

		// sync our local bookkeeping the with official bookkeeping fields
		primaryTableSize -= transferred;
		rehashTableSize += transferred;
		rehashIndex = rhIdx;
	}

	/**
	 * Replaces the value associated with the key without rehashing, and return the old value
	 * if it exists. It's only used in {@link CowHashRowMapPair#setValue(Row)}. Rehashing can
	 * move a visited key-value mapping to the position where we are going to iterate so that
	 * we may visit a mapping repeatedly.
	 */
	private Row replaceWithoutRehash(Row key, Row value) {
		if (key == null) {
			return null;
		}

		int hash = hashValue(key);
		CowHashRowMapEntry[] table = selectActiveTable(hash);
		int index = hash & (table.length - 1);

		for (CowHashRowMapEntry e = table[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key)) {
				if (e.entryVersion < highestSnapshotVersion) {
					e = handleChainedEntryCopyOnWrite(table, index, e);
				}
				Row oldValue = e.value;
				e.value = value;

				return oldValue;
			}
		}

		return null;
	}

	/**
	 * Removes a key without rehashing. It's only used in {@link CowHashRowMapPair#remove()}
	 * currently.Rehashing can move a visited key-value mapping to the position where
	 * we are going to iterate so that we may visit a mapping repeatedly.
	 */
	private Row removeWithoutRehash(Row key) {
		if (key == null) {
			return null;
		}

		CowHashRowMapEntry e = removeEntry(key);

		return e == null ? null : e.value;
	}

	// Iteration -------------------------------------------------------------------------------------------------------

	@Override
	public Iterator<Pair<Row, Row>> getIterator(Row prefixKeys) {
		if (prefixKeys != null) {
			throw new UnsupportedOperationException("Iterator with prefix keys is unsupported");
		}

		return new CowHashRowMapIterator();
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> getSubIterator(Row prefixKeys, K startKey, K endKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<Row, Row> firstPair(Row prefixKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<Row, Row> lastPair(Row prefixKeys) {
		throw new UnsupportedOperationException();
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	@Override
	public RowMapSnapshot createSnapshot() {
		synchronized (snapshotVersions) {
			// increase the row map version for copy-on-write and register the snapshot
			if (++rowMapVersion < 0) {
				// this is just a safety net against overflows, but should never happen in practice (i.e., only after 2^31 snapshots)
				throw new IllegalStateException("Version count overflow in CowHashRowMap. Enforcing restart.");
			}

			highestSnapshotVersion = rowMapVersion;
			snapshotVersions.add(rowMapVersion);
		}

		return new CowHashRowMapSnapshot(
			this,
			snapshotTable(),
			size(),
			rowMapVersion
		);
	}

	@Override
	public void releaseSnapshot(RowMapSnapshot snapshot) {
		CowHashRowMapSnapshot cowHashRowMapSnapshot = (CowHashRowMapSnapshot) snapshot;
		int snapshotVersion = cowHashRowMapSnapshot.snapshotVersion();

		synchronized (snapshotVersions) {
			Preconditions.checkState(snapshotVersions.remove(snapshotVersion), "Attempt to release unknown snapshot version");
			highestSnapshotVersion = snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
		}
	}

	/**
	 * Creates copy of the table arrays for a snapshot.
	 *
	 * @return The copy of the table arrays.
	 */
	private CowHashRowMapEntry[] snapshotTable() {
		CowHashRowMapEntry[] table = primaryTable;

		// In order to reuse the copied array as the destination array for the partitioned records in
		// CowHashRowMapSnapshot#partitionRowByGroup(), we need to make sure that the copied array
		// is big enough to hold the flattened entries. In fact, given the current rehashing algorithm, we only
		// need to do this check when isRehashing() is false, but in order to get a more robust code(in case that
		// the rehashing algorithm may changed in the future), we do this check for all the case.
		final int totalTableIndexSize = rehashIndex + table.length;
		final int copiedArraySize = Math.max(totalTableIndexSize, size());
		final CowHashRowMapEntry[] copy = new CowHashRowMapEntry[copiedArraySize];

		if (isRehashing()) {
			// consider both tables for the snapshot, the rehash index tells us which part of the two tables we need
			int localRehashIndex = rehashIndex;
			int localCopyLength = table.length - localRehashIndex;
			// for the primary table, take every index >= rhIdx.
			System.arraycopy(table, localRehashIndex, copy, 0, localCopyLength);

			// for the new table, we are sure that two regions contain all the entries:
			// [0, rhIdx[ AND [table.length / 2, table.length / 2 + rhIdx[
			table = rehashTable;
			System.arraycopy(table, 0, copy, localCopyLength, localRehashIndex);
			System.arraycopy(table, table.length >>> 1, copy, localCopyLength + localRehashIndex, localRehashIndex);
			} else {
			// we only need to copy the primary table
			System.arraycopy(table, 0, copy, 0, table.length);
		}

		return copy;
	}

	/**
	 * One entry in the {@link CowHashRowMap}.
	 */
	static class CowHashRowMapEntry implements Map.Entry<Row, Row> {

		/**
		 * The key. Assumed to be immutable and not null.
		 */
		final Row key;

		/**
		 * The value. Can be null.
		 */
		Row value;

		/**
		 * The version of this entry, used for copy-on-write.
		 */
		int entryVersion;

		/**
		 * The version of the value.
		 */
		int valueVersion;

		/**
		 * The computed secondary hash.
		 */
		final int hash;

		/**
		 * Link to another {@link CowHashRowMapEntry}. This is used to resolve collisions in the
		 * {@link CowHashRowMap} through chaining.
		 */
		CowHashRowMapEntry next;

		CowHashRowMapEntry() {
			this(null, null, 0,  0, 0, null);
		}

		CowHashRowMapEntry(CowHashRowMapEntry e, int entryVersion) {
			this(e.key, e.value, entryVersion, e.valueVersion, e.hash, e.next);
		}

		CowHashRowMapEntry(
			Row key,
			Row value,
			int entryVersion,
			int valueVersion,
			int hash,
			CowHashRowMapEntry next
		) {
			this.key = key;
			this.value = value;
			this.entryVersion = entryVersion;
			this.valueVersion = valueVersion;
			this.hash = hash;
			this.next = next;
		}

		@Override
		public Row getKey() {
			return key;
		}

		@Override
		public Row getValue() {
			return value;
		}

		@Override
		public Row setValue(Row newValue) {
			Row oldValue = this.value;
			this.value = newValue;

			return oldValue;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			CowHashRowMapEntry e = (CowHashRowMapEntry) o;

			return Objects.equals(key, e.key) && Objects.equals(value, e.value);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(key) ^ Objects.hashCode(value);
		}

		@Override
		public String toString() {
			return key + "=" + value;
		}
	}

	/**
	 * Implementation for {@link Pair}. All of
	 */
	private class CowHashRowMapPair implements Pair<Row, Row> {

		private final Row key;

		private Row value;

		private boolean isDeleted;

		CowHashRowMapPair(Row key, Row value) {
			this.key = key;
			this.value = value;
			this.isDeleted = false;
		}

		@Override
		public Row getKey() {
			return key;
		}

		@Override
		public Row getValue() {
			return value;
		}

		@Override
		public Row setValue(Row newValue) {
			if (isDeleted) {
				throw new IllegalStateException("This pair is already deleted");
			}

			Preconditions.checkNotNull(newValue);
			value = newValue;
			// copy-on-write may happen
			return CowHashRowMap.this.replaceWithoutRehash(key, newValue);
		}

		private void remove() {
			if (isDeleted) {
				return;
			}

			isDeleted = true;
			// copy-on-write may happen
			CowHashRowMap.this.removeWithoutRehash(key);
		}
	}

	/**
	 * Iterator over the entries in {@link CowHashRowMap}.
	 */
	private class CowHashRowMapIterator implements Iterator<Pair<Row, Row>> {

		CowHashRowMapEntry[] activeTable;

		CowHashRowMapPair currentPair;

		CowHashRowMapEntry nextEntry;

		int nextIndex;

		int expectedModCount;

		CowHashRowMapIterator() {
			this.activeTable = primaryTable;
			this.currentPair = null;
			this.nextEntry = ITERATOR_BOOTSTRAP_ENTRY;
			this.nextIndex = rehashIndex;
			this.expectedModCount = modCount;

			advanceIterator();
		}

		private void advanceIterator() {
			CowHashRowMapEntry next = nextEntry.next;

			while (next == null) {
				CowHashRowMapEntry[] table = activeTable;

				while (nextIndex < table.length) {
					next = table[nextIndex++];
					if (next != null) {
						nextEntry = next;
						return;
					}
				}

				if (!isRehashing() || activeTable == rehashTable) {
					break;
				}

				activeTable = rehashTable;
				nextIndex = 0;
			}

			nextEntry = next;
		}

		@Override
		public boolean hasNext() {
			return nextEntry != null;
		}

		@Override
		public Pair<Row, Row> next() {
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}

			if (nextEntry == null) {
				throw new NoSuchElementException();
			}

			CowHashRowMapPair pair = new CowHashRowMapPair(nextEntry.key, nextEntry.value);
			currentPair = pair;
			advanceIterator();

			return pair;
		}

		@Override
		public void remove() {
			CowHashRowMapPair pair = currentPair;

			if (pair == null) {
				throw new IllegalStateException();
			}

			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}

			currentPair.remove();
			currentPair = null;

			expectedModCount = modCount;
		}
	}
}
