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
import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.runtime.state.gemini.RowMap;
import org.apache.flink.runtime.state.gemini.RowMapSnapshot;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;

/**
 * Implementation of {@link RowMap} which stores data in some other {@link RowMap} and
 * builds index for the keys. The index will not be included in a snapshot.
 */
public class CowHashRowMapWithIndex implements RowMap {

	/**
	 * Index for keys.
	 */
	private final CowPrefixKeyIndex prefixKeyIndex;

	/**
	 * {@link RowMap} to store data.
	 */
	private final RowMap dataRowMap;

	/**
	 * Whether the row has ordered column keys.
	 */
	private final boolean orderedIndex;

	/**
	 * Constructor for {@link CowHashRowMapWithIndex}
	 *
	 * @param numKeyColumns The number of column keys in the key row.
	 * @param columnComparators Comparators for all column keys in a row.
	 * @param dataRowMap {@link RowMap} to store data.
	 */
	public CowHashRowMapWithIndex(
		int numKeyColumns,
		Comparator[] columnComparators,
		RowMap dataRowMap
	) {
		Preconditions.checkArgument(numKeyColumns > 0);
		Preconditions.checkArgument(columnComparators != null &&
			columnComparators.length == numKeyColumns);
		Preconditions.checkNotNull(dataRowMap);

		this.dataRowMap = dataRowMap;

		boolean ordered = false;
		for (Comparator comparator : columnComparators) {
			if (comparator != null) {
				ordered = true;
				break;
			}
		}
		this.orderedIndex = ordered;

		if (numKeyColumns == 1) {
			Preconditions.checkState(orderedIndex,
				"Row has only one key column and it must be ordered.");
			this.prefixKeyIndex = new OneKeyIndex(columnComparators[0]);
		} else {
			this.prefixKeyIndex = new MultipleKeysIndex(numKeyColumns, columnComparators);
		}
	}

	@Override
	public int size() {
		return dataRowMap.size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Row get(Row key) {
		return dataRowMap.get(key);
	}

	@Override
	public Row put(Row key, Row value) {
		Row result = dataRowMap.put(key, value);
		if (result == null) {
			prefixKeyIndex.addKey(key);
		}

		return result;
	}

	@Override
	public Row remove(Row key) {
		Row result = dataRowMap.remove(key);
		if (result != null) {
			prefixKeyIndex.removeKey(key);
		}

		return result;
	}

	@Override
	public Iterator<Pair<Row, Row>> getIterator(Row prefixKey) {
		return getSubIterator(prefixKey, null, null);
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> getSubIterator(Row prefixKey, K startKey, K endKey) {
		if (!orderedIndex && prefixKey == null && startKey == null && endKey == null) {
			return dataRowMap.getIterator(null);
		}

		Iterator<Row> keyIndexIterator = prefixKeyIndex.getSubIterator(prefixKey, startKey, endKey);
		return new CowHashRowMapWithIndexIterator(keyIndexIterator, dataRowMap);
	}

	@Override
	public Pair<Row, Row> firstPair(Row prefixKey) {
		// no need to check prefixKey num against row's key num
		Row key = prefixKeyIndex.firstRowKey(prefixKey);
		Row value = dataRowMap.get(key);
		return value != null ? new CowHashRowMapWithIndexPair(key, value) : null;
	}

	@Override
	public Pair<Row, Row> lastPair(Row prefixKey) {
		// no need to check prefixKey num against row's key num
		Row key = prefixKeyIndex.lastRowKey(prefixKey);
		Row value = dataRowMap.get(key);
		return value != null ? new CowHashRowMapWithIndexPair(key, value) : null;
	}

	@Override
	public RowMapSnapshot createSnapshot() {
		return dataRowMap.createSnapshot();
	}

	@Override
	public void releaseSnapshot(RowMapSnapshot snapshot) {
		dataRowMap.releaseSnapshot(snapshot);
	}

	@VisibleForTesting
	public CowPrefixKeyIndex getPrefixKeyIndex() {
		return prefixKeyIndex;
	}

	private class CowHashRowMapWithIndexPair implements Pair<Row, Row> {

		private final Row key;

		private Row value;

		private boolean isDeleted;

		CowHashRowMapWithIndexPair(Row key, Row value) {
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

			value = newValue;
			return CowHashRowMapWithIndex.this.dataRowMap.put(key, newValue);
		}

		private void remove() {
			if (isDeleted) {
				return;
			}

			isDeleted = true;
			CowHashRowMapWithIndex.this.dataRowMap.remove(key);
		}
	}

	private class CowHashRowMapWithIndexIterator implements Iterator<Pair<Row, Row>> {

		private final Iterator<Row> keyIndexIterator;

		private final RowMap dataRowMap;

		private CowHashRowMapWithIndexPair currentPair;

		CowHashRowMapWithIndexIterator(
			Iterator<Row> keyIndexIterator,
			RowMap dataRowMap
		) {
			this.keyIndexIterator = Preconditions.checkNotNull(keyIndexIterator);
			this.dataRowMap = Preconditions.checkNotNull(dataRowMap);
		}

		@Override
		public boolean hasNext() {
			return keyIndexIterator.hasNext();
		}

		@Override
		public Pair<Row, Row> next() {
			Row key = keyIndexIterator.next();
			Row value = dataRowMap.get(key);
			Preconditions.checkNotNull(value);

			CowHashRowMapWithIndexPair pair =
				new CowHashRowMapWithIndexPair(key, value);
			currentPair = pair;

			return pair;
		}

		@Override
		public void remove() {
			if (currentPair == null) {
				throw new IllegalStateException();
			}

			keyIndexIterator.remove();
			currentPair.remove();
			currentPair = null;
		}
	}

}
