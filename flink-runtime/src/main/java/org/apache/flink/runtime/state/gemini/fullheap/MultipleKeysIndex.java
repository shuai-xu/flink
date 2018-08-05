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
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Implementation of {@link CowPrefixKeyIndex} where the {@link Row} of key has multiple columns.
 * We use nested map to build index. If the row has 4 columns: K0, K1, K2, K3, the index will be
 * K0 -> K1 -> K2 -> Row. The last row will be stored in {@link HashSet} or {@link TreeSet}, depending
 * on whether K3 is ordered.
 */
class MultipleKeysIndex implements CowPrefixKeyIndex {

	/**
	 * Nested map to build the prefix index.
	 */
	private final Map keyIndexMap;

	/**
	 * Number of key columns in the row.
	 */
	private final int numKeyColumns;

	/**
	 * Comparator arrays of all keys. The key is unordered if the comparator is null.
	 */
	private final Comparator[] keyComparators;

	@SuppressWarnings("unchecked")
	public MultipleKeysIndex(
		int numKeyColumns,
		Comparator[] keyComparators
	) {
		Preconditions.checkArgument(numKeyColumns > 1);
		Preconditions.checkArgument(keyComparators != null &&
			keyComparators.length == numKeyColumns);

		this.numKeyColumns = numKeyColumns;
		this.keyComparators = keyComparators;

		if (keyComparators[0] == null) {
			this.keyIndexMap = new HashMap();
		} else {
			this.keyIndexMap = new TreeMap(keyComparators[0]);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void addKey(Row key) {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.getArity() == numKeyColumns);

		Map currentMap = keyIndexMap;

		for (int i = 0; i < numKeyColumns - 2; i++) {
			Object columnKey = key.getField(i);
			Map nextMap = (Map) currentMap.get(columnKey);
			if (nextMap == null) {
				Comparator comparator = keyComparators[i + 1];
				nextMap = comparator != null ?
					new TreeMap(comparator) :
					new HashMap();
				currentMap.put(columnKey, nextMap);
			}
			currentMap = nextMap;
		}

		Object lastMapKey = key.getField(numKeyColumns - 2);
		Set rowSet = (Set) currentMap.get(lastMapKey);
		if (rowSet == null) {
			Comparator comparator = keyComparators[numKeyColumns - 1];
			rowSet = comparator != null ?
				new TreeSet<Row>((row1, row2) ->
					comparator.compare(row1.getField(numKeyColumns - 1), row2.getField(numKeyColumns - 1))
				) :
				new HashSet<Row>();

			currentMap.put(lastMapKey, rowSet);
		}
		rowSet.add(key);
	}

	@Override
	public void removeKey(Row key) {
		if (key == null) {
			return;
		}

		Preconditions.checkArgument(key.getArity() == numKeyColumns);

		List<Map> internalMaps = new ArrayList<>(numKeyColumns - 1);

		Map currentMap = keyIndexMap;
		for (int i = 0; i < numKeyColumns - 2; i++) {
			internalMaps.add(currentMap);
			currentMap = (Map) currentMap.get(key.getField(i));
			if (currentMap == null) {
				return;
			}
		}
		internalMaps.add(currentMap);

		Object lastMapKey = key.getField(numKeyColumns - 2);
		Set rowSet = (Set) currentMap.get(lastMapKey);

		if (rowSet == null) {
			return;
		}

		rowSet.remove(key);

		// Prune empty internal nodes
		if (rowSet.isEmpty()) {
			currentMap.remove(lastMapKey);
			for (int i = numKeyColumns - 2; i > 0; i--) {
				if (!internalMaps.get(i).isEmpty()) {
					break;
				}
				Object columnKey = key.getField(i - 1);
				internalMaps.get(i - 1).remove(columnKey);
			}
		}
	}

	@Override
	public Row firstRowKey(Row prefixKey) {
		checkPrefixKey(prefixKey);
		return prefixRowKey(prefixKey, true);
	}

	@Override
	public Row lastRowKey(Row prefixKey) {
		checkPrefixKey(prefixKey);
		return prefixRowKey(prefixKey, false);
	}

	@Override
	public <K> Iterator<Row> getSubIterator(Row prefixKey, K startKey, K endKey) {
		checkPrefixKey(prefixKey);

		int numPrefixKeys = prefixKey == null ? 0 : prefixKey.getArity();
		Preconditions.checkState((startKey == null && endKey == null) ||
			keyComparators[numPrefixKeys] != null);

		if (numPrefixKeys == numKeyColumns - 1) {
			return new OneLevelIterator(numKeyColumns, prefixKey, startKey, endKey);
		} else {
			return new MultipleLevelIterator(numKeyColumns, prefixKey, startKey, endKey);
		}
	}

	/**
	 * Returns a key with the given prefix. if ascending is true, the returned key
	 * is the smallest among the keys with the prefix; otherwise, the returned key
	 * is the largest.
	 *
	 * @param prefixKey The prefix keys.
	 * @param ascending Whether to return the smallest or the largest key.
	 * @return The key with the given prefix. If ascending is true, it's the smallest
	 * 			among keys with the prefix; otherwise it's the largest.
	 */
	@SuppressWarnings("unchecked")
	private Row prefixRowKey(Row prefixKey, boolean ascending) {
		int numPrefixKeys = prefixKey == null ? 0 : prefixKey.getArity();
		Preconditions.checkArgument(numPrefixKeys < numKeyColumns);

		Map currentMap = keyIndexMap;

		// Traverse to the last level map.
		for (int i = 0; i < numKeyColumns - 2; i++) {
			if (currentMap == null || currentMap.isEmpty()) {
				return null;
			}

			if (i < numPrefixKeys) {
				// get the next map according to the prefix key.
				currentMap = (Map) currentMap.get(prefixKey.getField(i));
			} else {
				// get the next map according to ascending.
				if (currentMap instanceof TreeMap) {
					currentMap = ascending ?
						(Map) ((TreeMap) currentMap).firstEntry().getValue() :
						(Map) ((TreeMap) currentMap).lastEntry().getValue();
				} else {
					currentMap = (Map) currentMap.values().iterator().next();
				}
			}
		}

		if (currentMap == null || currentMap.isEmpty()) {
			return null;
		}

		// get the key set.
		Set<Row> rowSet;
		if (numPrefixKeys == numKeyColumns - 1) {
			rowSet = (Set) currentMap.get(prefixKey.getField(numPrefixKeys - 1));
		} else {
			if (currentMap instanceof TreeMap) {
				rowSet = ascending ?
					(Set) ((TreeMap) currentMap).firstEntry().getValue() :
					(Set) ((TreeMap) currentMap).lastEntry().getValue();
			} else {
				rowSet = (Set) currentMap.values().iterator().next();
			}
		}

		if (rowSet == null || rowSet.isEmpty()) {
			return null;
		}

		if (rowSet instanceof TreeSet) {
			return ascending ? ((TreeSet<Row>) rowSet).first() : ((TreeSet<Row>) rowSet).last();
		} else {
			return rowSet.iterator().next();
		}
	}

	private void checkPrefixKey(Row prefixKey) {
		Preconditions.checkArgument(prefixKey == null || prefixKey.getArity() < numKeyColumns);
	}

	@VisibleForTesting
	public Map getKeyIndexMap() {
		return keyIndexMap;
	}

	/**
	 * Iterator over the keys in the given group where heading keys are equal to the
	 * given prefix and the succeeding key locates in the given range. The number of
	 * prefix keys is one less than the number of keys in a row.
	 */
	private class OneLevelIterator implements Iterator<Row> {

		/**
		 * The number of column keys in the row.
		 */
		private final int numKeyColumns;

		/**
		 * The heading keys of the rows to be iterated over.
		 */
		private final Row prefixKey;

		/**
		 * The low endpoint (inclusive) of the keys succeeding the given prefix
		 * in the rows to be iterated over.
		 */
		private final Object startKey;

		/**
		 * The high endpoint (exclusive) of the keys succeeding the given prefix
		 * in the rows to be iterated over.
		 */
		private final Object endKey;

		/**
		 * Set where keys with the given prefix keys stored in.
		 */
		Set<Row> rowSet;

		/**
		 * Iterator over the rows where heading keys are equal to the given prefix
		 * and the succeeding key locates in the given range.
		 */
		Iterator<Row> rowSetIterator;

		OneLevelIterator(
			int numKeyColumns,
			Row prefixKey,
			Object startKey,
			Object endKey
		) {
			int numPrefixKeys = prefixKey == null ? 0 : prefixKey.getArity();
			Preconditions.checkArgument(numPrefixKeys == numKeyColumns - 1);

			this.numKeyColumns = numKeyColumns;
			this.prefixKey = prefixKey;
			this.startKey = startKey;
			this.endKey = endKey;

			build();
		}

		@SuppressWarnings("unchecked")
		private void build() {
			// Traverse to the map that maps the last prefix key to the set of keys
			// with the given prefix keys.
			int numPrefixKeys = prefixKey.getArity();
			Map currentMap = MultipleKeysIndex.this.keyIndexMap;
			for (int i = 0; i < numPrefixKeys - 1; i++) {
				currentMap = (Map) currentMap.get(prefixKey.getField(i));
				if (currentMap == null) {
					break;
				}
			}

			if (currentMap == null) {
				rowSetIterator = Collections.emptyIterator();
				return;
			}

			// Get the set of keys with the give prefix keys.
			Set<Row> rowSet = (Set<Row>) currentMap.get(prefixKey.getField(numPrefixKeys - 1));
			if (rowSet == null) {
				rowSetIterator = Collections.emptyIterator();
				return;
			}

			this.rowSet = rowSet;

			if (rowSet instanceof SortedSet) {
				if (startKey != null && endKey != null) {
					Row startRow = new Row(numKeyColumns);
					Row endRow = new Row(numKeyColumns);
					startRow.setField(numKeyColumns -1, startKey);
					endRow.setField(numKeyColumns -1, endKey);
					rowSetIterator = ((SortedSet) rowSet).subSet(startRow, endRow).iterator();
				} else if (startKey != null) {
					Row startRow = new Row(numKeyColumns);
					startRow.setField(numKeyColumns -1, startKey);
					rowSetIterator = ((SortedSet) rowSet).tailSet(startRow).iterator();
				} else if (endKey != null) {
					Row endRow = new Row(numKeyColumns);
					endRow.setField(numKeyColumns -1, endKey);
					rowSetIterator = ((SortedSet) rowSet).headSet(endRow).iterator();
				} else {
					rowSetIterator = rowSet.iterator();
				}
			} else {
				Preconditions.checkState(startKey == null && endKey == null);
				rowSetIterator = rowSet.iterator();
			}
		}

		@Override
		public boolean hasNext() {
			return rowSetIterator.hasNext();
		}

		@Override
		public Row next() {
			return rowSetIterator.next();
		}

		@Override
		public void remove() {
			rowSetIterator.remove();

			if (rowSet.isEmpty()) {
				// For multiple keys index, one level iterator must have at least one prefix key.
				Map currentMap = MultipleKeysIndex.this.keyIndexMap;
				ArrayList<Map> internalMaps = new ArrayList<>(prefixKey.getArity());

				internalMaps.add(currentMap);
				for (int i = 0; i < prefixKey.getArity() - 1; i++) {
					currentMap = (Map) currentMap.get(prefixKey.getField(i));
					internalMaps.add(currentMap);
				}

				for (int i = prefixKey.getArity() - 1; i >= 0; i--) {
					Object columnKey = prefixKey.getField(i);
					currentMap = internalMaps.get(i);
					currentMap.remove(columnKey);
					if (!currentMap.isEmpty()) {
						break;
					}
				}
			}
		}
	}

	/**
	 * Iterator over the keys in the given group where heading keys are equal to the
	 * given prefix and the succeeding key locates in the given range. The number of
	 * prefix keys is at least two less than the number of keys in a row.
	 */
	private class MultipleLevelIterator implements Iterator<Row> {

		/**
		 * The heading keys of the rows to be iterated over.
		 */
		private final Row prefixKey;

		/**
		 * The low endpoint (inclusive) of the keys succeeding the given prefix
		 * in the rows to be iterated over.
		 */
		private final Object startKey;

		/**
		 * The high endpoint (exclusive) of the keys succeeding the given prefix
		 * in the rows to be iterated over.
		 */
		private final Object endKey;

		/**
		 * The number of prefix keys;
		 */
		private final int numPrefixKeys;

		/**
		 * The number of levels for map iterators.
		 */
		private final int numLevelOfMapIterators;

		/**
		 * Map iterators for each level.
		 */
		private final IteratorNode[] levelIterators;

		/**
		 * Iterator over the row set.
		 */
		private Iterator<Row> rowSetIterator;

		MultipleLevelIterator(
			int numKeyColumns,
			Row prefixKey,
			Object startKey,
			Object endKey
		) {
			this.numPrefixKeys = prefixKey == null ? 0 : prefixKey.getArity();
			Preconditions.checkArgument(numPrefixKeys < numKeyColumns - 1);

			this.prefixKey = prefixKey;
			this.startKey = startKey;
			this.endKey = endKey;
			this.numLevelOfMapIterators = numKeyColumns - numPrefixKeys - 1;
			this.levelIterators = new IteratorNode[numLevelOfMapIterators];

			build();
		}

		@SuppressWarnings("unchecked")
		private void build() {
			// Traverse to the map that contains all keys with the given prefix keys..
			Map currentMap = MultipleKeysIndex.this.keyIndexMap;
			for (int i = 0; i < numPrefixKeys; i++) {
				currentMap = (Map) currentMap.get(prefixKey.getField(i));
				if (currentMap == null) {
					break;
				}
			}

			// Iterator for the map after the prefix keys.
			if (currentMap == null) {
				levelIterators[0] = new IteratorNode(Collections.emptyIterator());
			} else if (currentMap instanceof TreeMap) {
				Iterator iterator;
				if (startKey != null && endKey != null) {
					iterator = ((TreeMap) currentMap).subMap(startKey, endKey).entrySet().iterator();
				} else if (startKey != null) {
					iterator = ((TreeMap) currentMap).tailMap(startKey).entrySet().iterator();
				} else if (endKey != null) {
					iterator = ((TreeMap) currentMap).headMap(endKey).entrySet().iterator();
				} else {
					iterator = currentMap.entrySet().iterator();
				}
				levelIterators[0] = new IteratorNode(iterator);
			} else {
				Preconditions.checkState(startKey == null && endKey == null);
				levelIterators[0] = new IteratorNode(currentMap.entrySet().iterator());
			}

			// Iterators for internal maps.
			for (int i = 1; i < numLevelOfMapIterators; i++) {
				IteratorNode parentNode = levelIterators[i - 1];
				Iterator<Map.Entry> parentIterator = parentNode.iterator;
				Iterator<Map.Entry> currentIterator;
				if (parentIterator.hasNext()) {
					Map.Entry entry = parentIterator.next();
					parentNode.currentKey = entry.getKey();
					parentNode.currentValue = entry.getValue();
					currentIterator = ((Map) entry.getValue()).entrySet().iterator();
				} else {
					currentIterator = Collections.emptyIterator();
				}
				levelIterators[i] = new IteratorNode(currentIterator);
			}

			// Iterator for the row set.
			IteratorNode node = levelIterators[numLevelOfMapIterators - 1];
			Iterator<Map.Entry<Object, Set>> iterator = node.iterator;
			if (iterator.hasNext()) {
				Map.Entry<Object, Set> entry = iterator.next();
				node.currentKey = entry.getKey();
				node.currentValue = entry.getValue();
				rowSetIterator = entry.getValue().iterator();
			} else {
				rowSetIterator = Collections.emptyIterator();
			}
		}


		@Override
		public boolean hasNext() {
			for (IteratorNode node : levelIterators) {
				if (node.iterator.hasNext()) {
					return true;
				}
			}
			return rowSetIterator.hasNext();
		}

		@Override
		public Row next() {
			if (!rowSetIterator.hasNext()) {
				// Find the first level that has next element.
				int levelIndex = -1;
				for (int i = numLevelOfMapIterators - 1; i >= 0; i--) {
					if (levelIterators[i].iterator.hasNext()) {
						levelIndex = i;
						break;
					}
				}
				if (levelIndex == -1) {
					throw new NoSuchElementException();
				}

				// Rebuild iterators.
				IteratorNode currentNode = levelIterators[levelIndex];
				for (int i = levelIndex; i < numLevelOfMapIterators - 1; i++) {
					IteratorNode nextNode = levelIterators[i + 1];
					Map.Entry<Object, Map> entry = (Map.Entry) currentNode.iterator.next();
					currentNode.currentKey = entry.getKey();
					currentNode.currentValue = entry.getValue();
					nextNode.iterator = entry.getValue().entrySet().iterator();
					currentNode = nextNode;
				}

				// Iterator for the row set.
				IteratorNode node = levelIterators[numLevelOfMapIterators - 1];
				Iterator<Map.Entry<Object, Set>> iterator = node.iterator;
				Map.Entry<Object, Set> entry = iterator.next();
				node.currentKey = entry.getKey();
				node.currentValue = entry.getValue();
				rowSetIterator = entry.getValue().iterator();
			}

			return rowSetIterator.next();
		}

		@Override
		public void remove() {
			rowSetIterator.remove();

			IteratorNode lastNode = levelIterators[numLevelOfMapIterators - 1];
			// Check whether row set is empty.
			Set rowSet = (Set) lastNode.currentValue;
			if (!rowSet.isEmpty()) {
				return;
			}
			lastNode.iterator.remove();
			lastNode.currentKey = null;
			// release the references to the removed set.
			lastNode.currentValue = null;
			rowSetIterator = Collections.emptyIterator();

			boolean foundNonEmptyLevel = false;
			for (int i = numLevelOfMapIterators - 2; i >= 0; i--) {
				IteratorNode node = levelIterators[i];
				if (((Map) node.currentValue).isEmpty()) {
					node.iterator.remove();
					node.currentKey = null;
					// release the references to the removed map.
					node.currentValue = null;
					levelIterators[i + 1].iterator = Collections.emptyIterator();
				} else {
					foundNonEmptyLevel = true;
					break;
				}
			}

			// No need to prune backwards if we found a non-empty level.
			if (foundNonEmptyLevel) {
				return;
			}

			// Traverse the root map to prune all empty internal node
			if (numPrefixKeys > 0) {
				Map currentMap = MultipleKeysIndex.this.keyIndexMap;
				ArrayList<Map> internalMaps = new ArrayList<>(numPrefixKeys + 1);

				internalMaps.add(currentMap);
				for (int i = 0; i < numPrefixKeys; i++) {
					currentMap = (Map) currentMap.get(prefixKey.getField(i));
					internalMaps.add(currentMap);
				}

				for (int i = numPrefixKeys; i > 0; i--) {
					if (!internalMaps.get(i).isEmpty()) {
						break;
					}
					Object columnKey = prefixKey.getField(i -1);
					internalMaps.get(i - 1).remove(columnKey);
				}
			}
		}
	}

	private static class IteratorNode {

		Iterator iterator;

		Object currentKey;

		Object currentValue;

		IteratorNode(Iterator iterator) {
			this.iterator = iterator;
		}
	}

}
