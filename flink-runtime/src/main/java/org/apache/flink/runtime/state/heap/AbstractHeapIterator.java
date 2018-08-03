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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A helper class to iterate over all entries in a nested map with the given
 * prefix keys.
 */
abstract class AbstractHeapIterator implements Iterator<Pair<Row, Row>> {

	/**
	 * The map to be iterated over.
	 */
	private final Map rootMap;

	/**
	 * The number of keys in the nested map.
	 */
	private final int numKeys;

	/**
	 * The values of the prefix keys.
	 */
	@Nullable
	private final Row prefixKeys;

	/**
	 * The map under the prefix keys, which is composed of all the suffix keys
	 * to be iterated.
	 */
	private final Map suffixRootMap;

	/**
	 * The iterating states of the suffix keys.
	 */
	private final Node[] suffixNodes;

	/**
	 * Constructor with the map to be iterated over, the number of the keys in
	 * the map and the values of the prefix keys.
	 *
	 * @param rootMap The map to be iterated over.
	 * @param numKeys The number of the keys in the map.
	 * @param prefixKeys The values of the prefix keys.
	 */
	AbstractHeapIterator(Map rootMap, int numKeys, Row prefixKeys) {
		Preconditions.checkArgument(numKeys > 0);
		Preconditions.checkArgument(prefixKeys == null || prefixKeys.getArity() < numKeys);

		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();

		this.rootMap = rootMap;
		this.numKeys = numKeys;
		this.prefixKeys = prefixKeys;
		this.suffixNodes = new Node[numKeys - numPrefixKeys];

		Map currentMap = rootMap;
		for (int index = 0; index < numPrefixKeys; ++index) {
			currentMap = currentMap == null ?
				null : (Map) currentMap.get(prefixKeys.getField(index));
		}
		this.suffixRootMap = currentMap;
	}

	/**
	 * Build the initial traversal path. This method must be called in the
	 * constructor of subclasses.
	 */
	@SuppressWarnings("unchecked")
	void initialize() {
		// Traverse all internal nodes from the root node of the suffix tree.
		Map currentMap = suffixRootMap;
		for (int index = 0; index < suffixNodes.length - 1; ++index) {

			Iterator<Map.Entry<Object, Map>> iterator = currentMap == null ?
				Collections.emptyIterator() :
				buildSuffixIterator(index, currentMap);

			suffixNodes[index] = new Node(iterator);

			if (iterator.hasNext()) {
				Map.Entry<Object, Map> entry = iterator.next();
				suffixNodes[index].key = entry.getKey();
				suffixNodes[index].value = entry.getValue();
			}

			currentMap = (Map) suffixNodes[index].value;
		}

		// We cannot call the next operation for the leaf node here.
		Iterator<Map.Entry<Object, Row>> lastIterator = currentMap == null ?
			Collections.emptyIterator() :
			buildSuffixIterator(suffixNodes.length - 1, currentMap);
		suffixNodes[suffixNodes.length - 1] = new Node(lastIterator);
	}

	/**
	 * Creates an iterator over the entries in the given map which locates at
	 * the given level of the traversal tree.
	 *
	 * @param index The index of the map in the traversal path.
	 * @param map The internal node to be iterated.
	 * @return An iterator over the entries in the map.
	 */
	abstract Iterator buildSuffixIterator(int index, Map map);

	@Override
	public boolean hasNext() {
		for (Node suffixNode : suffixNodes) {
			if (suffixNode.iterator.hasNext()) {
				return true;
			}
		}

		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Pair<Row, Row> next() {

		// Traverse backwards to find the first level to perform the next
		// operation
		int startNextIndex = 0;
		for (int index = suffixNodes.length - 1; index > 0; --index) {
			Node suffixNode = suffixNodes[index];
			if (suffixNode.iterator.hasNext()) {
				startNextIndex = index;
				break;
			}
		}

		// No entries are left if there is no entry in the root level.
		if (startNextIndex == 0 && !suffixNodes[0].iterator.hasNext()) {
			throw new NoSuchElementException();
		}

		// Perform the next operation at the start level and rebuild the
		// iterator at succeeding levels. Because we prune all empty internal
		// nodes when removing entries, we cannot come across empty internal
		// nodes here.
		for (int index = startNextIndex; index < suffixNodes.length - 1; ++index) {
			Node suffixNode = suffixNodes[index];
			Node childSuffixNode = suffixNodes[index + 1];

			Iterator<Map.Entry<Object, Map>> iterator =
				(Iterator<Map.Entry<Object, Map>>) suffixNode.iterator;
			Map.Entry<Object, Map> nextEntry = iterator.next();

			suffixNode.key = nextEntry.getKey();
			suffixNode.value = nextEntry.getValue();
			childSuffixNode.iterator = buildSuffixIterator(index + 1, nextEntry.getValue());
		}

		Node lastSuffixNode = suffixNodes[suffixNodes.length - 1];
		Map.Entry<Object, Row> suffixEntry =
			(Map.Entry<Object, Row>) lastSuffixNode.iterator.next();
		lastSuffixNode.key = suffixEntry.getKey();
		lastSuffixNode.value = suffixEntry.getValue();

		// Construct the key with the prefix keys and the suffix keys in current
		// traversal path.
		Row key = new Row(numKeys);

		if (prefixKeys != null) {
			for (int index = 0; index < prefixKeys.getArity(); ++index) {
				key.setField(index, prefixKeys.getField(index));
			}
		}

		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		for (int index = 0; index < suffixNodes.length; ++index) {
			key.setField(index + numPrefixKeys, suffixNodes[index].key);
		}

		return new HeapPair(key, suffixEntry);
	}

	@Override
	public void remove() {
		// Remove the current entry from the leaf map.
		Node lastSuffixNode = suffixNodes[suffixNodes.length - 1];
		lastSuffixNode.iterator.remove();

		// Traverse backwards to remove all empty maps from their parents.
		for (int index = suffixNodes.length - 2; index >= 0; --index) {
			Node suffixNode = suffixNodes[index];
			if (((Map) suffixNode.value).isEmpty()) {
				suffixNode.iterator.remove();
			}
		}

		// All the entries under the prefix keys have been removed. Traverse
		// the root map to prune all empty internal node.
		if (suffixRootMap.isEmpty() &&
			prefixKeys != null && prefixKeys.getArity() > 0) {
			Map currentMap = rootMap;
			List<Map> internalMaps = new ArrayList<>(prefixKeys.getArity() - 1);
			for (int index = 0; index < prefixKeys.getArity() - 1; ++index) {
				internalMaps.add(currentMap);

				Object columnKey = prefixKeys.getField(index);
				currentMap = (Map) currentMap.get(columnKey);
				Preconditions.checkState(currentMap != null);
			}
			internalMaps.add(currentMap);

			Object lastPrefixKey = prefixKeys.getField(prefixKeys.getArity() - 1);
			currentMap.remove(lastPrefixKey);

			// Prune empty internal nodes
			for (int index = internalMaps.size() - 1; index >= 1; index--) {
				if (internalMaps.get(index).isEmpty()) {
					Object parentPrefixKey = prefixKeys.getField(index - 1);
					internalMaps.get(index - 1).remove(parentPrefixKey);
				}
			}
		}
	}

	//--------------------------------------------------------------------------

	/**
	 * A helper class to maintain the iterating state at a level.
	 */
	private static class Node {
		/**
		 * The iterator at the the current level.
		 */
		Iterator iterator;

		/**
		 * The current key pointed by the iterator.
		 */
		Object key;

		/**
		 * The current value pointed by the iterator.
		 */
		Object value;

		/**
		 * Constructor with the given iterator.
		 *
		 * @param iterator The iterator at the current level.
		 */
		Node(Iterator iterator) {
			this.iterator = iterator;
		}
	}

	//--------------------------------------------------------------------------

	/**
	 * An {@link Pair} which is composed of a key and a value.
	 *
	 * @param <K> Type of the keys in the state.
	 * @param <V> Type of the values in the state.
	 */
	private static class HeapPair<K, V> implements Pair<K, V> {

		/** The key of the pair. */
		private final K key;

		/** The entry of state value in heap. */
		private final Map.Entry<K, V> heapEntry;

		/**
		 * Constructor with the given key and value.
		 *
		 * @param entry The corresponding entry backed by {@link HeapInternalState} of the pair.
		 */
		HeapPair(K key, Map.Entry<K, V> entry) {
			this.key = key;
			this.heapEntry = entry;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return heapEntry.getValue();
		}

		@Override
		public V setValue(V value) {
			Preconditions.checkNotNull(value);
			return heapEntry.setValue(value);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			HeapPair<?, ?> that = (HeapPair<?, ?>) o;

			return Objects.equals(key, that.key) &&
				Objects.equals(heapEntry, that.heapEntry);
		}

		@Override
		public int hashCode() {
			int result = Objects.hashCode(key);
			result = 31 * result + Objects.hashCode(heapEntry);
			return result;
		}

		@Override
		public String toString() {
			return "HeapPair{" +
				"key=" + getKey() +
				", value=" + getValue() +
				"}";
		}
	}
}
