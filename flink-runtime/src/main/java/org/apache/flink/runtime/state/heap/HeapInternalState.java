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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalColumnDescriptor;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.PrefixPartitionIterator;
import org.apache.flink.runtime.state.SortedPrefixPartitionIterator;
import org.apache.flink.types.DefaultPair;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An implementation of {@link InternalState} which is backed by
 * {@link HeapInternalStateBackend}.
 */
final class HeapInternalState implements InternalState {

	/**
	 * The backend by which the state is backed.
	 */
	private final HeapInternalStateBackend backend;

	/**
	 * The descriptor of the state.
	 */
	private final InternalStateDescriptor descriptor;

	/** group of next operation, update by {@code setGroup} **/
	private int currentGroup;

	/**
	 * Constructor with the given backend and the descriptor.
	 *
	 * @param backend The backend by which the state is backed.
	 * @param descriptor The descriptor of the state.
	 */
	HeapInternalState(HeapInternalStateBackend backend, InternalStateDescriptor descriptor) {
		Preconditions.checkNotNull(backend);
		Preconditions.checkNotNull(descriptor);

		this.backend = backend;
		this.descriptor = descriptor;
	}

	@Override
	public InternalStateDescriptor getDescriptor() {
		return descriptor;
	}

	@Override
	public int getNumGroups() {
		return backend.getNumGroups();
	}

	@Override
	public GroupSet getPartitionGroups() {
		return backend.getGroups();
	}

	//------------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public Row get(Row key) {
		if (key == null) {
			return null;
		}

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());

		Map currentMap = backend.getRootMap(descriptor, currentGroup, false);
		if (currentMap == null) {
			return null;
		}

		for (int index = 0; index < key.getArity() - 1; ++index) {
			Object columnKey = key.getField(index);

			currentMap = (Map) currentMap.get(columnKey);
			if (currentMap == null) {
				return null;
			}
		}

		Object lastColumnKey = key.getField(key.getArity() - 1);
		return (Row) currentMap.get(lastColumnKey);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void put(Row key, Row value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());
		Preconditions.checkArgument(value.getArity() == descriptor.getNumValueColumns());

		Map currentMap = backend.getRootMap(descriptor, currentGroup, true);
		Preconditions.checkState(currentMap != null);

		for (int index = 0; index < key.getArity() - 1; ++index) {
			Object columnKey = key.getField(index);

			Map nextMap = (Map) currentMap.get(columnKey);
			if (nextMap == null) {
				InternalColumnDescriptor<?> keyColumnDescriptor =
					descriptor.getKeyColumnDescriptor(index + 1);

				nextMap = keyColumnDescriptor.isOrdered() ?
					new TreeMap(keyColumnDescriptor.getComparator()) :
					new HashMap();

				currentMap.put(columnKey, nextMap);
			}

			currentMap = nextMap;
		}

		Object lastColumnKey = key.getField(key.getArity() - 1);
		currentMap.put(lastColumnKey, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void merge(Row key, Row value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());
		Preconditions.checkArgument(value.getArity() == descriptor.getNumValueColumns());
		Preconditions.checkNotNull(descriptor.getValueMerger());

		Map currentMap = backend.getRootMap(descriptor, currentGroup, true);
		Preconditions.checkState(currentMap != null);

		for (int index = 0; index < key.getArity() - 1; ++index) {
			Object columnKey = key.getField(index);

			Map nextMap = (Map) currentMap.get(columnKey);
			if (nextMap == null) {
				InternalColumnDescriptor<?> keyColumnDescriptor =
					descriptor.getKeyColumnDescriptor(index + 1);

				nextMap = keyColumnDescriptor.isOrdered() ?
					new TreeMap(keyColumnDescriptor.getComparator()) :
					new HashMap();

				currentMap.put(columnKey, nextMap);
			}

			currentMap = nextMap;
		}

		Object lastColumnKey = key.getField(key.getArity() - 1);
		Row oldValue = (Row) currentMap.get(lastColumnKey);
		Row newValue = value;
		if (oldValue != null) {
			newValue = descriptor.getValueMerger().merge(oldValue, value);
		}
		currentMap.put(lastColumnKey, newValue);
	}

	@Override
	public void remove(Row key) {
		if (key == null) {
			return;
		}

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());

		Map currentMap = backend.getRootMap(descriptor, currentGroup, false);
		if (currentMap == null) {
			return;
		}

		List<Map> internalMaps = new ArrayList<>(key.getArity() - 1);
		for (int index = 0; index < key.getArity() - 1; ++index) {
			internalMaps.add(currentMap);

			Object columnKey = key.getField(index);
			currentMap = (Map) currentMap.get(columnKey);
			if (currentMap == null) {
				return;
			}
		}
		internalMaps.add(currentMap);

		Object lastColumnKey = key.getField(key.getArity() - 1);
		currentMap.remove(lastColumnKey);

		// Prune empty internal nodes
		for (int index = internalMaps.size() - 1; index >= 1; index--) {
			if (internalMaps.get(index).isEmpty()) {
				Object parentColumnKey = key.getField(index - 1);
				internalMaps.get(index - 1).remove(parentColumnKey);
			}
		}
	}

	@Override
	public Map<Row, Row> getAll(Collection<Row> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<Row, Row> results = new HashMap<>();

		for (Row key : keys) {
			currentGroup = getGroupForKey(key);
			Row value = get(key);
			if (value != null) {
				results.put(key, value);
			}
		}

		return results;
	}

	@Override
	public void putAll(Map<Row, Row> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			currentGroup = getGroupForKey(pair.getKey());
			put(pair.getKey(), pair.getValue());
		}
	}

	@Override
	public <K, MK, MV> void rawPutAll(K key, Map<MK, MV> maps) {
		for (Map.Entry<MK, MV> entry : maps.entrySet()) {
			Row internalKey = Row.of(key, entry.getKey());
			put(internalKey, Row.of(entry.getValue()));
		}
	}

	@Override
	public void mergeAll(Map<Row, Row> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			currentGroup = getGroupForKey(pair.getKey());
			merge(pair.getKey(), pair.getValue());
		}
	}

	@Override
	public void removeAll(Collection<Row> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}

		for (Row key : keys) {
			currentGroup = getGroupForKey(key);
			remove(key);
		}
	}

	@Override
	public Iterator<Pair<Row, Row>> iterator() {
		Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();

		GroupSet groups = getPartitionGroups();
		for (int group : groups) {
			Map rootMap = backend.getRootMap(descriptor, group, false);
			Iterator<Pair<Row, Row>> groupIterator = rootMap == null ?
				Collections.emptyIterator() : new PrefixHeapIterator(rootMap, descriptor.getNumKeyColumns(), null);
			if (groupIterator.hasNext()) {
				groupIterators.add(groupIterator);
			}
		}

		if (descriptor.getKeyColumnDescriptor(0).isOrdered()) {
			return new SortedPrefixPartitionIterator(groupIterators,
				(Comparator<Object>) descriptor.getKeyColumnDescriptor(0).getComparator(),
				0);
		} else {
			return new PrefixPartitionIterator(groupIterators);
		}
	}

	@Override
	public Iterator<Pair<Row, Row>> prefixIterator(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());

		Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();

		GroupSet groups = getPartitionGroups();
		for (int group : groups) {
			Map rootMap = backend.getRootMap(descriptor, group, false);
			Iterator<Pair<Row, Row>> groupIterator = rootMap == null ?
				Collections.emptyIterator() : new PrefixHeapIterator(rootMap, descriptor.getNumKeyColumns(), prefixKeys);
			groupIterators.add(groupIterator);
		}

		if (descriptor.getKeyColumnDescriptor(numPrefixKeys).isOrdered()) {
			return new SortedPrefixPartitionIterator(groupIterators,
				(Comparator<Object>) getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator(),
				numPrefixKeys);
		} else {
			return new PrefixPartitionIterator(groupIterators);
		}
	}

	@Override
	public Pair<Row, Row> firstPair(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		int numKeys = descriptor.getNumKeyColumns();
		Preconditions.checkArgument(numPrefixKeys < numKeys);

		Comparator<?> keyComparator = getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator();
		Comparator<Object> comparator = keyComparator == null ? null : (Comparator<Object>) keyComparator;

		Pair<Row, Row> firstInternalPair = null;
		Object firstKey = null;
		for (int group : getPartitionGroups()) {
			Pair<Row, Row> groupFirstPair = firstPair(group, prefixKeys);
			if (groupFirstPair != null) {

				if (comparator == null) {
					return groupFirstPair;
				}

				Object groupFirstKey = groupFirstPair.getKey().getField(numPrefixKeys);
				if (firstKey == null || comparator.compare(firstKey, groupFirstKey) > 0) {
					firstInternalPair = groupFirstPair;
					firstKey = groupFirstKey;
				}
			}
		}
		return firstInternalPair;
	}

	@Override
	public Pair<Row, Row> lastPair(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		int numKeys = descriptor.getNumKeyColumns();
		Preconditions.checkArgument(numPrefixKeys < numKeys);

		Comparator<?> keyComparator = getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator();
		Comparator<Object> comparator = keyComparator == null ? null : (Comparator<Object>) keyComparator;

		Pair<Row, Row> lastInternalPair = null;
		Object lastKey = null;
		for (int group : getPartitionGroups()) {
			Pair<Row, Row> groupLastPair = lastPair(group, prefixKeys);
			if (groupLastPair != null) {

				if (comparator == null) {
					return groupLastPair;
				}

				Object groupFirstKey = groupLastPair.getKey().getField(numPrefixKeys);
				if (lastKey == null || comparator.compare(lastKey, groupFirstKey) < 0) {
					lastInternalPair = groupLastPair;
					lastKey = groupFirstKey;
				}
			}
		}
		return lastInternalPair;
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> headIterator(Row prefixKeys, K endKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(descriptor.getKeyColumnDescriptor(numPrefixKeys).isOrdered());

		Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();

		GroupSet groups = getPartitionGroups();
		for (int group : groups) {
			Map rootMap = backend.getRootMap(descriptor, group, false);
			if (rootMap != null) {
				groupIterators.add(new RangeHeapIterator(rootMap, descriptor.getNumKeyColumns(), prefixKeys, null, endKey));
			}
		}

		if (groupIterators.isEmpty()) {
			return Collections.emptyIterator();
		} else {
			return new SortedPrefixPartitionIterator(groupIterators,
				(Comparator<Object>) getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator(),
				numPrefixKeys);
		}
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> tailIterator(Row prefixKeys, K startKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(descriptor.getKeyColumnDescriptor(numPrefixKeys).isOrdered());

		Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();

		GroupSet groups = getPartitionGroups();
		for (int group : groups) {
			Map rootMap = backend.getRootMap(descriptor, group, false);
			if (rootMap != null) {
				groupIterators.add(new RangeHeapIterator(rootMap, descriptor.getNumKeyColumns(), prefixKeys, startKey, null));
			}
		}

		if (groupIterators.isEmpty()) {
			return Collections.emptyIterator();
		} else {
			return new SortedPrefixPartitionIterator(groupIterators,
				(Comparator<Object>) getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator(),
				numPrefixKeys);
		}
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> subIterator(Row prefixKeys, K startKey, K endKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(descriptor.getKeyColumnDescriptor(numPrefixKeys).isOrdered());

		Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();

		GroupSet groups = getPartitionGroups();
		for (int group : groups) {
			Map rootMap = backend.getRootMap(descriptor, group, false);
			if (rootMap != null) {
				groupIterators.add(new RangeHeapIterator(rootMap, descriptor.getNumKeyColumns(), prefixKeys, startKey, endKey));
			}
		}

		if (groupIterators.isEmpty()) {
			return Collections.emptyIterator();
		} else {
			return new SortedPrefixPartitionIterator(groupIterators,
				(Comparator<Object>) getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator(),
				numPrefixKeys);
		}
	}

	@Override
	public void setCurrentGroup(int group) {
		currentGroup = group;
	}

	// Private helper methods ------------------------------------------------------------------------------------------

	private Pair<Row, Row> firstPair(int group, Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		int numKeys = descriptor.getNumKeyColumns();

		Row resultKey = new Row(descriptor.getNumKeyColumns());

		Map currentMap = backend.getRootMap(descriptor, group, false);
		for (int index = 0; index < numPrefixKeys; ++index) {
			if (currentMap == null) {
				return null;
			}

			Object currentKey = prefixKeys.getField(index);

			resultKey.setField(index, currentKey);
			currentMap = (Map) currentMap.get(currentKey);
		}

		if (currentMap == null) {
			return null;
		}

		for (int index = numPrefixKeys; index < numKeys - 1; ++index) {
			Object currentKey = currentMap instanceof SortedMap ?
				((SortedMap) currentMap).firstKey() :
				currentMap.keySet().iterator().next();

			resultKey.setField(index, currentKey);
			currentMap = (Map) currentMap.get(currentKey);
		}

		Object lastKey = currentMap instanceof SortedMap ?
			((SortedMap) currentMap).firstKey() :
			currentMap.keySet().iterator().next();

		resultKey.setField(numKeys - 1, lastKey);
		Row resultValue = (Row) currentMap.get(lastKey);

		return new DefaultPair<>(resultKey, resultValue);
	}

	private Pair<Row, Row> lastPair(int group, Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		int numKeys = descriptor.getNumKeyColumns();

		Row resultKey = new Row(descriptor.getNumKeyColumns());

		Map currentMap = backend.getRootMap(descriptor, group, false);
		for (int index = 0; index < numPrefixKeys; ++index) {
			if (currentMap == null) {
				return null;
			}

			Object currentKey = prefixKeys.getField(index);

			resultKey.setField(index, currentKey);
			currentMap = (Map) currentMap.get(currentKey);
		}

		if (currentMap == null) {
			return null;
		}

		for (int index = numPrefixKeys; index < numKeys - 1; ++index) {
			Object currentKey = currentMap instanceof SortedMap ?
				((SortedMap) currentMap).lastKey() :
				currentMap.keySet().iterator().next();

			resultKey.setField(index, currentKey);
			currentMap = (Map) currentMap.get(currentKey);
		}

		Object lastKey = currentMap instanceof SortedMap ?
			((SortedMap) currentMap).lastKey() :
			currentMap.keySet().iterator().next();

		resultKey.setField(numKeys - 1, lastKey);
		Row resultValue = (Row) currentMap.get(lastKey);

		return new DefaultPair<>(resultKey, resultValue);
	}

	/**
	 * Get the group for the key.
	 *
	 * @param key The key to partition.
	 * @return The group for the key.
	 */
	private int getGroupForKey(Row key) {
		int groupsToPartition = backend.getNumGroups();

		return descriptor.getPartitioner()
			.partition(key, groupsToPartition);
	}
}
