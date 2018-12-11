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

package org.apache.flink.runtime.state3.subkeyed;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An implementation of {@link SubKeyedSortedMapState} backed by a state storage.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class SubKeyedSortedMapStateImpl<K, N, MK, MV>
	extends AbstractSubKeyedMapStateImpl<K, N, MK, MV, SortedMap<MK, MV>>
	implements SubKeyedSortedMapState<K, N, MK, MV> {

	/**
	 * The descriptor of current state.
	 */
	private SubKeyedSortedMapStateDescriptor stateDescriptor;

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param stateStorage The state storage where the mappings are stored.
	 */
	public SubKeyedSortedMapStateImpl(
		SubKeyedSortedMapStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		super(stateStorage);

		this.stateDescriptor = Preconditions.checkNotNull(descriptor);
	}

	@Override
	public SubKeyedSortedMapStateDescriptor getDescriptor() {
		return stateDescriptor;
	}

	@SuppressWarnings("unchecked")
	@Override
	SortedMap<MK, MV> createMap() {
		Comparator<MK> comparator = stateDescriptor.getComparator();
		return new TreeMap<>(comparator);
	}

	//--------------------------------------------------------------------------

	@Override
	public Map.Entry<MK, MV> firstEntry(K key, N namespace) {
		if (key == null || namespace == null) {
			return null;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.firstEntry();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Map.Entry<MK, MV> lastEntry(K key, N namespace) {
		if (key == null || namespace == null) {
			return null;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.lastEntry();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> headIterator(K key, N namespace, MK endMapKey) {
		if (key == null || namespace == null || endMapKey == null) {
			return Collections.emptyIterator();
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.headMap(endMapKey).entrySet().iterator();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> tailIterator(K key, N namespace, MK startMapKey) {
		if (key == null || namespace == null || startMapKey == null) {
			return Collections.emptyIterator();
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.tailMap(startMapKey).entrySet().iterator();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> subIterator(K key, N namespace, MK startMapKey, MK endMapKey) {
		if (key == null || namespace == null || startMapKey == null || endMapKey == null) {
			return Collections.emptyIterator();
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.subMap(startMapKey, endMapKey).entrySet().iterator();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}
}

