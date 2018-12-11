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

import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link SubKeyedValueState} based on an {@link StateStorage}
 * The pairs in the state storage are formatted as {(K, N) -> V}, and are
 * partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <V> Type of the values in the state.
 */
public final class SubKeyedValueStateImpl<K, N, V> implements SubKeyedValueState<K, N, V> {

	/**
	 * The descriptor of this state.
	 */
	private final SubKeyedValueStateDescriptor descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	/**
	 * Constructor with the state storage to store the values.
	 *
	 * @param descriptor The descriptor of this state.
	 * @param stateStorage The state storage where the values are stored.
	 */
	public SubKeyedValueStateImpl(
		SubKeyedValueStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
	}

	@Override
	public SubKeyedValueStateDescriptor getDescriptor() {
		return descriptor;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				return heapStateStorage.get(key) != null;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public V get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V getOrDefault(K key, N namespace, V defaultValue) {
		if (key == null || namespace == null) {
			return defaultValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				V value = (V) heapStateStorage.get(key);
				return value == null ? defaultValue : value;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, V> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).getAll(key);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void remove(K key, N namespace) {
		if (key == null || namespace == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.remove(key);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(K key) {
		if (key == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				((HeapStateStorage) stateStorage).removeAll(key);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void put(K key, N namespace, V value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.put(key, value);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<N> iterator(K key) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).namespaceIterator(key);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

}
