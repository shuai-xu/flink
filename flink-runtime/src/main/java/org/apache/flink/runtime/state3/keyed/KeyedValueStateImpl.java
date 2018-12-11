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

package org.apache.flink.runtime.state3.keyed;

import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link KeyedValueState} based on a {@link StateStorage}
 * The pairs are formatted as {K -> V}, and are partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <V> Type of the values in the state.
 */
public final class KeyedValueStateImpl<K, V> implements KeyedValueState<K, V> {

	/**
	 * The descriptor of this state.
	 */
	private final KeyedValueStateDescriptor descriptor;

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
	public KeyedValueStateImpl(
		KeyedValueStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
	}

	@Override
	public KeyedValueStateDescriptor getDescriptor() {
		return descriptor;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key) {
		if (key == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				return stateStorage.get(key) != null;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public V get(K key) {
		return getOrDefault(key, null);
	}

	@Override
	public V getOrDefault(K key, V defaultValue) {
		if (key == null) {
			return defaultValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				V value = (V) stateStorage.get(key);
				return value == null ? defaultValue : value;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Map<K, V> getAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		try {
			if (stateStorage.lazySerde()) {
				Map<K, V> results = new HashMap<>();
				for (K key : keys) {
					if (key == null) {
						continue;
					}
					V value = (V) stateStorage.get(key);
					if (value != null) {
						results.put(key, value);
					}
				}
				return results;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void remove(K key) {
		if (key == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				stateStorage.remove(key);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}

		if (stateStorage.lazySerde()) {
			for (K key : keys) {
				remove(key);
			}
		} else {
			if (stateStorage.supportMultiColumnFamilies()) {

			} else {

			}
		}
	}

	@Override
	public void put(K key, V value) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				stateStorage.put(key, value);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				for (Map.Entry<? extends K, ? extends V> entry : pairs.entrySet()) {
					stateStorage.put(entry.getKey(), entry.getValue());
				}
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Map<K, V> getAll() {

		try {
			if (stateStorage.lazySerde()) {
				Map<K, V> results = new HashMap<>();
				Iterator<Pair<K, V>> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<K, V> pair = iterator.next();
					results.put(pair.getKey(), pair.getValue());
				}
				return results;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll() {
		if (stateStorage.lazySerde()) {
			((HeapStateStorage) stateStorage).removeAll();
		} else {

		}
	}

	@Override
	public Iterable<K> keys() {

		return new Iterable<K>() {
			@Override
			public Iterator<K> iterator() {
				try {
					if (stateStorage.lazySerde()) {
						Iterator<Pair<K, V>> iterator = stateStorage.iterator();
						return new Iterator<K>() {

							@Override
							public boolean hasNext() {
								return iterator.hasNext();
							}

							@Override
							public K next() {
								return iterator.next().getKey();
							}

							@Override
							public void remove() {
								iterator.remove();
							}
						};
					} else {
						return null;
					}
				} catch (Exception e) {
					throw new StateAccessException(e);
				}
			}
		};
	}

}
