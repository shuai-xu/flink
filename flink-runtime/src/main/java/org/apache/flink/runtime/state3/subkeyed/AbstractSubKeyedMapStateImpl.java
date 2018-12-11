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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * The base implementation of {@link AbstractSubKeyedMapState} backed by an a state storage.
 * The pairs in the storage are formatted as {(K, N, MK) -> MV}. Because the pairs are
 * partitioned by K, the mappings under the same key will be assigned to the same group and
 * can be easily retrieved with the prefix iterator on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 * @param <M> Type of the maps in the state.
 */
abstract class AbstractSubKeyedMapStateImpl<K, N, MK, MV, M extends Map<MK, MV>> implements AbstractSubKeyedMapState<K, N, MK, MV, M> {

	/**
	 * The state storage where the values are stored.
	 */
	protected final StateStorage stateStorage;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param stateStorage The state storage where the values are stored.
	 */
	AbstractSubKeyedMapStateImpl(StateStorage stateStorage) {
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
	}

	/**
	 * Creates a map under a key.
	 *
	 * @return A map under a key.
	 */
	abstract M createMap();

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key, N namespace, MK mapKey) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				Map<MK, MV> map = get(key, namespace);
				return map != null && map.containsKey(mapKey);
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		if (stateStorage.lazySerde()) {
			Map<MK, MV> map = get(key, namespace);
			return map != null;
		} else {
			return false;
		}
	}

	@Override
	public MV get(K key, N namespace, MK mapKey) {
		return getOrDefault(key, namespace, mapKey, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public MV getOrDefault(K key, N namespace, MK mapKey, MV defaultMapValue) {
		if (key == null || namespace == null) {
			return defaultMapValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map<MK, MV> map = (Map<MK, MV>) heapStateStorage.get(key);
				if (map == null) {
					return defaultMapValue;
				}
				MV value = map.get(mapKey);
				return value == null ? defaultMapValue : value;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public M get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@Override
	public M getOrDefault(K key, N namespace, M defaultMap) {
		if (key == null || namespace == null) {
			return defaultMap;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				M map = (M) heapStateStorage.get(key);
				return map == null ? defaultMap : map;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public M getAll(K key, N namespace, Collection<? extends MK> mapKeys) {
		if (key == null || namespace == null || mapKeys == null || mapKeys.isEmpty()) {
			return null;
		}

		try {
			if (stateStorage.lazySerde()) {
				M map = get(key, namespace);

				if (map == null) {
					return null;
				}

				// lazy create
				M results = null;
				for (MK mapKey : mapKeys) {
					MV value = map.get(mapKey);
					if (value != null) {
						if (results == null) {
							results = createMap();
						}
						results.put(mapKey, value);
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

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, M> getAll(K key) {
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
	public void add(K key, N namespace, MK mapKey, MV mapValue) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map<MK, MV> map = (Map<MK, MV>) heapStateStorage.get(key);
				if (map == null) {
					map = createMap();
					heapStateStorage.put(key, map);
				}
				map.put(mapKey, mapValue);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void addAll(K key, N namespace, Map<? extends MK, ? extends MV> mappings) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		if (mappings == null || mappings.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map<MK, MV> map = (Map<MK, MV>) heapStateStorage.get(key);
				if (map == null) {
					map = createMap();
					heapStateStorage.put(key, map);
				}
				map.putAll(mappings);
			} else {

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
	public void remove(K key, N namespace, MK mapKey) {
		if (key == null || namespace == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				if (map != null) {
					map.remove(mapKey);

					if (map.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(K key, N namespace, Collection<? extends MK> mapKeys) {
		if (key == null || namespace == null || mapKeys.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				if (map != null) {
					for (MK mk : mapKeys) {
						map.remove(mk);
					}

					if (map.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
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

		if (stateStorage.lazySerde()) {
			((HeapStateStorage) stateStorage).removeAll(key);
		} else {

		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> iterator(K key, N namespace) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptyIterator() : map.entrySet().iterator();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterable<Map.Entry<MK, MV>> entries(K key, N namespace) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptySet() : map.entrySet();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterable<MK> keys(K key, N namespace) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptySet() : map.keySet();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterable<MV> values(K key, N namespace) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptySet() : map.values();
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<N> iterator(K key) {
		Preconditions.checkNotNull(key);

		if (stateStorage.lazySerde()) {
			return ((HeapStateStorage) stateStorage).namespaceIterator(key);
		} else {
			return null;
		}
	}

}
