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
 * The base implementation of {@link AbstractKeyedMapState} backed by a
 * {@link StateStorage}. The pairs are formatted as {(K, MK) -> MV}.
 * Because the pairs are partitioned by K, the mappings under
 * the same key will be assigned to the same group and can be easily
 * retrieved with the prefix iterator on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 * @param <M> Type of the maps in the state.
 */
abstract class AbstractKeyedMapStateImpl<K, MK, MV, M extends Map<MK, MV>>
	implements AbstractKeyedMapState<K, MK, MV, M> {

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
	AbstractKeyedMapStateImpl(StateStorage stateStorage) {
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
	public boolean contains(K key) {
		if (key == null) {
			return false;
		}

		if (stateStorage.lazySerde()) {
			Map<MK, MV> map = get(key);
			return map != null;
		} else {
			return false;
		}
	}

	@Override
	public boolean contains(K key, MK mapKey) {
		if (key == null) {
			return false;
		}

		if (stateStorage.lazySerde()) {
			Map<MK, MV> map = get(key);
			return map != null && map.containsKey(mapKey);
		} else {
			return false;
		}
	}

	@Override
	public M get(K key) {
		return getOrDefault(key, null);
	}

	@Override
	public M getOrDefault(K key, M defaultValue) {
		if (key == null) {
			return defaultValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				M map = (M) stateStorage.get(key);
				return map == null ? defaultValue : map;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public MV get(K key, MK mapKey) {
		return getOrDefault(key, mapKey, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public MV getOrDefault(K key, MK mapKey, MV defaultMapValue) {
		if (key == null) {
			return defaultMapValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				Map<MK, MV> map = (Map<MK, MV>) stateStorage.get(key);
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
	public Map<K, M> getAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<K, M> results = new HashMap<>();
		for (K key : keys) {
			if (key == null) {
				continue;
			}

			M result = get(key);
			if (result != null && !result.isEmpty()) {
				results.put(key, result);
			}
		}

		return results;
	}

	@SuppressWarnings("unchecked")
	@Override
	public M getAll(K key, Collection<? extends MK> mapKeys) {
		if (key == null || mapKeys == null || mapKeys.isEmpty()) {
			return createMap();
		}

		if (stateStorage.lazySerde()) {
			M results = createMap();
			M map = get(key);
			if (map != null) {
				for (MK mapKey : mapKeys) {
					if (mapKey == null) {
						continue;
					}
					MV value = map.get(mapKey);
					if (value != null) {
						results.put(mapKey, value);
					}
				}
			}
			return results;
		} else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<K, M> getAll(Map<K, ? extends Collection<? extends MK>> map) {
		if (map == null || map.isEmpty()) {
			return Collections.emptyMap();
		}

		if (stateStorage.lazySerde()) {
			Map<K, M> results = new HashMap<>();
			for (Map.Entry<K, ? extends Collection<? extends MK>> entry : map.entrySet()) {
				K key = entry.getKey();
				M keyMap = get(key);
				if (keyMap != null) {
					// lazy create
					M subMap = null;
					for (MK mk : entry.getValue()) {
						MV mv = keyMap.get(mk);
						if (mv != null) {
							if (subMap == null) {
								subMap = createMap();
								results.put(key, subMap);
							}
							subMap.put(mk, mv);
						}
					}
				}
			}
			return results;
		} else {
			return null;
		}
	}

	@Override
	public void add(K key, MK mapKey, MV mapValue) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				Map<MK, MV> map = (Map<MK, MV>) stateStorage.get(key);
				if (map == null) {
					map = createMap();
					stateStorage.put(key, map);
				}
				map.put(mapKey, mapValue);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void addAll(K key, Map<? extends MK, ? extends MV> mappings) {
		Preconditions.checkNotNull(key);

		if (mappings == null || mappings.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				Map<MK, MV> map = (Map<MK, MV>) stateStorage.get(key);
				if (map == null) {
					map = createMap();
					stateStorage.put(key, map);
				}
				map.putAll(mappings);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void addAll(Map<? extends K, ? extends Map<? extends MK, ? extends MV>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		if (stateStorage.lazySerde()) {
			for (Map.Entry entry : map.entrySet()) {
				addAll((K) entry.getKey(), (Map) entry.getValue());
			}
		} else {

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
	public void remove(K key, MK mapKey) {
		if (key == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				Map map = (Map) stateStorage.get(key);
				if (map == null) {
					return;
				}
				map.remove(mapKey);
				if (map.isEmpty()) {
					remove(key);
				}
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

		for (K key : keys) {
			remove(key);
		}
	}

	@Override
	public void removeAll(K key, Collection<? extends MK> mapKeys) {
		if (key == null || mapKeys == null || mapKeys.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				Map map = (Map) stateStorage.get(key);
				if (map != null) {
					for (MK mapKey : mapKeys) {
						map.remove(mapKey);
					}
					if (map.isEmpty()) {
						remove(key);
					}
				}
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(Map<? extends K, ? extends Collection<? extends MK>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		if (stateStorage.lazySerde()) {
			for (Map.Entry<? extends K, ? extends Collection<? extends MK>> entry : map.entrySet()) {
				removeAll(entry.getKey(), entry.getValue());
			}
		} else {

		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> iterator(K key) {
		Preconditions.checkNotNull(key);

		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptyIterator() : map.entrySet().iterator();
		} else {
			return null;
		}
	}

	@Override
	public Iterable<Map.Entry<MK, MV>> entries(K key) {
		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptySet() : map.entrySet();
		} else {
			return null;
		}
	}

	@Override
	public Iterable<MK> mapKeys(K key) {
		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptySet() : map.keySet();
		} else {
			return null;
		}
	}

	@Override
	public Iterable<MV> mapValues(K key) {
		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptySet() : map.values();
		} else {
			return null;
		}
	}

	@Override
	public Map<K, M> getAll() {
		try {
			if (stateStorage.lazySerde()) {
				Map<K, M> result = new HashMap<>();
				Iterator<Pair<K, M>> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<K, M> pair = iterator.next();
					result.put(pair.getKey(), pair.getValue());
				}
				return result;

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
						Iterator<Pair<K, Map>> iterator = stateStorage.iterator();
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
