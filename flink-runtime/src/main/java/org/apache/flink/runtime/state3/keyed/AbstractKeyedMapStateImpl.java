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

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.AbstractInternalStateBackend;
import org.apache.flink.runtime.state3.BatchPutWrapper;
import org.apache.flink.runtime.state3.GroupIterator;
import org.apache.flink.runtime.state3.StateIteratorUtil;
import org.apache.flink.runtime.state3.StateSerializerUtil;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.StorageInstance;
import org.apache.flink.runtime.state3.StorageIterator;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.runtime.state3.StateSerializerUtil.KEY_END_BYTE;

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

	/**
	 * Serialized bytes of current state name.
	 */
	protected byte[] stateNameByte;

	/**
	 * The key serializer of current state.
	 */
	protected TypeSerializer<K> keySerializer;

	/**
	 * The mapKey serializer of current state.
	 */
	protected TypeSerializer<MK> mapKeySerializer;

	/**
	 * The mapValue serializer of current state.
	 */
	protected TypeSerializer<MV> mapValueSerializer;

	/**
	 * The state backend who created the current state.
	 */
	private AbstractInternalStateBackend internalStateBackend;

	protected ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
	protected DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

	/** partitioner used to generate key group. **/
	protected static final HashPartitioner PARTITIONER = HashPartitioner.INSTANCE;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param internalStateBackend The state backend who creates the current state.
	 * @param stateStorage The state storage where the values are stored.
	 */
	AbstractKeyedMapStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		StateStorage stateStorage) {
		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
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
			Iterator<Map.Entry<MK, MV>> iterator = iterator(key);
			return iterator.hasNext();
		}
	}

	@Override
	public boolean contains(K key, MK mapKey) {
		if (key == null || mapKey == null) {
			return false;
		}

		if (stateStorage.lazySerde()) {
			Map<MK, MV> map = get(key);
			return map != null && map.containsKey(mapKey);
		} else {
			try {
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedMapState(
					key,
					keySerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);

				return stateStorage.get(serializedKey) != null;
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
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
				Iterator<Map.Entry<MK, MV>> iterator = iterator(key);
				M result = createMap();
				while (iterator.hasNext()) {
					Map.Entry<MK, MV> entry = iterator.next();
					if (entry.getKey() != null && entry.getValue() != null) {
						result.put(entry.getKey(), entry.getValue());
					}
				}
				return result.isEmpty() ? defaultValue : result;
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
		if (key == null || mapKey == null) {
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedMapState(
					key,
					keySerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
				if (serializedValue == null) {
					return defaultMapValue;
				} else {
					return StateSerializerUtil.getDeserializeSingleValue(serializedValue, mapValueSerializer);
				}
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

		M results = createMap();
		if (stateStorage.lazySerde()) {
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
			for (MK mapKey : mapKeys) {
				if (mapKey != null) {
					MV value = get(key, mapKey);
					if (value != null) {
						results.put(mapKey, value);
					}
				}
			}
			return results;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<K, M> getAll(Map<K, ? extends Collection<? extends MK>> map) {
		if (map == null || map.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<K, M> results = new HashMap<>();
		if (stateStorage.lazySerde()) {
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
		} else {
			for (Map.Entry<K, ? extends Collection<? extends MK>> entry : map.entrySet()) {
				K key = entry.getKey();
				Collection<? extends MK> mapKeys = entry.getValue();
				M resultMap = getAll(key, mapKeys);
				if (!resultMap.isEmpty()) {
					results.put(key, getAll(key, mapKeys));
				}
			}
		}
		return results;
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedMapState(
					key,
					keySerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				byte[] serializedValue = StateSerializerUtil.getSerializeSingleValue(mapValue, mapValueSerializer);
				stateStorage.put(serializedKey, serializedValue);
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
				StorageInstance instance = stateStorage.getStorageInstance();
				try (BatchPutWrapper batchPutWrapper = instance.getBatchPutWrapper()){
					int group = getKeyGroup(key);

					outputStream.reset();
					StateSerializerUtil.writeGroup(outputStream, group);
					// we need to use state name to distinguish different column families.
					if (!stateStorage.supportMultiColumnFamilies()) {
						outputStream.write(stateNameByte);
					}
					StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
					int prefixLength = outputStream.getPosition();

					for (Map.Entry<? extends MK, ? extends MV> entry : mappings.entrySet()) {
						Preconditions.checkNotNull(entry.getKey(), "Can not insert null key to mapstate");
						Preconditions.checkNotNull(entry.getValue(), "Can not insert null value to mapstate");

						outputStream.setPosition(prefixLength);
						StateSerializerUtil.serializeItemWithKeyPrefix(outputView, entry.getKey(), mapKeySerializer);
						byte[] byteKey = outputStream.toByteArray();
						byte[] byteValue = StateSerializerUtil.getSerializeSingleValue(entry.getValue(), mapValueSerializer);

						batchPutWrapper.put(byteKey, byteValue);
					}
				}
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

		for (Map.Entry entry : map.entrySet()) {
			addAll((K) entry.getKey(), (Map) entry.getValue());
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
				Iterator<Map.Entry<MK, MV>> iterator = iterator(key);
				while (iterator.hasNext()) {
					Map.Entry<MK, MV> entry = iterator.next();
					remove(key, entry.getKey());
				}
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedMapState(
					key,
					keySerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				stateStorage.remove(serializedKey);
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
				for (MK mapKey : mapKeys) {
					if (mapKey != null) {
						remove(key, mapKey);
					}
				}
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

		for (Map.Entry<? extends K, ? extends Collection<? extends MK>> entry : map.entrySet()) {
			removeAll(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> iterator(K key) {
		Preconditions.checkNotNull(key);

		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptyIterator() : map.entrySet().iterator();
		} else {
			try {
				byte[] keyPrefix = StateSerializerUtil.getSerializedPrefixKeyForKeyedMapState(
					key,
					keySerializer,
					null,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				StorageIterator<byte[], byte[]> iterator = stateStorage.prefixIterator(keyPrefix);

				return new Iterator<Map.Entry<MK, MV>>() {
					Pair<byte[], byte[]> pair = null;

					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public Map.Entry<MK, MV> next() {
						pair = iterator.next();
						return new Map.Entry<MK, MV>() {
							@Override
							public MK getKey() {
								try {
									return StateSerializerUtil.getDeserializedMapKeyForKeyedMapState(
												pair.getKey(),
												keySerializer,
												mapKeySerializer,
												stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
								} catch (Exception e) {
									throw new StateAccessException(e);
								}
							}

							@Override
							public MV getValue() {
								try {
									return StateSerializerUtil.getDeserializeSingleValue(pair.getValue(), mapValueSerializer);
								} catch (Exception e) {
									throw new StateAccessException(e);
								}
							}

							@Override
							public MV setValue(MV value) {
								try {
									return StateSerializerUtil.getDeserializeSingleValue(pair.setValue(StateSerializerUtil.getSerializeSingleValue(value, mapValueSerializer)), mapValueSerializer);
								} catch (Exception e) {
									throw new StateAccessException(e);
								}
							}
						};
					}
				};
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Iterable<Map.Entry<MK, MV>> entries(K key) {
		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptySet() : map.entrySet();
		} else {
			return new Iterable<Map.Entry<MK, MV>>() {
				@Override
				public Iterator<Map.Entry<MK, MV>> iterator() {
					final Iterator<Map.Entry<MK, MV>> innerIter = AbstractKeyedMapStateImpl.this.iterator(key);
					return new Iterator<Map.Entry<MK, MV>>() {
						@Override
						public boolean hasNext() {
							return innerIter.hasNext();
						}

						@Override
						public Map.Entry<MK, MV> next() {
							return innerIter.next();
						}

						@Override
						public void remove() {
							innerIter.remove();
						}
					};
				}
			};
		}
	}

	@Override
	public Iterable<MK> mapKeys(K key) {
		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptySet() : map.keySet();
		} else {
			return new Iterable<MK>() {
				@Override
				public Iterator<MK> iterator() {
					final Iterator<Map.Entry<MK, MV>> innerIter = AbstractKeyedMapStateImpl.this.iterator(key);
					return new Iterator<MK>() {
						@Override
						public boolean hasNext() {
							return innerIter.hasNext();
						}

						@Override
						public MK next() {
							return innerIter.next().getKey();
						}

						@Override
						public void remove() {
							innerIter.remove();
						}
					};
				}
			};
		}
	}

	@Override
	public Iterable<MV> mapValues(K key) {
		if (stateStorage.lazySerde()) {
			Map map = get(key);
			return map == null ? Collections.emptySet() : map.values();
		} else {
			return new Iterable<MV>() {
				@Override
				public Iterator<MV> iterator() {
					final Iterator<Map.Entry<MK, MV>> innerIter = AbstractKeyedMapStateImpl.this.iterator(key);
					return new Iterator<MV>() {
						@Override
						public boolean hasNext() {
							return innerIter.hasNext();
						}

						@Override
						public MV next() {
							return innerIter.next().getValue();
						}

						@Override
						public void remove() {
							innerIter.remove();
						}
					};
				}
			};
		}
	}

	@Override
	public Map<K, M> getAll() {
		try {
			Map<K, M> result = new HashMap<>();
			if (stateStorage.lazySerde()) {
				Iterator<Pair<K, M>> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<K, M> pair = iterator.next();
					result.put(pair.getKey(), pair.getValue());
				}

			} else {
				if (!stateStorage.supportMultiColumnFamilies() && internalStateBackend.getStateStorages().size() > 1) {
					for (Integer group : internalStateBackend.getGroups()) {
						outputStream.reset();
						StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
						byte[] groupPrefix = outputStream.toByteArray();
						outputStream.write(KEY_END_BYTE);
						byte[] groupPrefixEnd = outputStream.toByteArray();

						StorageIterator<byte[], byte[]> iterator = (StorageIterator<byte[], byte[]>) stateStorage.subIterator(groupPrefix, groupPrefixEnd);
						iteratorToMap(iterator, result, stateNameByte.length);
					}
				} else {
					StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();
					iteratorToMap(iterator, result, stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
				}
			}

			return result;
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll() {
		if (stateStorage.lazySerde()) {
			((HeapStateStorage) stateStorage).removeAll();
		} else {
			try {
				if (!stateStorage.supportMultiColumnFamilies() && internalStateBackend.getStateStorages().size() > 1) {
					for (Integer group : internalStateBackend.getGroups()) {
						outputStream.reset();
						StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
						byte[] groupPrefix = outputStream.toByteArray();
						outputStream.write(KEY_END_BYTE);
						byte[] groupPrefixEnd = outputStream.toByteArray();

						StorageIterator iterator = stateStorage.subIterator(groupPrefix, groupPrefixEnd);
						while (iterator.hasNext()) {
							iterator.next();
							iterator.remove();
						}
					}
				} else {
					StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();
					while (iterator.hasNext()) {
						iterator.next();
						iterator.remove();
					}
				}
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
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
						if (!stateStorage.supportMultiColumnFamilies() && internalStateBackend.getStateStorages().size() > 1) {
							Collection<Iterator<Pair<byte[], byte[]>>> groupIterators = new ArrayList<>();
							for (Integer group : internalStateBackend.getGroups()) {
								outputStream.reset();
								StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
								byte[] groupPrefix = outputStream.toByteArray();
								outputStream.write(KEY_END_BYTE);
								byte[] groupPrefixEnd = outputStream.toByteArray();

								StorageIterator iterator = stateStorage.subIterator(groupPrefix, groupPrefixEnd);
								groupIterators.add(iterator);
							}
							GroupIterator groupIterator = new GroupIterator(groupIterators);
							return StateIteratorUtil.createKeyIterator(groupIterator, keySerializer, stateNameByte.length);
						} else {
							StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();

							return StateIteratorUtil.createKeyIterator(iterator, keySerializer, stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
						}
					}
				} catch (Exception e) {
					throw new StateAccessException(e);
				}
			}
		};
	}

	protected  <K> int getKeyGroup(K key) {
		return PARTITIONER.partition(key, internalStateBackend.getNumGroups());
	}

	private void iteratorToMap(StorageIterator<byte[], byte[]> iterator, Map<K, M> result, int stateNameByteLength) throws IOException {
		while (iterator.hasNext()) {
			Pair<byte[], byte[]> pair = iterator.next();
			K key = StateSerializerUtil.getDeserializedKeyForKeyedMapState(pair.getKey(),
				keySerializer, stateNameByteLength);
			MK mapKey = StateSerializerUtil.getDeserializedMapKeyForKeyedMapState(pair.getKey(),
				keySerializer, mapKeySerializer, stateNameByteLength);
			MV mapValue = StateSerializerUtil.getDeserializeSingleValue(pair.getValue(), mapValueSerializer);

			M map = result.get(key);
			if (map == null) {
				map = createMap();
				result.put(key, map);
			}
			map.put(mapKey, mapValue);
		}
	}
}
