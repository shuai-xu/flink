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
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.AbstractInternalStateBackend;
import org.apache.flink.runtime.state3.BatchPutWrapper;
import org.apache.flink.runtime.state3.StateSerializerUtil;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.StorageInstance;
import org.apache.flink.runtime.state3.StorageIterator;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
	private final KeyedValueStateDescriptor<K, V> descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	/**
	 * Serializer of key for current state.
	 */
	private TypeSerializer<K> keySerializer;

	/**
	 * Serializer of value for current state.
	 */
	private TypeSerializer<V> valueSerializer;

	/**
	 * Serialized bytes of current state name.
	 */
	private byte[] stateNameByte;

	/**
	 * State backend who creates current state.
	 */
	private AbstractInternalStateBackend internalStateBackend;

	/** partitioner used to generate key group. */
	private static final HashPartitioner partitioner = HashPartitioner.INSTANCE;
	/**
	 * Constructor with the state storage to store the values.
	 *
	 * @param internalStateBackend The state backend who creates the current state.
	 * @param descriptor The descriptor of this state.
	 * @param stateStorage The state storage where the values are stored.
	 */
	public KeyedValueStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		KeyedValueStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);

		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
		this.keySerializer = descriptor.getKeySerializer();
		this.valueSerializer = descriptor.getValueSerializer();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			StringSerializer.INSTANCE.serialize(descriptor.getName(), new DataOutputViewStreamWrapper(out));
			stateNameByte = out.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(key,
					keySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				return stateStorage.get(serializedKey) != null;
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(key,
					keySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
				if (serializedValue == null) {
					return defaultValue;
				} else {
					return StateSerializerUtil.getDeserializeSingleValue(serializedValue, valueSerializer);
				}
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
			Map<K, V> results = new HashMap<>();

			if (stateStorage.lazySerde()) {
				for (K key : keys) {
					if (key == null) {
						continue;
					}
					V value = (V) stateStorage.get(key);
					if (value != null) {
						results.put(key, value);
					}
				}
			} else {
				for (K key : keys) {
					if (key == null) {
						continue;
					}
					byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(key,
						keySerializer,
						getKeyGroup(key),
						stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
					byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
					if (serializedValue != null) {
						results.put(key, StateSerializerUtil.getDeserializeSingleValue(serializedValue, valueSerializer));
					}
				}
			}
			return results;

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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(key,
					keySerializer,
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
	public void put(K key, V value) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				stateStorage.put(key, value);
			} else {
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(key,
					keySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				byte[] serializedValue = StateSerializerUtil.getSerializeSingleValue(value, valueSerializer);
				stateStorage.put(serializedKey, serializedValue);
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
				StorageInstance instance = stateStorage.getStorageInstance();
				try (BatchPutWrapper batchPutWrapper = instance.getBatchPutWrapper()) {
					for (Map.Entry<? extends K, ? extends V> entry : pairs.entrySet()) {
						K key = entry.getKey();
						byte[] byteKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
							key,
							keySerializer,
							getKeyGroup(key),
							stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
						byte[] byteValue = StateSerializerUtil.getSerializeSingleValue(entry.getValue(), valueSerializer);
						batchPutWrapper.put(byteKey, byteValue);
					}
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Map<K, V> getAll() {

		try {
			Map<K, V> results = new HashMap<>();

			if (stateStorage.lazySerde()) {
				Iterator<Pair<K, V>> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<K, V> pair = iterator.next();
					results.put(pair.getKey(), pair.getValue());
				}
			} else {
				StorageIterator iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<byte[], byte[]> bytePair = (Pair<byte[], byte[]>) iterator.next();
					K key = StateSerializerUtil.getDeserializedKeyForKeyedValueState(
						bytePair.getKey(),
						keySerializer,
						stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
					V value = StateSerializerUtil.getDeserializeSingleValue(bytePair.getValue(), valueSerializer);
					results.put(key, value);
				}
			}
			return results;

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
				StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<byte[], byte[]> pair = iterator.next();
					stateStorage.remove(pair.getKey());
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
						StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();

						return new Iterator<K>() {
							@Override
							public boolean hasNext() {
								return iterator.hasNext();
							}

							@Override
							public K next() {
								byte[] serializedKey = iterator.next().getKey();
								try {
									return StateSerializerUtil.getDeserializedKeyForKeyedValueState(
										serializedKey,
										keySerializer,
										stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
								} catch (IOException e) {
									throw new StateAccessException(e);
								}
							}

							@Override
							public void remove() {
								iterator.remove();
							}
						};
					}
				} catch (Exception e) {
					throw new StateAccessException(e);
				}
			}
		};
	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalStateBackend.getNumGroups());
	}
}
