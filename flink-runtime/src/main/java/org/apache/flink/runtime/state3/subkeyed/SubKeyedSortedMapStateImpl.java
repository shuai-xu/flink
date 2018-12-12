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
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.AbstractInternalStateBackend;
import org.apache.flink.runtime.state3.StateSerializerUtil;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
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
	 * @param backend The state backend who creates the current state.
	 * @param descriptor The descriptor of current state.
	 * @param stateStorage The state storage where the mappings are stored.
	 */
	public SubKeyedSortedMapStateImpl(
		AbstractInternalStateBackend backend,
		SubKeyedSortedMapStateDescriptor<K, N, MK, MV> descriptor,
		StateStorage stateStorage
	) {
		super(backend, stateStorage);

		this.stateDescriptor = Preconditions.checkNotNull(descriptor);
		this.keySerializer = descriptor.getKeySerializer();
		this.namespaceSerializer = descriptor.getNamespaceSerializer();
		this.mapKeySerializer = descriptor.getMapKeySerializer();
		this.mapValueSerializer = descriptor.getMapValueSerializer();
		try {
			outputStream.reset();
			StringSerializer.INSTANCE.serialize(descriptor.getName(), outputView);
			stateNameByte = outputStream.toByteArray();
		} catch (Exception e) {
			throw new SerializationException(e);
		}
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
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.firstEntry();
			} else {
				byte[] prefixKey = StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				Pair<byte[], byte[]> firstEntry = stateStorage.firstEntry(prefixKey);
				return new Map.Entry<MK, MV>() {
					@Override
					public MK getKey() {
						try {
							if (firstEntry == null || firstEntry.getKey() == null) {
								return null;
							} else {
								return StateSerializerUtil.getDeserializedMapKeyForSubKeyedMapState(
									firstEntry.getKey(),
									keySerializer,
									namespaceSerializer,
									mapKeySerializer,
									stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
							}
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV getValue() {
						try {
							if (firstEntry == null || firstEntry.getValue() == null) {
								return null;
							} else {
								return StateSerializerUtil.getDeserializeSingleValue(
									firstEntry.getValue(),
									mapValueSerializer);
							}
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV setValue(MV value) {
						return null;
					}
				};
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
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.lastEntry();
			} else {
				byte[] prefixKey = StateSerializerUtil.getSerializedPrefixKeyEndForSubKeyedMapState(
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					null,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				Pair<byte[], byte[]> firstEntry = stateStorage.lastEntry(prefixKey);
				return new Map.Entry<MK, MV>(){
					@Override
					public MK getKey() {
						try {
							if (firstEntry == null || firstEntry.getKey() == null) {
								return null;
							} else {
								return StateSerializerUtil.getDeserializedMapKeyForSubKeyedMapState(
									firstEntry.getKey(),
									keySerializer,
									namespaceSerializer,
									mapKeySerializer,
									stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
							}
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV getValue() {
						try {
							if (firstEntry == null || firstEntry.getValue() == null) {
								return null;
							} else {
								return StateSerializerUtil.getDeserializeSingleValue(
									firstEntry.getValue(),
									mapValueSerializer);
							}
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV setValue(MV value) {
						return null;
					}
				};
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
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.headMap(endMapKey).entrySet().iterator();
			} else {
				int group = getKeyGroup(key);
				outputStream.reset();
				StateSerializerUtil.writeGroup(outputStream, group);
				if (!stateStorage.supportMultiColumnFamilies()) {
					outputView.write(stateNameByte);
				}
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, namespace, namespaceSerializer);
				byte[] prefixKey = outputStream.toByteArray();
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, endMapKey, mapKeySerializer);
				byte[] prefixKeyEnd = outputStream.toByteArray();
				return subIterator(prefixKey, prefixKeyEnd);
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
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.tailMap(startMapKey).entrySet().iterator();
			} else {
				int group = getKeyGroup(key);
				outputStream.reset();
				StateSerializerUtil.writeGroup(outputStream, group);
				if (!stateStorage.supportMultiColumnFamilies()) {
					outputView.write(stateNameByte);
				}
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, namespace, namespaceSerializer);
				int namespacePosition = outputStream.getPosition();
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, startMapKey, mapKeySerializer);
				byte[] prefixKey = outputStream.toByteArray();

				outputStream.setPosition(namespacePosition);
				outputStream.write(StateSerializerUtil.KEY_END_BYTE);
				byte[] prefixKeyEnd = outputStream.toByteArray();
				return subIterator(prefixKey, prefixKeyEnd);
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
				((HeapStateStorage) stateStorage).setCurrentNamespace(namespace);
				TreeMap map = (TreeMap) stateStorage.get(key);
				return map == null ? null : map.subMap(startMapKey, endMapKey).entrySet().iterator();
			} else {
				int group = getKeyGroup(key);
				outputStream.reset();
				StateSerializerUtil.writeGroup(outputStream, group);
				if (!stateStorage.supportMultiColumnFamilies()) {
					outputView.write(stateNameByte);
				}
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, namespace, namespaceSerializer);
				int namespacePosition = outputStream.getPosition();
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, startMapKey, mapKeySerializer);
				byte[] prefixKey = outputStream.toByteArray();

				outputStream.setPosition(namespacePosition);
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, endMapKey, mapKeySerializer);
				byte[] prefixKeyEnd = outputStream.toByteArray();
				return subIterator(prefixKey, prefixKeyEnd);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	private Iterator<Map.Entry<MK, MV>> subIterator(byte[] prefixKeyStart, byte[] prefixKeyEnd) {
		if (stateStorage.lazySerde()) {
			return null;
		} else {
			try {
				Iterator<Pair<byte[], byte[]>> subIterator = stateStorage.subIterator(prefixKeyStart, prefixKeyEnd);
				return new Iterator<Map.Entry<MK, MV>>(){
					@Override
					public boolean hasNext() {
						return subIterator.hasNext();
					}

					@Override
					public Map.Entry<MK, MV> next() {
						Pair<byte[], byte[]> nextByteEntry = subIterator.next();
						return new Map.Entry<MK, MV>() {
							@Override
							public MK getKey() {
								try {
									return StateSerializerUtil.getDeserializedMapKeyForSubKeyedMapState(
										nextByteEntry.getKey(),
										keySerializer,
										namespaceSerializer,
										mapKeySerializer,
										stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
								} catch (Exception e) {
									throw new StateAccessException(e);
								}
							}

							@Override
							public MV getValue() {
								try {
									if (nextByteEntry == null || nextByteEntry.getValue() == null) {
										return null;
									} else {
										return StateSerializerUtil.getDeserializeSingleValue(
											nextByteEntry.getValue(),
											mapValueSerializer);
									}
								} catch (Exception e) {
									throw new StateAccessException(e);
								}
							}

							@Override
							public MV setValue(MV value) {
								Preconditions.checkNotNull(value);
								try {
									byte[] oldValue = nextByteEntry.setValue(
										StateSerializerUtil.getSerializeSingleValue(
											value,
											mapValueSerializer)
									);
									if (oldValue == null) {
										return null;
									} else {
										return StateSerializerUtil.getDeserializeSingleValue(
											oldValue,
											mapValueSerializer);
									}
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
}

