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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.AbstractInternalStateBackend;
import org.apache.flink.runtime.state3.StateSerializerUtil;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An implementation of {@link KeyedSortedMapState} backed by a state storage.
 *
 * @param <K> Type of the keys in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class KeyedSortedMapStateImpl<K, MK, MV>
	extends AbstractKeyedMapStateImpl<K, MK, MV, SortedMap<MK, MV>>
	implements KeyedSortedMapState<K, MK, MV> {

	/**
	 * The descriptor of current state.
	 */
	private KeyedSortedMapStateDescriptor<K, MK, MV> stateDescriptor;

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param internalStateBackend The state backend who creates the current state.
	 * @param descriptor The descriptor of current state.
	 * @param stateStorage The state storage where the mappings are stored.
	 */
	public KeyedSortedMapStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		KeyedSortedMapStateDescriptor<K, MK, MV> descriptor,
		StateStorage stateStorage) {
		super(internalStateBackend, stateStorage);

		this.stateDescriptor = Preconditions.checkNotNull(descriptor);
		this.keySerializer = descriptor.getKeySerializer();
		this.mapKeySerializer = descriptor.getMapKeySerializer();
		this.mapValueSerializer = descriptor.getMapValueSerializer();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			StringSerializer.INSTANCE.serialize(descriptor.getName(), new DataOutputViewStreamWrapper(out));
			stateNameByte = out.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public KeyedSortedMapStateDescriptor getDescriptor() {
		return stateDescriptor;
	}

	@SuppressWarnings("unchecked")
	@Override
	SortedMap<MK, MV> createMap() {
		Comparator<MK> comparator = stateDescriptor.getMapKeyComparator();
		return new TreeMap<>(comparator);
	}

	//--------------------------------------------------------------------------

	@Override
	public Map.Entry<MK, MV> firstEntry(K key) {
		if (key == null) {
			return null;
		}

		if (stateStorage.lazySerde()) {
			TreeMap<MK, MV> map = (TreeMap<MK, MV>) get(key);

			return map == null ? null : map.firstEntry();
		} else {
			try {
				byte[] prefixKey = StateSerializerUtil.getSerializedPrefixKeyForKeyedMapState(
					key,
					keySerializer,
					null,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				Pair<byte[], byte[]> firstEntry = stateStorage.firstEntry(prefixKey);
				if (firstEntry == null || !isEntryWithPrefix(prefixKey, prefixKey.length, firstEntry.getKey())) {
					return null;
				}
				return new Map.Entry<MK, MV>() {
					@Override
					public MK getKey() {
						try {
							return StateSerializerUtil.getDeserializedMapKeyForKeyedMapState(
									firstEntry.getKey(),
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
							return StateSerializerUtil.getDeserializeSingleValue(
									firstEntry.getValue(),
									mapValueSerializer);
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV setValue(MV value) {
						return null;
					}
				};
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Map.Entry<MK, MV> lastEntry(K key) {
		if (key == null) {
			return null;
		}

		if (stateStorage.lazySerde()) {
			TreeMap<MK, MV> map = (TreeMap<MK, MV>) get(key);

			return map == null ? null : map.lastEntry();
		} else {
			try {
				byte[] prefixKey = StateSerializerUtil.getSerializedPrefixKeyEndForKeyedMapState(
					key,
					keySerializer,
					null,
					mapKeySerializer,
					getKeyGroup(key),
					stateStorage.supportMultiColumnFamilies() ? null : stateNameByte);
				Pair<byte[], byte[]> lastEntry = stateStorage.lastEntry(prefixKey);
				if (lastEntry == null || !isEntryWithPrefix(prefixKey, prefixKey.length - 1, lastEntry.getKey())) {
					return null;
				} else {
					return new Map.Entry<MK, MV>() {
						@Override
						public MK getKey() {
							try {
								return StateSerializerUtil.getDeserializedMapKeyForKeyedMapState(
									lastEntry.getKey(),
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
								return StateSerializerUtil.getDeserializeSingleValue(
									lastEntry.getValue(),
									mapValueSerializer);
							} catch (Exception e) {
								throw new StateAccessException(e);
							}
						}

						@Override
						public MV setValue(MV value) {
							try {
								byte[] oldValue = lastEntry.setValue(StateSerializerUtil.getSerializeSingleValue(value, mapValueSerializer));
								if (oldValue == null) {
									return null;
								} else {
									return StateSerializerUtil.getDeserializeSingleValue(oldValue, mapValueSerializer);
								}
							} catch (Exception e) {
								throw new StateAccessException(e);
							}
						}
					};
				}
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> headIterator(K key, MK endMapKey) {
		if (key == null || endMapKey == null) {
			return Collections.emptyIterator();
		}

		if (stateStorage.lazySerde()) {
			SortedMap<MK, MV> map = get(key);
			return map == null ? Collections.emptyIterator() : map.headMap(endMapKey).entrySet().iterator();
		} else {
			try {
				int group = getKeyGroup(key);
				outputStream.reset();
				StateSerializerUtil.writeGroup(outputStream, group);
				if (!stateStorage.supportMultiColumnFamilies()) {
					outputView.write(stateNameByte);
				}
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
				byte[] prefixKey = outputStream.toByteArray();

				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, endMapKey, mapKeySerializer);
				byte[] prefixKeyEnd = outputStream.toByteArray();
				return subIterator(prefixKey, prefixKeyEnd);
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> tailIterator(K key, MK startMapKey) {
		if (key == null || startMapKey == null) {
			return Collections.emptyIterator();
		}

		if (stateStorage.lazySerde()) {
			SortedMap<MK, MV> map = get(key);
			return map == null ? Collections.emptyIterator() : map.tailMap(startMapKey).entrySet().iterator();
		} else {
			try {
				int group = getKeyGroup(key);
				outputStream.reset();
				StateSerializerUtil.writeGroup(outputStream, group);
				if (!stateStorage.supportMultiColumnFamilies()) {
					outputView.write(stateNameByte);
				}
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
				int keyPosition = outputStream.getPosition();
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, startMapKey, mapKeySerializer);
				byte[] prefixKey = outputStream.toByteArray();

				outputStream.setPosition(keyPosition);
				outputStream.write(StateSerializerUtil.KEY_END_BYTE);
				byte[] prefixKeyEnd = outputStream.toByteArray();
				return subIterator(prefixKey, prefixKeyEnd);
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> subIterator(K key, MK startMapKey, MK endMapKey) {
		if (key == null || startMapKey == null || endMapKey == null) {
			return Collections.emptyIterator();
		}

		if (stateStorage.lazySerde()) {
			SortedMap<MK, MV> map = get(key);
			return map == null ? Collections.emptyIterator() : map.subMap(startMapKey, endMapKey).entrySet().iterator();
		} else {
			try {
				int group = getKeyGroup(key);
				outputStream.reset();
				StateSerializerUtil.writeGroup(outputStream, group);
				if (!stateStorage.supportMultiColumnFamilies()) {
					outputView.write(stateNameByte);
				}
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, key, keySerializer);
				int keyPosition = outputStream.getPosition();
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, startMapKey, mapKeySerializer);
				byte[] prefixKey = outputStream.toByteArray();

				outputStream.setPosition(keyPosition);
				StateSerializerUtil.serializeItemWithKeyPrefix(outputView, endMapKey, mapKeySerializer);
				byte[] prefixKeyEnd = outputStream.toByteArray();
				return subIterator(prefixKey, prefixKeyEnd);
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	private boolean isEntryWithPrefix(byte[] prefixKey, int length, byte[] actualKey) {
		// minus 1 for KEY_END_BYTE.
		if (actualKey.length < length) {
			return false;
		}
		int commonLength = Math.min(length, actualKey.length);
		for (int i = 0; i < commonLength; ++i) {
			int leftByte = prefixKey[i] & 0xFF;
			int rightByte = actualKey[i] & 0xFF;

			if (leftByte < rightByte) {
				return false;
			}
		}

		return true;
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
									if (nextByteEntry == null || nextByteEntry.getKey() == null) {
										return null;
									} else {
										return StateSerializerUtil.getDeserializedMapKeyForKeyedMapState(
											nextByteEntry.getKey(),
											keySerializer,
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

