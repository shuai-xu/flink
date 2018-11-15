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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.FieldBasedPartitioner;
import org.apache.flink.runtime.state.InternalColumnDescriptor;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link KeyedValueState} based on an {@link InternalState}
 * The pairs in the internal state are formatted as {K -> V}, and are
 * partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <V> Type of the values in the state.
 */
public final class KeyedValueStateImpl<K, V> implements KeyedValueState<K, V> {

	/** The index of the value in the internal key. */
	private static final int KEY_FIELD_INDEX = 0;

	/** The index of the value in the internal value. */
	private static final int VALUE_FIELD_INDEX = 0;

	/**
	 * The internal state where the values are stored.
	 */
	private final InternalState internalState;

	/** partitioner used to generate key group. */
	private static final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	/**
	 * Constructor with the internal state to store the values.
	 *
	 * @param internalState The internal state where the values are stored.
	 */
	public KeyedValueStateImpl(InternalState internalState) {
		Preconditions.checkNotNull(internalState);
		this.internalState = internalState;
	}

	/**
	 * Creates and returns the descriptor for the internal state backing the
	 * keyed state.
	 *
	 * @param keyedStateDescriptor The descriptor of the keyed state.
	 * @param <K> Type of the keys in the state.
	 * @param <V> Type of the values in the state.
	 * @return The descriptor for the internal state backing the keyed state.
	 */
	public static <K, V> InternalStateDescriptor buildInternalStateDescriptor(
		final KeyedValueStateDescriptor<K, V> keyedStateDescriptor
	) {
		Preconditions.checkNotNull(keyedStateDescriptor);

		return new InternalStateDescriptorBuilder(keyedStateDescriptor.getName())
			.addKeyColumn("key", keyedStateDescriptor.getKeySerializer())
			.addValueColumn("value",
				keyedStateDescriptor.getValueSerializer(),
				keyedStateDescriptor.getValueMerger())
			.setPartitioner(new FieldBasedPartitioner(KEY_FIELD_INDEX, partitioner))
			.getDescriptor();
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key) {
		if (key == null) {
			return false;
		}

		internalState.setCurrentGroup(getKeyGroup(key));
		Row row = internalState.get(Row.of(key));
		return (row != null);
	}

	@Override
	public V get(K key) {
		return getOrDefault(key, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V getOrDefault(K key, V defaultValue) {
		if (key == null) {
			return defaultValue;
		}

		internalState.setCurrentGroup(getKeyGroup(key));
		Row row = internalState.get(Row.of(key));
		return row == null ? defaultValue : (V) row.getField(VALUE_FIELD_INDEX);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<K, V> getAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Collection<Row> internalKeys = new ArrayList<>(keys.size());
		for (K key : keys) {
			if (key != null) {
				internalKeys.add(Row.of(key));
			}
		}

		Map<Row, Row> internalPairs = internalState.getAll(internalKeys);
		Map<K, V> results = new HashMap<>(internalPairs.size());
		for (Map.Entry<Row, Row> internalPair : internalPairs.entrySet()) {
			K key = (K) (internalPair.getKey().getField(KEY_FIELD_INDEX));
			V value = (V) (internalPair.getValue().getField(VALUE_FIELD_INDEX));

			results.put(key, value);
		}

		return results;
	}

	@Override
	public void remove(K key) {
		if (key == null) {
			return;
		}

		internalState.setCurrentGroup(getKeyGroup(key));
		internalState.remove(Row.of(key));
	}

	@Override
	public void removeAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}

		Collection<Row> internalKeys = new ArrayList<>(keys.size());
		for (K key : keys) {
			if (key != null) {
				internalKeys.add(Row.of(key));
			}
		}

		internalState.removeAll(internalKeys);
	}

	@Override
	public void put(K key, V value) {
		Preconditions.checkNotNull(key);

		internalState.setCurrentGroup(getKeyGroup(key));
		internalState.put(Row.of(key), Row.of(value));
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		Map<Row, Row> internalPairs = new HashMap<>(pairs.size());

		for (Map.Entry<? extends K, ? extends V> pair : pairs.entrySet()) {
			K key = pair.getKey();
			Preconditions.checkNotNull(key);

			V value = pair.getValue();
			internalPairs.put(Row.of(key), Row.of(value));
		}

		internalState.putAll(internalPairs);
	}

	@Override
	public Map<K, V> getAll() {
		Map<K, V> result = new HashMap<>();

		Iterator<Pair<Row, Row>> iterator = internalState.iterator();
		while (iterator.hasNext()) {
			Pair<Row, Row> entry = iterator.next();
			K key = (K) entry.getKey().getField(KEY_FIELD_INDEX);
			V value = (V) entry.getValue().getField(VALUE_FIELD_INDEX);
			result.put(key, value);
		}

		return result;
	}

	@Override
	public void removeAll() {
		Iterator<Pair<Row, Row>> iterator = internalState.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	@Override
	public Iterable<K> keys() {

		return new Iterable<K>() {
			@Override
			public Iterator<K> iterator() {
				Iterator<Pair<Row, Row>> internalIterator = internalState.iterator();

				return new Iterator<K>() {
					@Override
					public boolean hasNext() {
						return internalIterator.hasNext();
					}

					@Override
					public K next() {
						return (K) internalIterator.next().getKey().getField(KEY_FIELD_INDEX);
					}

					@Override
					public void remove() {
						internalIterator.remove();
					}
				};
			}
		};
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKey) throws Exception {
		InternalStateDescriptor descriptor = internalState.getDescriptor();
		InternalColumnDescriptor<K> keyDescriptor = (InternalColumnDescriptor<K>)descriptor.getKeyColumnDescriptor(KEY_FIELD_INDEX);
		K key = KvStateSerializer.deserializeValue(serializedKey, keyDescriptor.getSerializer());

		V value = get(key);
		if (value == null) {
			return null;
		}

		InternalColumnDescriptor<V> valueDescriptor =
			(InternalColumnDescriptor<V>) descriptor.getValueColumnDescriptor(VALUE_FIELD_INDEX);
		return KvStateSerializer.serializeValue(value, valueDescriptor.getSerializer());
	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalState.getNumGroups());
	}
}
