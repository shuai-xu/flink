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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.FieldBasedPartitioner;
import org.apache.flink.runtime.state.InternalColumnDescriptor;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link KeyedListState} backed by an internal state. The
 * pairs in the internal state are formatted as {K -> List{E}}. Because the
 * pairs are partitioned by K, all the elements under the same key reside in the
 * same group.
 *
 * @param <K> Type of the keys in the state.
 * @param <E> Type of the elements in the state.
 */
public final class KeyedListStateImpl<K, E> implements KeyedListState<K, E> {

	/** The index of the column for keys in the internal state. */
	private static final int KEY_FIELD_INDEX = 0;

	/** The index of the column for elements in the internal state. */
	private static final int ELEMENTS_FIELD_INDEX = 0;

	/**
	 * The internal state where the elements are stored.
	 */
	private final InternalState internalState;

	/**
	 * The descriptor of current state.
	 */
	private static KeyedListStateDescriptor stateDescriptor;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the internal state to store elements.
	 *
	 * @param internalState The internal state where elements are stored.
	 */
	public KeyedListStateImpl(InternalState internalState) {
		Preconditions.checkNotNull(internalState);

		this.internalState = internalState;
	}

	/**
	 * Creates and returns the descriptor for the internal state backing the
	 * keyed state.
	 *
	 * @param keyedStateDescriptor The descriptor for the keyed state.
	 * @param <K> Type of the keys in the state.
	 * @param <E> Type of the elements in the state.
	 * @return The descriptor for the internal state backing the keyed state.
	 */
	public static <K, E> InternalStateDescriptor buildInternalStateDescriptor(
		final KeyedListStateDescriptor<K, E> keyedStateDescriptor
	) {
		stateDescriptor = Preconditions.checkNotNull(keyedStateDescriptor);

		return new InternalStateDescriptorBuilder(keyedStateDescriptor.getName())
			.addKeyColumn("key", keyedStateDescriptor.getKeySerializer())
			.addValueColumn("elements",
				keyedStateDescriptor.getValueSerializer(),
				keyedStateDescriptor.getValueMerger())
			.setPartitioner(new FieldBasedPartitioner(KEY_FIELD_INDEX, HashPartitioner.INSTANCE))
			.getDescriptor();
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key) {
		List<E> list = getOrDefault(key, null);

		return list != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<E> get(K key) {
		return getOrDefault(key, null);
	}

	@Override
	public List<E> getOrDefault(K key, List<E> defaultValue) {
		if (key == null) {
			return defaultValue;
		}

		Row rowValue = this.internalState.get(Row.of(key));
		List<E> list = defaultValue;
		if (rowValue != null) {
			list = (List<E>)rowValue.getField(ELEMENTS_FIELD_INDEX);
		}
		return list;
	}

	@Override
	public Map<K, List<E>> getAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<K, List<E>> results = new HashMap<>(keys.size());

		for (K key : keys) {
			if (key == null) {
				continue;
			}

			List<E> result = get(key);
			if (result != null && !result.isEmpty()) {
				results.put(key, result);
			}
		}

		return results;
	}

	@Override
	public void add(K key, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(element, "You can not add null value to list state.");

		Row internalKey = Row.of(key);
		Row internalValue = Row.of(new ArrayList<>(Arrays.asList(element)));
		internalState.merge(internalKey, internalValue);
	}

	@Override
	public void addAll(K key, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(elements, "List of values to add cannot be null.");

		if (elements.isEmpty()) {
			return;
		}

		Row internalKey = Row.of(key);
		List<E> internalList = new ArrayList<>();
		for (E element : elements) {
			Preconditions.checkNotNull(element, "You can not add null value to list state.");
			internalList.add(element);
		}
		Row internalValue = Row.of(internalList);

		internalState.merge(internalKey, internalValue);
	}

	@Override
	public void addAll(Map<? extends K, ? extends Collection<? extends E>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		Map<Row, Row> internalPairs = new HashMap<>();

		for (Map.Entry<? extends K,  ? extends Collection<? extends E>> entry : map.entrySet()) {
			K key = entry.getKey();
			Preconditions.checkNotNull(key);

			Collection<? extends E> elements = entry.getValue();
			Preconditions.checkNotNull(elements);
			if (elements.isEmpty()) {
				continue;
			}

			Row internalKey = Row.of(key);
			List<E> internalList = new ArrayList<>();
			for (E element : elements) {
				Preconditions.checkNotNull(element);
				internalList.add(element);
			}
			Row internalValue = Row.of(internalList);
			internalPairs.put(internalKey, internalValue);
		}

		internalState.mergeAll(internalPairs);
	}

	@Override
	public void put(K key, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(element, "You can not add null value to list state.");

		Row internalKey = Row.of(key);
		Row internalValue = Row.of(new ArrayList<>(Arrays.asList(element)));
		internalState.put(internalKey, internalValue);
	}

	@Override
	public void putAll(K key, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);

		Row internalKey = Row.of(key);
		List<E> internalList = new ArrayList<>();
		for (E element : elements) {
			Preconditions.checkNotNull(element);
			internalList.add(element);
		}
		Row internalValue = Row.of(internalList);
		internalState.put(internalKey, internalValue);
	}

	@Override
	public void putAll(Map<? extends K, ? extends Collection<? extends E>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		Map<Row, Row> internalPairs = new HashMap<>();
		for (Map.Entry<? extends K, ? extends Collection<? extends E>> entry : map.entrySet()) {
			K key = entry.getKey();
			Preconditions.checkNotNull(key);

			Collection<? extends E> elements = entry.getValue();
			Preconditions.checkNotNull(elements);
			if (elements.isEmpty()) {
				continue;
			}

			Row internalKey = Row.of(key);
			List<E> internalList = new ArrayList<>();
			for (E element : elements) {
				Preconditions.checkNotNull(element);
				internalList.add(element);
			}
			Row internalValue = Row.of(internalList);
			internalPairs.put(internalKey, internalValue);
		}

		internalState.putAll(internalPairs);
	}

	@Override
	public void remove(K key) {
		if (key == null) {
			return;
		}

		internalState.remove(Row.of(key));
	}

	@Override
	public boolean remove(K key, E elementToRemove) {
		if (key == null) {
			return false;
		}

		List<E> list = getOrDefault(key, Collections.emptyList());
		boolean success = list.remove(elementToRemove);
		if (list.isEmpty()) {
			internalState.remove(Row.of(key));
		} else {
			internalState.put(Row.of(key), Row.of(list));
		}
		return success;
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
	public boolean removeAll(K key, Collection<? extends E> elementsToRemove) {
		if (key == null) {
			return false;
		}

		List<E> list = get(key);
		if (list == null) {
			return false;
		}

		boolean success = false;
		Iterator<E> iter = list.iterator();
		while (iter.hasNext()) {
			E element = iter.next();
			if (elementsToRemove.contains(element)) {
				iter.remove();
				success = true;
			}
		}
		if (!list.isEmpty()) {
			internalState.put(Row.of(key), Row.of(list));
		} else {
			internalState.remove(Row.of(key));
		}
		return success;
	}

	@Override
	public boolean removeAll(Map<? extends K, ? extends Collection<? extends E>> map) {
		if (map == null || map.isEmpty()) {
			return false;
		}

		boolean success = false;
		for (Map.Entry<? extends K, ? extends Collection<? extends E>> entry : map.entrySet()) {
			K key = entry.getKey();
			Collection<? extends E> elements = entry.getValue();
			success = removeAll(key, elements) || success;
		}

		return success;
	}

	@Override
	public Map<K, List<E>> getAll() {
		Map<K, List<E>> result = new HashMap<>();

		Iterator<Pair<Row, Row>> iterator = internalState.iterator();
		while (iterator.hasNext()) {
			Pair<Row, Row> entry = iterator.next();
			K key = (K) entry.getKey().getField(KEY_FIELD_INDEX);
			List<E> value = result.getOrDefault(key, new ArrayList<>());
			List<E> addValue = (List<E>) entry.getValue().getField(ELEMENTS_FIELD_INDEX);
			value.addAll(addValue);

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
	public E poll(K key) {
		List<E> list = get(key);

		E element = null;
		if (list != null) {
			element = list.get(0);
			list.remove(0);
		}

		internalState.put(Row.of(key), Row.of(list));
		return element;
	}

	@Override
	public E peek(K key) {
		List<E> list = get(key);

		E element = null;
		if (list != null) {
			element = list.get(0);
		}

		return element;
	}


	@Override
	public byte[] getSerializedValue(byte[] serializedKey) throws Exception {
		InternalStateDescriptor descriptor = internalState.getDescriptor();
		InternalColumnDescriptor<K> keyDescriptor = (InternalColumnDescriptor<K>)descriptor.getKeyColumnDescriptor(KEY_FIELD_INDEX);
		K key = KvStateSerializer.deserializeValue(serializedKey, keyDescriptor.getSerializer());

		List<E> result = get(key);
		if (result == null) {
			return null;
		}

		TypeSerializer<E> serializer = stateDescriptor.getElementSerializer();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

		// write the same as RocksDB writes lists, with one ',' separator
		for (int i = 0; i < result.size(); i++) {
			serializer.serialize(result.get(i), view);
			if (i < result.size() -1) {
				view.writeByte(',');
			}
		}
		view.flush();

		return baos.toByteArray();
	}
}

