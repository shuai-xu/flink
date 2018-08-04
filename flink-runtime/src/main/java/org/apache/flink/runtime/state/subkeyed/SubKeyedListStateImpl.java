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

package org.apache.flink.runtime.state.subkeyed;

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.runtime.state.FieldBasedPartitioner;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation of {@link SubKeyedListState} backed by an internal state.
 * The pairs in the internal state are formatted as {(K, N) -> List{E}}.
 * Because the pairs are partitioned by K, all the elements under the same key
 * reside in the same group. They can be easily retrieved with a prefix iterator
 * on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <E> Type of the elements in the state.
 */
public final class SubKeyedListStateImpl<K, N, E> implements SubKeyedListState<K, N, E> {

	/** The index of the column for keys in the internal state. */
	private static final int KEY_FIELD_INDEX = 0;

	/** The index of the column for namespaces in the internal state. */
	private static final int NAMESPACE_FIELD_INDEX = 1;

	/** The index of the column for elements in the internal state. */
	private static final int ELEMENTS_FIELD_INDEX = 0;

	/**
	 * The internal state where the elements are stored.
	 */
	private final InternalState internalState;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the internal state to store elements.
	 *
	 * @param internalState The internal state where elements are stored.
	 */
	public SubKeyedListStateImpl(
		InternalState internalState
	) {
		Preconditions.checkNotNull(internalState);

		this.internalState = internalState;
	}

	/**
	 * Creates and returns the descriptor for the internal state backing the
	 * subkeyed state.
	 *
	 * @param subKeyedStateDescriptor The descriptor for the subkeyed state.
	 * @param <K> Type of the keys in the state.
	 * @param <N> Type of the namespaces in the state.
	 * @param <E> Type of the elements in the state.
	 * @return The descriptor for the internal state backing the keyed state.
	 */
	public static <K, N, E> InternalStateDescriptor buildInternalStateDescriptor(
		final SubKeyedListStateDescriptor<K, N, E> subKeyedStateDescriptor
	) {
		Preconditions.checkArgument(subKeyedStateDescriptor != null);

		return new InternalStateDescriptorBuilder(subKeyedStateDescriptor.getName())
			.addKeyColumn("key", subKeyedStateDescriptor.getKeySerializer())
			.addKeyColumn("namespace", subKeyedStateDescriptor.getNamespaceSerializer())
			.addValueColumn("elements",
				subKeyedStateDescriptor.getValueSerializer(),
				subKeyedStateDescriptor.getValueMerger())
			.setPartitioner(new FieldBasedPartitioner(KEY_FIELD_INDEX, HashPartitioner.INSTANCE))
			.getDescriptor();
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		List<E> list = get(key, namespace);
		return list != null;
	}

	@Override
	public List<E> get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@Override
	public List<E> getOrDefault(K key, N namespace, List<E> defaultList) {
		if (key == null) {
			return defaultList;
		}

		Row rowValue = this.internalState.get(Row.of(key, namespace));
		List<E> list = defaultList;
		if (rowValue != null) {
			list = (List<E>)rowValue.getField(ELEMENTS_FIELD_INDEX);
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, List<E>> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.prefixIterator(Row.of(key));

		Map<N, List<E>> results = new HashMap<>();
		while (internalIterator.hasNext()) {
			Pair<Row, Row> internalPair = internalIterator.next();
			Row internalKey = internalPair.getKey();
			Row internalValue = internalPair.getValue();

			N namespace = (N) internalKey.getField(NAMESPACE_FIELD_INDEX);
			List<E> elements = (List<E>) internalValue.getField(ELEMENTS_FIELD_INDEX);

			List<E> result = results.get(namespace);
			if (result == null) {
				result = new ArrayList<>();
				results.put(namespace, result);
			}

			result.addAll(elements);
		}

		return results;
	}

	@Override
	public void add(K key, N namespace, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(element);

		Row internalKey = Row.of(key, namespace);
		Row internalValue = Row.of(new ArrayList<>(Arrays.asList(element)));
		internalState.merge(internalKey, internalValue);
	}

	@Override
	public void addAll(K key, N namespace, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(elements);

		if (elements.isEmpty()) {
			return;
		}

		Row internalKey = Row.of(key, namespace);
		List<E> listValue = new ArrayList<>();
		for (E element : elements) {
			Preconditions.checkNotNull(element);
			listValue.add(element);
		}
		Row internalValue = Row.of(listValue);

		internalState.merge(internalKey, internalValue);
	}

	@Override
	public void put(K key, N namespace, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(element);

		Row internalKey = Row.of(key, namespace);
		Row internalValue = Row.of(new ArrayList<>(Arrays.asList(element)));
		internalState.put(internalKey, internalValue);
	}

	@Override
	public void putAll(K key, N namespace, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(elements);

		if (elements.isEmpty()) {
			return;
		}

		Row internalKey = Row.of(key, namespace);
		List<E> listValue = new ArrayList<>();
		for (E element : elements) {
			Preconditions.checkNotNull(element);
			listValue.add(element);
		}
		Row internalValue = Row.of(listValue);

		internalState.put(internalKey, internalValue);
	}

	@Override
	public void remove(K key, N namespace) {
		if (key == null || namespace == null) {
			return;
		}

		internalState.remove(Row.of(key, namespace));
	}

	@Override
	public boolean remove(K key, N namespace, E elementToRemove) {
		if (key == null || namespace == null) {
			return false;
		}

		boolean success = false;

		List<E> listValue = getOrDefault(key, namespace, Collections.emptyList());
		Iterator<E> iterator = listValue.iterator();
		while (iterator.hasNext()) {
			E element = iterator.next();

			if (Objects.equals(element, elementToRemove)) {
				iterator.remove();
				success = true;
				break;
			}
		}
		if (listValue.isEmpty()) {
			internalState.remove(Row.of(key, namespace));
		} else {
			internalState.put(Row.of(key, namespace), Row.of(listValue));
		}
		return success;
	}

	@Override
	public boolean removeAll(K key, N namespace, Collection<? extends E> elements) {
		if (key == null || namespace == null || elements == null || elements.isEmpty()) {
			return false;
		}

		boolean success = false;
		List<E> listValue = getOrDefault(key, namespace, Collections.emptyList());
		Iterator<E> iterator = listValue.iterator();
		while (iterator.hasNext()) {
			E element = iterator.next();

			if (elements.contains(element)) {
				iterator.remove();
				success = true;
			}
		}

		if (listValue.isEmpty()) {
			internalState.remove(Row.of(key, namespace));
		} else {
			internalState.put(Row.of(key, namespace), Row.of(listValue));
		}
		return success;
	}

	@Override
	public void removeAll(K key) {
		if (key == null) {
			return;
		}

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.prefixIterator(Row.of(key));

		while (internalIterator.hasNext()) {
			internalIterator.next();
			internalIterator.remove();
		}
	}

	@Override
	public Iterator<N> iterator(K key) {
		Preconditions.checkNotNull(key);

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.prefixIterator(Row.of(key));

		Set<N> namespaceSet = new HashSet<>();

		while (internalIterator.hasNext()) {
			Pair<Row, Row> pair = internalIterator.next();
			N namespace = (N) pair.getKey().getField(NAMESPACE_FIELD_INDEX);
			namespaceSet.add(namespace);
		}

		Iterator<N> iterator = namespaceSet.iterator();
		return new Iterator<N>() {
			private N namespace = null;
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public N next() {
				namespace = iterator.next();
				return namespace;
			}

			@Override
			public void remove() {
				if (namespace == null) {
					throw new IllegalStateException();
				}

				iterator.remove();
				SubKeyedListStateImpl.this.remove(key, namespace);
			}
		};
	}

	@Override
	public E poll(K key, N namespace) {
		List<E> listValue = getOrDefault(key, namespace, Collections.emptyList());
		Iterator<E> iterator = listValue.iterator();

		E element = null;
		if (iterator.hasNext()) {
			element = iterator.next();
			iterator.remove();
		}

		return element;
	}

	@Override
	public E peek(K key, N namespace) {
		List<E> listValue = getOrDefault(key, namespace, Collections.emptyList());
		Iterator<E> iterator = listValue.iterator();

		E element = null;
		if (iterator.hasNext()) {
			element = iterator.next();
		}

		return element;
	}
}

