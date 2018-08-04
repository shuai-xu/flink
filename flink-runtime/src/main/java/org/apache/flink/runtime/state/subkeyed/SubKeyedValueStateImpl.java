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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link SubKeyedValueState} based on an {@link InternalState}
 * The pairs in the internal state are formatted as {(K, N) -> V}, and are
 * partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <V> Type of the values in the state.
 */
public final class SubKeyedValueStateImpl<K, N, V> implements SubKeyedValueState<K, N, V> {

	/** The index of the value in the internal key. */
	private static final int KEY_FIELD_INDEX = 0;

	/** The index fo the namespace in the internal key. */
	private static final int NAMESPACE_FIELD_INDEX = 1;

	/** The index of the value in the internal value. */
	private static final int VALUE_FIELD_INDEX = 0;

	/**
	 * The internal state where the values are stored.
	 */
	private final InternalState internalState;

	/**
	 * Constructor with the internal state to store the values.
	 *
	 * @param internalState The internal state where the values are stored.
	 */
	public SubKeyedValueStateImpl(InternalState internalState) {
		Preconditions.checkNotNull(internalState);
		this.internalState = internalState;
	}

	/**
	 * Creates and returns the descriptor for the internal state backing the
	 * subkeyed state.
	 *
	 * @param subKeyedStateDescriptor The descriptor of the subkeyed state.
	 * @param <K> Type of the keys in the state.
	 * @param <N> Type of the namespaces in the state.
	 * @param <V> Type of the values in the state.
	 * @return The descriptor for the internal state backing the keyed state.
	 */
	public static <K, N, V> InternalStateDescriptor buildInternalStateDescriptor(
		final SubKeyedValueStateDescriptor<K, N, V> subKeyedStateDescriptor
	) {
		Preconditions.checkNotNull(subKeyedStateDescriptor);

		return new InternalStateDescriptorBuilder(subKeyedStateDescriptor.getName())
			.addKeyColumn("key", subKeyedStateDescriptor.getKeySerializer())
			.addKeyColumn("namespace", subKeyedStateDescriptor.getNamespaceSerializer())
			.addValueColumn("value",
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

		Row row = internalState.get(Row.of(key, namespace));
		return (row != null);
	}

	@Override
	public V get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V getOrDefault(K key, N namespace, V defaultValue) {
		if (key == null || namespace == null) {
			return defaultValue;
		}

		Row row = internalState.get(Row.of(key, namespace));
		return row == null ? defaultValue : (V) row.getField(VALUE_FIELD_INDEX);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, V> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.prefixIterator(Row.of(key));

		Map<N, V> result = new HashMap<>();
		while (internalIterator.hasNext()) {
			Pair<Row, Row> internalPair = internalIterator.next();

			N namespace = (N) internalPair.getKey().getField(NAMESPACE_FIELD_INDEX);
			V value = (V) internalPair.getValue().getField(VALUE_FIELD_INDEX);
			result.put(namespace, value);
		}

		return result;
	}

	@Override
	public void remove(K key, N namespace) {
		if (key == null || namespace == null) {
			return;
		}

		internalState.remove(Row.of(key, namespace));
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
	public void put(K key, N namespace, V value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		internalState.put(Row.of(key, namespace), Row.of(value));
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
				SubKeyedValueStateImpl.this.remove(key, namespace);
			}
		};
	}
}
