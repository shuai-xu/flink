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
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The base implementation of {@link AbstractSubKeyedMapState} backed by an
 * internal state. The pairs in the internal state are formatted as
 * {(K, N, MK) -> MV}. Because the pairs are partitioned by K, the mappings
 * under the same key will be assigned to the same group and can be easily
 * retrieved with the prefix iterator on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 * @param <M> Type of the maps in the state.
 */
abstract class AbstractSubKeyedMapStateImpl<K, N, MK, MV, M extends Map<MK, MV>> implements AbstractSubKeyedMapState<K, N, MK, MV, M> {

	/** The index of the column for keys in internal keys. */
	static final int KEY_FIELD_INDEX = 0;

	/** The index of the column for namespaces in internal keys. */
	static final int NAMESPACE_FIELD_INDEX = 1;

	/** The index of the column for map keys in internal keys. */
	static final int MAPKEY_FIELD_INDEX = 2;

	/** The index of the column for map values in internal values. */
	static final int MAPVALUE_FIELD_INDEX = 0;

	/**
	 * The internal state where the mappings are stored.
	 */
	final InternalState internalState;

	/** partitioner used to get key group.**/
	protected static final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the internal state to store mappings.
	 *
	 * @param internalState The internal state where the mappings are stored.
	 */
	AbstractSubKeyedMapStateImpl(InternalState internalState) {
		Preconditions.checkNotNull(internalState);
		this.internalState = internalState;
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

		Row internalKey = Row.of(key, namespace, mapKey);
		internalState.setCurrentGroup(getKeyGroup(key));
		return internalState.get(internalKey) != null;
	}

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		Iterator<Map.Entry<MK, MV>> iterator = iterator(key, namespace);
		return iterator.hasNext();
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

		internalState.setCurrentGroup(getKeyGroup(key));
		Row internalValue = internalState.get(Row.of(key, namespace, mapKey));
		return internalValue == null ? defaultMapValue :
			(MV) internalValue.getField(MAPVALUE_FIELD_INDEX);
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

		Iterator<Map.Entry<MK, MV>> iterator = iterator(key, namespace);
		if (!iterator.hasNext()) {
			return defaultMap;
		}

		M result = createMap();
		while (iterator.hasNext()) {
			Map.Entry<MK, MV> entry = iterator.next();
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public M getAll(K key, N namespace, Collection<? extends MK> mapKeys) {
		if (key == null || namespace == null || mapKeys == null || mapKeys.isEmpty()) {
			return null;
		}

		List<Row> internalKeys = new ArrayList<>(mapKeys.size());
		for (MK mapKey : mapKeys) {
			internalKeys.add(Row.of(key, namespace, mapKey));
		}

		Map<Row, Row> internalPairs = internalState.getAll(internalKeys);
		if (internalPairs.isEmpty()) {
			return null;
		}

		M results = createMap();
		for (Map.Entry<Row, Row> internalPair : internalPairs.entrySet()) {
			MK mapKey = (MK) internalPair.getKey().getField(MAPKEY_FIELD_INDEX);
			MV mapValue = (MV) internalPair.getValue().getField(MAPVALUE_FIELD_INDEX);
			results.put(mapKey, mapValue);
		}

		return results;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, M> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		Iterator<Pair<Row, Row>> internalIterator = internalState.prefixIterator(Row.of(key));

		Map<N, M> results = new HashMap<>();
		while (internalIterator.hasNext()) {
			Pair<Row, Row> internalPair = internalIterator.next();

			N namespace = (N) internalPair.getKey().getField(NAMESPACE_FIELD_INDEX);
			M map = results.get(namespace);
			if (map == null) {
				map = createMap();
				results.put(namespace, map);
			}

			MK mapKey = (MK) internalPair.getKey().getField(MAPKEY_FIELD_INDEX);
			MV mapValue = (MV) internalPair.getValue().getField(MAPVALUE_FIELD_INDEX);
			map.put(mapKey, mapValue);
		}

		return results;
	}

	@Override
	public void add(K key, N namespace, MK mapKey, MV mapValue) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		internalState.setCurrentGroup(getKeyGroup(key));
		internalState.put(Row.of(key, namespace, mapKey), Row.of(mapValue));
	}

	@Override
	public void addAll(K key, N namespace, Map<? extends MK, ? extends MV> mappings) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		if (mappings == null || mappings.isEmpty()) {
			return;
		}

		Map<Row, Row> internalPairs = new HashMap<>(mappings.size());
		for (Map.Entry<? extends MK, ? extends MV> mapping : mappings.entrySet()) {
			MK mapKey = mapping.getKey();
			Preconditions.checkNotNull(mapKey);

			MV mapValue = mapping.getValue();
			internalPairs.put(Row.of(key, namespace, mapKey), Row.of(mapValue));
		}

		internalState.putAll(internalPairs);
	}

	@Override
	public void remove(K key, N namespace) {
		if (key == null || namespace == null) {
			return;
		}

		Iterator<Map.Entry<MK, MV>> iterator = iterator(key, namespace);
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	@Override
	public void remove(K key, N namespace, MK mapKey) {
		if (key == null || namespace == null) {
			return;
		}

		internalState.setCurrentGroup(getKeyGroup(key));
		internalState.remove(Row.of(key, namespace, mapKey));
	}

	@Override
	public void removeAll(K key, N namespace, Collection<? extends MK> mapKeys) {
		if (key == null || namespace == null || mapKeys.isEmpty()) {
			return;
		}

		Collection<Row> internalKeys = new ArrayList<>(mapKeys.size());
		for (MK mapKey : mapKeys) {
			internalKeys.add(Row.of(key, namespace, mapKey));
		}

		internalState.removeAll(internalKeys);
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
	public Iterator<Map.Entry<MK, MV>> iterator(K key, N namespace) {
		Preconditions.checkNotNull(key);

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.prefixIterator(Row.of(key, namespace));

		return new SubKeyedMapStateIterator<>(internalIterator);
	}

	@Override
	public Iterable<Map.Entry<MK, MV>> entries(K key, N namespace) {
		return new Iterable<Map.Entry<MK, MV>>() {
			@Override
			public Iterator<Map.Entry<MK, MV>> iterator() {
				final Iterator<Map.Entry<MK, MV>> innerIter = AbstractSubKeyedMapStateImpl.this.iterator(key, namespace);
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

	@Override
	public Iterable<MK> keys(K key, N namespace) {
		return new Iterable<MK>() {
			@Override
			public Iterator<MK> iterator() {
				final Iterator<Map.Entry<MK, MV>> innerIter = AbstractSubKeyedMapStateImpl.this.iterator(key, namespace);
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

	@Override
	public Iterable<MV> values(K key, N namespace) {
		return new Iterable<MV>() {
			@Override
			public Iterator<MV> iterator() {
				final Iterator<Map.Entry<MK, MV>> innerIter = AbstractSubKeyedMapStateImpl.this.iterator(key, namespace);
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
				AbstractSubKeyedMapStateImpl.this.remove(key, namespace);
			}
		};
	}

	//--------------------------------------------------------------------------

	/**
	 * An {@link Map.Entry} in the map under a key and namespace in the state.
	 * The entry is backed by the corresponding pair in the internal state. The
	 * changes to the entry will be reflected in the sate, and vice versa.
	 *
	 * @param <MK> Type of the keys in the map.
	 * @param <MV> Type of the values in the map.
	 */
	static class SubKeyedMapStateEntry<MK, MV> implements Map.Entry<MK, MV> {

		/**
		 * The corresponding pair in the internal state.
		 */
		private final Pair<Row, Row> internalPair;

		/**
		 * Constructor with the corresponding pair in the internal state.
		 *
		 * @param internalPair The corresponding pair in the internal state.
		 */
		SubKeyedMapStateEntry(Pair<Row, Row> internalPair) {
			Preconditions.checkNotNull(internalPair);
			this.internalPair = internalPair;
		}

		@SuppressWarnings("unchecked")
		@Override
		public MK getKey() {
			Row internalKey = internalPair.getKey();
			return (MK) internalKey.getField(MAPKEY_FIELD_INDEX);
		}

		@SuppressWarnings("unchecked")
		@Override
		public MV getValue() {
			Row internalValue = internalPair.getValue();
			return (MV) internalValue.getField(MAPVALUE_FIELD_INDEX);
		}

		@SuppressWarnings("unchecked")
		@Override
		public MV setValue(MV value) {
			Row newInternalValue = Row.of(value);
			Row oldInternalValue = internalPair.setValue(newInternalValue);
			return (MV) oldInternalValue.getField(MAPVALUE_FIELD_INDEX);
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			SubKeyedMapStateEntry<MK, MV> that = (SubKeyedMapStateEntry<MK, MV>) o;
			return internalPair.equals(that.internalPair);
		}

		@Override
		public int hashCode() {
			return internalPair.hashCode();
		}

		@Override
		public String toString() {
			return "SubKeyedMapStateEntry{" + "internalPair=" + internalPair + "}";
		}
	}


	/**
	 * An iterator over the mappings under the same key and namespace. The
	 * iterator is backed by an iterator over the key-value pairs in the
	 * internal state.
	 *
	 * @param <MK> Type of the keys in the map.
	 * @param <MV> Type of the values in the map.
	 */
	static class SubKeyedMapStateIterator<MK, MV> implements Iterator<Map.Entry<MK, MV>> {

		/**
		 * The iterator over the key-value pairs with the same key in the
		 * internal state.
		 */
		private final Iterator<Pair<Row, Row>> internalIterator;

		/**
		 * Constructor with the iterator over the corresponding pairs in the
		 * internal state.
		 *
		 * @param internalIterator The iterator over the corresponding pairs
		 *                             in the internal state.
		 */
		SubKeyedMapStateIterator(Iterator<Pair<Row, Row>> internalIterator) {
			Preconditions.checkNotNull(internalIterator);
			this.internalIterator = internalIterator;
		}

		@Override
		public boolean hasNext() {
			return internalIterator.hasNext();
		}

		@Override
		public Map.Entry<MK, MV> next() {
			Pair<Row, Row> internalPair = internalIterator.next();
			return new SubKeyedMapStateEntry<>(internalPair);
		}

		@Override
		public void remove() {
			internalIterator.remove();
		}
	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalState.getNumGroups());
	}
}

