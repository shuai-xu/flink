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

import org.apache.flink.runtime.state.InternalState;
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
 * The base implementation of {@link AbstractKeyedMapState} backed by an
 * internal state. The pairs in the internal state are formatted as
 * {(K, MK) -> MV}. Because the pairs are partitioned by K, the mappings under
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

	/** The index of the field for keys in internal keys. */
	static final int KEY_FIELD_INDEX = 0;

	/** The index of the field for map keys in internal keys. */
	static final int MAPKEY_FIELD_INDEX = 1;

	/** The index of the field for map values in internal values. */
	static final int MAPVALUE_FIELD_INDEX = 0;

	/**
	 * The internal state where the mappings are stored.
	 */
	final InternalState internalState;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the internal state to store mappings.
	 *
	 * @param internalState The internal state where the mappings are stored.
	 */
	AbstractKeyedMapStateImpl(InternalState internalState) {
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
	public boolean contains(K key) {
		if (key == null) {
			return false;
		}

		Iterator<Map.Entry<MK, MV>> iterator = iterator(key);
		return iterator.hasNext();
	}

	@Override
	public boolean contains(K key, MK mapKey) {
		if (key == null) {
			return false;
		}

		Row internalKey = Row.of(key, mapKey);
		return internalState.get(internalKey) != null;
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

		Iterator<Map.Entry<MK, MV>> iterator = iterator(key);
		if (!iterator.hasNext()) {
			return defaultValue;
		}

		M result = createMap();
		while (iterator.hasNext()) {
			Map.Entry<MK, MV> entry = iterator.next();
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
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

		Row internalValue = internalState.get(Row.of(key, mapKey));
		return internalValue == null ? defaultMapValue :
			(MV) internalValue.getField(MAPVALUE_FIELD_INDEX);
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

		Collection<Row> internalKeys = new ArrayList<>(mapKeys.size());
		for (MK mapKey : mapKeys) {
			if (mapKey == null) {
				continue;
			}

			internalKeys.add(Row.of(key, mapKey));
		}

		Map<Row, Row> internalPairs = internalState.getAll(internalKeys);

		M results = createMap();
		for (Map.Entry<Row, Row> internalPair : internalPairs.entrySet()) {
			Row internalKey = internalPair.getKey();
			MK mapKey = (MK) internalKey.getField(MAPKEY_FIELD_INDEX);

			Row internalValue = internalPair.getValue();
			MV mapValue = (MV) internalValue.getField(MAPVALUE_FIELD_INDEX);

			results.put(mapKey, mapValue);
		}

		return results;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<K, M> getAll(Map<K, ? extends Collection<? extends MK>> map) {
		if (map == null || map.isEmpty()) {
			return Collections.emptyMap();
		}

		Collection<Row> internalKeys = new ArrayList<>();
		for (Map.Entry<K, ? extends Collection<? extends MK>> entry : map.entrySet()) {
			K key = entry.getKey();
			Collection<? extends MK> mapKeys = entry.getValue();

			if (key == null || mapKeys == null || mapKeys.isEmpty()) {
				continue;
			}

			for (MK mapKey : mapKeys) {
				internalKeys.add(Row.of(key, mapKey));
			}
		}

		Map<Row, Row> internalPairs = internalState.getAll(internalKeys);

		Map<K, M> results = new HashMap<>();
		for (Map.Entry<Row, Row> internalPair : internalPairs.entrySet()) {
			Row internalKey = internalPair.getKey();
			K key = (K) internalKey.getField(KEY_FIELD_INDEX);
			MK mapKey = (MK) internalKey.getField(MAPKEY_FIELD_INDEX);

			Row internalValue = internalPair.getValue();
			MV mapValue = (MV) internalValue.getField(MAPVALUE_FIELD_INDEX);

			M result = results.get(key);
			if (result == null) {
				result = createMap();
				results.put(key, result);
			}

			result.put(mapKey, mapValue);
		}

		return results;
	}

	@Override
	public void add(K key, MK mapKey, MV mapValue) {
		Preconditions.checkNotNull(key);

		internalState.put(Row.of(key, mapKey), Row.of(mapValue));
	}

	@Override
	public void addAll(K key, Map<? extends MK, ? extends MV> mappings) {
		Preconditions.checkNotNull(key);

		if (mappings == null || mappings.isEmpty()) {
			return;
		}

		Map<Row, Row> internalPairs = new HashMap<>(mappings.size());
		for (Map.Entry<? extends MK, ? extends MV> mapping : mappings.entrySet()) {
			MK mapKey = mapping.getKey();

			MV mapValue = mapping.getValue();
			internalPairs.put(Row.of(key, mapKey), Row.of(mapValue));
		}

		internalState.putAll(internalPairs);
	}

	@Override
	public void addAll(Map<? extends K, ? extends Map<? extends MK, ? extends MV>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		Map<Row, Row> internalPairs = new HashMap<>();
		for (Map.Entry<? extends K, ? extends Map<? extends MK, ? extends MV>> entry : map.entrySet()) {
			K key = entry.getKey();
			Preconditions.checkNotNull(key);

			Map<? extends MK, ? extends MV> mappings = entry.getValue();
			if (mappings == null || mappings.isEmpty()) {
				continue;
			}

			for (Map.Entry<? extends MK, ? extends MV> mapping : mappings.entrySet()) {
				MK mapKey = mapping.getKey();

				MV mapValue = mapping.getValue();
				internalPairs.put(Row.of(key, mapKey), Row.of(mapValue));
			}
		}

		internalState.putAll(internalPairs);
	}

	@Override
	public void remove(K key) {
		if (key == null) {
			return;
		}

		Iterator<Map.Entry<MK, MV>> iterator = iterator(key);
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	@Override
	public void remove(K key, MK mapKey) {
		if (key == null) {
			return;
		}

		internalState.remove(Row.of(key, mapKey));
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

		Collection<Row> internalKeys = new ArrayList<>(mapKeys.size());

		for (MK mapKey : mapKeys) {
			internalKeys.add(Row.of(key, mapKey));
		}

		internalState.removeAll(internalKeys);
	}

	@Override
	public void removeAll(Map<? extends K, ? extends Collection<? extends MK>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		Collection<Row> internalKeys = new ArrayList<>();
		for (Map.Entry<? extends K, ? extends Collection<? extends MK>> entry : map.entrySet()) {
			K key = entry.getKey();
			Collection<? extends MK> mapKeys = entry.getValue();

			if (key == null || mapKeys == null || mapKeys.isEmpty()) {
				continue;
			}

			for (MK mapKey : mapKeys) {
				internalKeys.add(Row.of(key, mapKey));
			}
		}

		internalState.removeAll(internalKeys);
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> iterator(K key) {
		Preconditions.checkNotNull(key);

		Iterator<Pair<Row, Row>> pairIterator = internalState.prefixIterator(Row.of(key));
		return new KeyedMapStateIterator<>(pairIterator);
	}

	@Override
	public Iterable<Map.Entry<MK, MV>> entries(K key) {
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

	@Override
	public Iterable<MK> mapKeys(K key) {
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

	@Override
	public Iterable<MV> mapValues(K key) {
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

	@Override
	public Map<K, M> getAll() {
		Map<K, M> result = new HashMap<>();
		Iterator<Pair<Row, Row>> iterator = internalState.iterator();
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			K key = (K) pair.getKey().getField(KEY_FIELD_INDEX);
			MK mk = (MK) pair.getKey().getField(MAPKEY_FIELD_INDEX);
			MV mv = (MV) pair.getValue().getField(MAPVALUE_FIELD_INDEX);

			M map = result.get(key);
			if (map == null) {
				map = createMap();
			}
			map.put(mk, mv);
			result.put(key, map);
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

	//--------------------------------------------------------------------------

	/**
	 * An {@link Map.Entry} in the map under a key in the state. The entry is
	 * backed by the corresponding pair in the internal state. The changes to
	 * the entry will be reflected in the sate, and vice versa.
	 *
	 * @param <MK> Type of the keys in the map.
	 * @param <MV> Type of the values in the map.
	 */
	static class KeyedMapStateEntry<MK, MV> implements Map.Entry<MK, MV> {

		/**
		 * The corresponding pair in the internal state.
		 */
		private final Pair<Row, Row> internalPair;

		/**
		 * Constructor with the corresponding pair in the internal state.
		 *
		 * @param internalPair The corresponding pair in the internal state.
		 */
		KeyedMapStateEntry(Pair<Row, Row> internalPair) {
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

			KeyedMapStateEntry<MK, MV> that = (KeyedMapStateEntry<MK, MV>) o;
			return internalPair.equals(that.internalPair);
		}

		@Override
		public int hashCode() {
			return internalPair.hashCode();
		}

		@Override
		public String toString() {
			return "KeyedMapStateEntry{" + "internalPair=" + internalPair + "}";
		}
	}


	/**
	 * An iterator over the mappings under a key. The iterator is backed by an
	 * iterator over the key-value pairs in the internal state.
	 *
	 * @param <MK> Type of the keys in the map.
	 * @param <MV> Type of the values in the map.
	 */
	static class KeyedMapStateIterator<MK, MV> implements Iterator<Map.Entry<MK, MV>> {

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
		 *                         in the internal state.
		 */
		KeyedMapStateIterator(Iterator<Pair<Row, Row>> internalIterator) {
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
			return new KeyedMapStateEntry<>(internalPair);
		}

		@Override
		public void remove() {
			internalIterator.remove();
		}
	}
}
