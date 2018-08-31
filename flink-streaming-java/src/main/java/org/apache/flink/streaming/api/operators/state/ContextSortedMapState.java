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

package org.apache.flink.streaming.api.operators.state;

import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * An implementation of {@link SortedMapState} which is backed by a
 * {@link KeyedSortedMapState}. The values of the states depend on the current key
 * of the operator. That is, when the current key of the operator changes, the
 * values accessed will be changed as well.
 *
 * @param <K> The type of the keys in the state.
 * @param <V> The type of the values in the state.
 */
public class ContextSortedMapState<K, V> implements SortedMapState<K, V> {

	/** The operator to which the state belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The keyed state backing the state. */
	private final KeyedSortedMapState<Object, K, V> keyedState;

	public ContextSortedMapState(
		final AbstractStreamOperator<?> operator,
		KeyedSortedMapState<Object, K, V> keyedState
	) {
		this.operator = operator;
		this.keyedState = keyedState;
	}

	@Override
	public Map.Entry<K, V> firstEntry() {
		return keyedState.firstEntry(operator.getCurrentKey());
	}

	@Override
	public Map.Entry<K, V> lastEntry() {
		return keyedState.lastEntry(operator.getCurrentKey());
	}

	@Override
	public Iterator<Map.Entry<K, V>> headIterator(K endKey) {
		return keyedState.headIterator(operator.getCurrentKey(), endKey);
	}

	@Override
	public Iterator<Map.Entry<K, V>> tailIterator(K startKey) {
		return keyedState.tailIterator(operator.getCurrentKey(), startKey);
	}

	@Override
	public Iterator<Map.Entry<K, V>> subIterator(K startKey, K endKey) {
		return keyedState.subIterator(operator.getCurrentKey(), startKey, endKey);
	}

	@Override
	public boolean isEmpty() {
		return !keyedState.contains(operator.getCurrentKey());
	}

	@Override
	public boolean contains(K key) {
		return keyedState.contains(operator.getCurrentKey(), key);
	}

	@Override
	public V get(K key) {
		return keyedState.get(operator.getCurrentKey(), key);
	}

	@Override
	public V getOrDefault(K key, V defaultValue) {
		return keyedState.getOrDefault(operator.getCurrentKey(), key, defaultValue);
	}

	@Override
	public Map<K, V> getAll(Collection<? extends K> keys) {
		return keyedState.getAll(operator.getCurrentKey(), keys);
	}

	@Override
	public void put(K key, V value) {
		keyedState.add(operator.getCurrentKey(), key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		keyedState.addAll(operator.getCurrentKey(), map);
	}

	@Override
	public void remove(K key) {
		keyedState.remove(operator.getCurrentKey(), key);
	}

	@Override
	public void removeAll(Collection<? extends K> keys) {
		keyedState.removeAll(operator.getCurrentKey(), keys);
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() {
		return ContextSortedMapState.this::iterator;
	}

	@Override
	public Iterable<K> keys() {
		return () -> {
			Iterator<Map.Entry<K, V>> entryIterator = ContextSortedMapState.this.iterator();

			return new Iterator<K>() {
				@Override
				public boolean hasNext() {
					return entryIterator.hasNext();
				}

				@Override
				public K next() {
					Map.Entry<K, V> entry = entryIterator.next();
					return entry.getKey();
				}
			};
		};
	}

	@Override
	public Iterable<V> values() {
		return () -> {
			Iterator<Map.Entry<K, V>> entryIterator = ContextSortedMapState.this.iterator();

			return new Iterator<V>() {
				@Override
				public boolean hasNext() {
					return entryIterator.hasNext();
				}

				@Override
				public V next() {
					Map.Entry<K, V> entry = entryIterator.next();
					return entry.getValue();
				}
			};
		};
	}

	@Override
	public SortedMap<K, V> value() {
		return keyedState.get(operator.getCurrentKey());
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return keyedState.iterator(operator.getCurrentKey());
	}

	@Override
	public void clear() {
		keyedState.remove(operator.getCurrentKey());
	}
}
