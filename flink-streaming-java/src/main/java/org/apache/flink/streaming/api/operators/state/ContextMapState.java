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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.keyed.ContextKeyedState;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link MapState} which is backed by a
 * {@link KeyedMapState}. The values of the states depend on the current key
 * of the operator. That is, when the current key of the operator changes, the
 * values accessed will be changed as well.
 *
 * @param <K> The type of the keys in the state.
 * @param <V> The type of the values in the state.
 */
public class ContextMapState<K, V> implements MapState<K, V>, ContextKeyedState {

	/** The operator to which the state belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The keyed state backing the state. */
	private final KeyedMapState<Object, K, V> keyedState;

	public ContextMapState(
		final AbstractStreamOperator<?> operator,
		KeyedMapState<Object, K, V> keyedState
	) {
		this.operator = operator;
		this.keyedState = keyedState;
	}

	@Override
	public void clear() {
		keyedState.remove(operator.getCurrentKey());
	}

	@Override
	public V get(K key) throws Exception {
		return keyedState.get(operator.getCurrentKey(), key);
	}

	@Override
	public void put(K key, V value) throws Exception {
		keyedState.add(operator.getCurrentKey(), key, value);
	}

	@Override
	public void putAll(Map<K, V> map) throws Exception {
		keyedState.addAll(operator.getCurrentKey(), map);
	}

	@Override
	public void remove(K key) throws Exception {
		keyedState.remove(operator.getCurrentKey(), key);
	}

	@Override
	public boolean contains(K key) throws Exception {
		return keyedState.contains(operator.getCurrentKey(), key);
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() throws Exception {
		final Iterator<Map.Entry<K, V>> iterator = keyedState.iterator(operator.getCurrentKey());
		return () -> iterator;
	}

	@Override
	public Iterable<K> keys() throws Exception {
		final Iterator<Map.Entry<K, V>> entryIterator = keyedState.iterator(operator.getCurrentKey());

		final Iterator<K> keyIterator = new Iterator<K>() {
			@Override
			public boolean hasNext() {
				return entryIterator.hasNext();
			}

			@Override
			public K next() {
				return entryIterator.next().getKey();
			}
		};

		return () -> keyIterator;
	}

	@Override
	public Iterable<V> values() throws Exception {
		final Iterator<Map.Entry<K, V>> entryIterator = keyedState.iterator(operator.getCurrentKey());

		final Iterator<V> valueIterator = new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return entryIterator.hasNext();
			}

			@Override
			public V next() {
				return entryIterator.next().getValue();
			}
		};

		return () -> valueIterator;
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() throws Exception {
		return keyedState.iterator(operator.getCurrentKey());
	}

	@Override
	public KeyedState getKeyedState() {
		return keyedState;
	}
}
