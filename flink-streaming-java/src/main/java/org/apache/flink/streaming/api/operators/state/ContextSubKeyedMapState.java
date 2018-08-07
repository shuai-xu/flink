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
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * used for map state.
 * @param <N>
 * @param <K>
 * @param <V>
 */
public class ContextSubKeyedMapState<N, K, V> implements MapState<K, V>, ContextSubKeyedState<N> {

	private N namespace;

	private final AbstractStreamOperator<?> operator;

	private final SubKeyedMapState<Object, N, K, V> subKeyedMapState;

	public ContextSubKeyedMapState(
		AbstractStreamOperator<?> operator,
		SubKeyedMapState<Object, N, K, V> subKeyedMapState) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedMapState);
		this.operator = operator;
		this.subKeyedMapState = subKeyedMapState;
	}

	@Override
	public V get(K key) throws Exception {
		return subKeyedMapState.get(getCurrentKey(), getNamespace(), key);
	}

	@Override
	public void put(K key, V value) throws Exception {
		subKeyedMapState.add(getCurrentKey(), getNamespace(), key, value);
	}

	@Override
	public void putAll(Map<K, V> map) throws Exception {
		subKeyedMapState.addAll(getCurrentKey(), getNamespace(), map);
	}

	@Override
	public void remove(K key) throws Exception {
		subKeyedMapState.remove(getCurrentKey(), getNamespace(), key);
	}

	@Override
	public boolean contains(K key) throws Exception {
		return subKeyedMapState.contains(getCurrentKey(), getNamespace(), key);
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() throws Exception {
		final Iterator<Map.Entry<K, V>> iterator = subKeyedMapState.iterator(operator.getCurrentKey(), getNamespace());
		return () -> iterator;
	}

	@Override
	public Iterable<K> keys() throws Exception {
		final Iterator<Map.Entry<K, V>> entryIterator = subKeyedMapState.iterator(operator.getCurrentKey(), getNamespace());

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
		final Iterator<Map.Entry<K, V>> entryIterator = subKeyedMapState.iterator(operator.getCurrentKey(), getNamespace());

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
		return subKeyedMapState.iterator(operator.getCurrentKey(), getNamespace());
	}

	@Override
	public void clear() {
		subKeyedMapState.remove(getCurrentKey(), getNamespace());
	}

	@Override
	public Object getCurrentKey() {
		return operator.getCurrentKey();
	}

	@Override
	public N getNamespace() {
		return namespace;
	}

	@Override
	public void setNamespace(N namespace) {
		this.namespace = namespace;
	}
}
