/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.gemini;

import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link InternalState} which is backed by {@link GeminiInternalStateBackend}.
 */
public class GeminiInternalState implements InternalState {

	/**
	 * The backend by which the state is backed.
	 */
	private final GeminiInternalStateBackend backend;

	/**
	 * The descriptor of the state.
	 */
	private final InternalStateDescriptor descriptor;

	/**
	 * Whether to copy the returned value.
	 */
	private final boolean copyValue;

	/**
	 * Store key-value mappings.
	 */
	private final StateStore stateStore;

	/**
	 * Constructor with the given backend and the descriptor.
	 *
	 * @param backend The backend by which the state is backed.
	 * @param descriptor The descriptor of the state.
	 * @param copyValue Whether to copy the returned value.
	 */
	GeminiInternalState(
		GeminiInternalStateBackend backend,
		InternalStateDescriptor descriptor,
		StateStore stateStore,
		boolean copyValue
	) {
		this.backend = Preconditions.checkNotNull(backend);
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStore = Preconditions.checkNotNull(stateStore);
		this.copyValue = copyValue;
	}

	public boolean isCopyValue() {
		return copyValue;
	}

	@Override
	public InternalStateDescriptor getDescriptor() {
		return descriptor;
	}

	@Override
	public int getNumGroups() {
		return backend.getNumGroups();
	}

	@Override
	public GroupSet getPartitionGroups() {
		return backend.getGroups();
	}

	@Override
	public Row get(Row key) {
		if (key == null) {
			return null;
		}

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());

		Row value = stateStore.get(key);

		return (value != null && isCopyValue()) ? descriptor.getValueSerializer().copy(value) : value;
	}

	@Override
	public void put(Row key, Row value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());
		Preconditions.checkArgument(value.getArity() == descriptor.getNumValueColumns());

		Row newValue = isCopyValue() ? descriptor.getValueSerializer().copy(value) : value;

		stateStore.put(key, newValue);
	}

	@Override
	public void merge(Row key, Row value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());
		Preconditions.checkArgument(value.getArity() == descriptor.getNumKeyColumns());
		Preconditions.checkNotNull(descriptor.getValueMerger());

		Row newValue = isCopyValue() ? descriptor.getValueSerializer().copy(value) : value;
		Row oldValue = stateStore.get(key);
		if (oldValue != null) {
			newValue = descriptor.getValueMerger().merge(oldValue, newValue);
		}
		stateStore.put(key, newValue);
	}

	@Override
	public void remove(Row key) {
		if (key == null) {
			return;
		}

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());

		stateStore.remove(key);
	}

	@Override
	public Map<Row, Row> getAll(Collection<Row> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<Row, Row> results = new HashMap<>();
		for (Row key : keys) {
			Row value = get(key);
			if (value != null) {
				results.put(key, value);
			}
		}

		return results;
	}

	@Override
	public void putAll(Map<Row, Row> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			put(pair.getKey(), pair.getValue());
		}
	}

	@Override
	public void removeAll(Collection<Row> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}

		for (Row key : keys) {
			remove(key);
		}
	}

	@Override
	public void mergeAll(Map<Row, Row> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			merge(pair.getKey(), pair.getValue());
		}
	}

	// Iteration -------------------------------------------------------------------------------------------------------

	@Override
	public Iterator<Pair<Row, Row>> iterator() {
		return prefixIterator(null);
	}

	@Override
	public Iterator<Pair<Row, Row>> prefixIterator(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());

		Iterator<Pair<Row, Row>> iterator = stateStore.getIterator(prefixKeys);

		return isCopyValue() ? new CopyValueIterator(iterator) : iterator;
	}

	@Override
	public Pair<Row, Row> firstPair(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		int numKeys = descriptor.getNumKeyColumns();
		Preconditions.checkArgument(numPrefixKeys < numKeys);

		Pair<Row, Row> pair = stateStore.firstPair(prefixKeys);
		return (pair != null && isCopyValue()) ? new CopyValuePair(pair) : pair;
	}

	@Override
	public Pair<Row, Row> lastPair(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		int numKeys = descriptor.getNumKeyColumns();
		Preconditions.checkArgument(numPrefixKeys < numKeys);

		Pair<Row, Row> pair = stateStore.lastPair(prefixKeys);
		return (pair != null && isCopyValue()) ? new CopyValuePair(pair) : pair;
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> headIterator(Row prefixKeys, K endKey) {
		return subIterator(prefixKeys, null, endKey);
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> tailIterator(Row prefixKeys, K startKey) {
		return subIterator(prefixKeys, startKey, null);
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> subIterator(Row prefixKeys, K startKey, K endKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(descriptor.getKeyColumnDescriptor(numPrefixKeys).isOrdered());

		Iterator<Pair<Row, Row>> iterator = stateStore.getSubIterator(prefixKeys, startKey, endKey);

		return isCopyValue() ? new CopyValueIterator(iterator) : iterator;
	}

	/**
	 * Implementation of {@link Pair} where {@link #getValue()} will copy the return value
	 * and {@link #setValue(Row)} will copy the new value.
	 */
	private class CopyValuePair implements Pair<Row, Row> {

		private final Pair<Row, Row> delegatedPair;

		private Row value;

		CopyValuePair(Pair<Row, Row> delegatedPair) {
			this.delegatedPair = Preconditions.checkNotNull(delegatedPair);
			this.value = delegatedPair.getValue() == null ? null :
				descriptor.getValueSerializer().copy(delegatedPair.getValue());
		}

		@Override
		public Row getKey() {
			return delegatedPair.getKey();
		}

		@Override
		public Row getValue() {
			return value;
		}

		@Override
		public Row setValue(Row newValue) {
			Row oldValue = delegatedPair.setValue(newValue == null ? null :
				descriptor.getValueSerializer().copy(newValue));
			value = newValue;
			return oldValue;
		}
	}

	private class CopyValueIterator implements Iterator<Pair<Row, Row>> {

		private final Iterator<Pair<Row, Row>> delegatedIterator;

		CopyValueIterator(Iterator<Pair<Row, Row>> delegatedIterator) {
			this.delegatedIterator = Preconditions.checkNotNull(delegatedIterator);
		}

		@Override
		public boolean hasNext() {
			return delegatedIterator.hasNext();
		}

		@Override
		public Pair<Row, Row> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			Pair<Row, Row> nextPair = delegatedIterator.next();
			return new CopyValuePair(nextPair);
		}

		@Override
		public void remove() {
			delegatedIterator.remove();
		}
	}
}
