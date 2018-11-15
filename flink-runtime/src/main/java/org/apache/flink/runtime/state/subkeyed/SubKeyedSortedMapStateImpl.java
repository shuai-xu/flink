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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.runtime.state.FieldBasedPartitioner;
import org.apache.flink.runtime.state.InternalColumnDescriptor;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An implementation of {@link SubKeyedSortedMapState} backed by an internal
 * state.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class SubKeyedSortedMapStateImpl<K, N, MK, MV>
	extends AbstractSubKeyedMapStateImpl<K, N, MK, MV, SortedMap<MK, MV>>
	implements SubKeyedSortedMapState<K, N, MK, MV> {

	private static final Logger LOG = LoggerFactory.getLogger(SubKeyedSortedMapStateImpl.class);

	/**
	 * Constructor with the internal state to store mappings.
	 *
	 * @param internalState The internal state where the mappings are stored.
	 */
	public SubKeyedSortedMapStateImpl(InternalState internalState) {
		super(internalState);
	}

	/**
	 * Creates and returns the descriptor for the internal state backing the
	 * subkeyed state.
	 *
	 * @param subKeyedStateDescriptor The descriptor for the subkeyed state.
	 * @param <K> Type of the keys in the state.
	 * @param <N> Type of the namespaces in the state.
	 * @param <MK> Type of the map keys in the state.
	 * @param <MV> Type of the map values in the state.
	 * @return The descriptor for the internal state backing the keyed state.
	 */
	public static <K, N, MK, MV> InternalStateDescriptor createInternalStateDescriptor(
		final SubKeyedSortedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor
	) {
		Preconditions.checkNotNull(subKeyedStateDescriptor);

		LOG.warn("If using stateBackend which would store keys in bytes format," +
				" please ensure the mapKey's comparator: {} and serializer: {} are both ordered.",
			subKeyedStateDescriptor.getMapKeySerializer(),
			subKeyedStateDescriptor.getComparator());

		return new InternalStateDescriptorBuilder(subKeyedStateDescriptor.getName())
			.addKeyColumn("key",
				subKeyedStateDescriptor.getKeySerializer())
			.addKeyColumn("namespace",
				subKeyedStateDescriptor.getNamespaceSerializer())
			.addKeyColumn("mapKey",
				subKeyedStateDescriptor.getMapKeySerializer(),
				subKeyedStateDescriptor.getComparator())
			.addValueColumn("mapValue",
				subKeyedStateDescriptor.getMapValueSerializer(),
				subKeyedStateDescriptor.getMapValueMerger())
			.setPartitioner(new FieldBasedPartitioner(KEY_FIELD_INDEX, partitioner))
			.getDescriptor();
	}

	@SuppressWarnings("unchecked")
	@Override
	SortedMap<MK, MV> createMap() {
		InternalColumnDescriptor<MK> mapKeyColumnDescriptor =
			(InternalColumnDescriptor<MK>)
				internalState.getDescriptor().getKeyColumnDescriptor(MAPKEY_FIELD_INDEX);
		Comparator<MK> comparator = mapKeyColumnDescriptor.getComparator();
		return new TreeMap<>(comparator);
	}

	//--------------------------------------------------------------------------

	@Override
	public Map.Entry<MK, MV> firstEntry(K key, N namespace) {
		if (key == null || namespace == null) {
			return null;
		}

		Pair<Row, Row> firstInternalPair = internalState.firstPair(Row.of(key, namespace));

		return firstInternalPair == null ? null :
			new SubKeyedMapStateEntry<>(firstInternalPair);
	}

	@Override
	public Map.Entry<MK, MV> lastEntry(K key, N namespace) {
		if (key == null || namespace == null) {
			return null;
		}

		Pair<Row, Row> lastInternalPair = internalState.lastPair(Row.of(key, namespace));

		return lastInternalPair == null ? null :
			new SubKeyedMapStateEntry<>(lastInternalPair);
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> headIterator(K key, N namespace, MK endMapKey) {
		if (key == null || namespace == null || endMapKey == null) {
			return Collections.emptyIterator();
		}

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.headIterator(Row.of(key, namespace), endMapKey);

		return new SubKeyedMapStateIterator<>(internalIterator);
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> tailIterator(K key, N namespace, MK startMapKey) {
		if (key == null || namespace == null || startMapKey == null) {
			return Collections.emptyIterator();
		}

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.tailIterator(Row.of(key, namespace), startMapKey);

		return new SubKeyedMapStateIterator<>(internalIterator);
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> subIterator(K key, N namespace, MK startMapKey, MK endMapKey) {
		if (key == null || namespace == null || startMapKey == null || endMapKey == null) {
			return Collections.emptyIterator();
		}

		Iterator<Pair<Row, Row>> internalIterator =
			internalState.subIterator(Row.of(key, namespace), startMapKey, endMapKey);

		return new SubKeyedMapStateIterator<>(internalIterator);
	}
}

