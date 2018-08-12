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

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedListStateImpl;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedMapStateImpl;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateImpl;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateBinder;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateBinder;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateImpl;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base implementation for {@link InternalStateBackend}.
 */
public abstract class AbstractInternalStateBackend implements InternalStateBackend, KeyedStateBinder, SubKeyedStateBinder {

	/**
	 * The total number of groups in all subtasks.
	 */
	private int numberOfGroups;

	/**
	 * The groups of the given scope in the backend.
	 */
	private GroupSet groups;

	/**
	 * The classloader for the user code in this operator.
	 */
	private ClassLoader userClassLoader;

	/**
	 * The states backed by the backend.
	 */
	protected transient Map<String, InternalState> states;

	/**
	 * The duplicated key/value row-serializers, this is used to avoid multi-thread access the same serializer.
	 */
	protected transient Map<String, Tuple2<RowSerializer, RowSerializer>> duplicatedKVSerializers;

	/** Registry for all opened streams, so they can be closed if the task using this backend is closed */
	protected CloseableRegistry cancelStreamRegistry;

	/**
	 * StateRegistry helper for this task.
	 */
	protected TaskKvStateRegistry kvStateRegistry;

	/**
	 * Subclasses should implement this method to release unused resources.
	 */
	protected void closeImpl() {}

	/**
	 * Creates the internal state described by the given descriptor.
	 *
	 * @param stateDescriptor The descriptor of the state to be created.
	 * @return The internal state described by the given descriptor.
	 */
	protected abstract InternalState createInternalState(InternalStateDescriptor stateDescriptor);

	//--------------------------------------------------------------------------

	protected AbstractInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		TaskKvStateRegistry kvStateRegistry) {

		this.numberOfGroups = numberOfGroups;
		this.groups = Preconditions.checkNotNull(groups);
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		this.states = new HashMap<>();
		this.duplicatedKVSerializers = new HashMap<>();
		this.cancelStreamRegistry = new CloseableRegistry();
		this.kvStateRegistry = kvStateRegistry;
	}

	/**
	 * Returns the total number of groups in all subtasks.
	 *
	 * @return The total number of groups in all subtasks.
	 */
	public int getNumGroups() {
		return numberOfGroups;
	}

	/**
	 * Returns the groups of the given scope in the backend.
	 *
	 * @return The groups of the given scope in the backend.
	 */
	public GroupSet getGroups() {
		return groups;
	}

	/**
	 * Returns the class loader for the user code in this operator.
	 *
	 * @return The class loader for the user code in this operator.
	 */
	public ClassLoader getUserClassLoader() {
		return userClassLoader;
	}

	/**
	 * Return the states backed by the state backend.
	 *
	 * @return The states backed by the state backend.
	 */
	public Map<String, InternalState> getStates() {
		return states;
	}


	/**
	 * Return the duplicated key/value serializers backed by the state backend.
	 *
	 * @return The duplicated key/value serializers backed by the state backend.
	 */
	public Map<String, Tuple2<RowSerializer, RowSerializer>> getDuplicatedKVSerializers() {
		return duplicatedKVSerializers;
	}

	//--------------------------------------------------------------------------

	@Override
	public void close() {
	}

	@Override
	public void dispose() {
		closeImpl();

		IOUtils.closeQuietly(cancelStreamRegistry);
		this.states.clear();
		this.duplicatedKVSerializers.clear();
	}

	@Override
	public InternalState getInternalState(InternalStateDescriptor stateDescriptor) {
		checkNotNull(stateDescriptor);

		String stateName = stateDescriptor.getName();
		InternalState state = states.get(stateName);
		if (state != null) {
			if (!state.getDescriptor().equals(stateDescriptor)) {
				throw new StateIncompatibleAccessException(state.getDescriptor(), stateDescriptor);
			}
		} else {
			state = createInternalState(stateDescriptor);
			states.put(stateName, state);
			duplicatedKVSerializers.put(stateName,
				Tuple2.of((RowSerializer) stateDescriptor.getKeySerializer().duplicate(),
					(RowSerializer) stateDescriptor.getValueSerializer().duplicate()));
		}

		return state;
	}

	@Override
	public <K, V, S extends KeyedState<K, V>> S getKeyedState(
		KeyedStateDescriptor<K, V, S> keyedStateDescriptor
	) {
		checkNotNull(keyedStateDescriptor);

		return keyedStateDescriptor.bind(this);
	}

	@Override
	public <K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(
		SubKeyedStateDescriptor<K, N, V, S> stateDescriptor
	) {
		checkNotNull(stateDescriptor);

		return stateDescriptor.bind(this);
	}

	@Override
	public <K, V> KeyedValueState<K, V> createKeyedValueState(KeyedValueStateDescriptor<K, V> keyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor = KeyedValueStateImpl.buildInternalStateDescriptor(keyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		KeyedValueState<K, V> state = new KeyedValueStateImpl<>(internalState);
		registQueryableStateIfNeeded(keyedStateDescriptor, state);
		return state;
	}

	@Override
	public <K, E> KeyedListState<K, E> createKeyedListState(KeyedListStateDescriptor<K, E> keyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			KeyedListStateImpl.buildInternalStateDescriptor(keyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		KeyedListState<K, E> state = new KeyedListStateImpl<>(internalState);
		registQueryableStateIfNeeded(keyedStateDescriptor, state);
		return state;
	}

	@Override
	public <K, MK, MV> KeyedMapState<K, MK, MV> createKeyedMapState(KeyedMapStateDescriptor<K, MK, MV> keyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			KeyedMapStateImpl.createInternalStateDescriptor(keyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		KeyedMapState<K, MK, MV> state = new KeyedMapStateImpl<>(internalState);
		registQueryableStateIfNeeded(keyedStateDescriptor, state);
		return state;
	}

	@Override
	public <K, MK, MV> KeyedSortedMapState<K, MK, MV> createKeyedSortedMapState(KeyedSortedMapStateDescriptor<K, MK, MV> keyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			KeyedSortedMapStateImpl.createInternalStateDescriptor(keyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		return new KeyedSortedMapStateImpl<>(internalState);
	}

	@Override
	public <K, N, V> SubKeyedValueState<K, N, V> createSubKeyedValueState(SubKeyedValueStateDescriptor<K, N, V> subKeyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			SubKeyedValueStateImpl.buildInternalStateDescriptor(subKeyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		return new SubKeyedValueStateImpl<>(internalState);
	}

	@Override
	public <K, N, E> SubKeyedListState<K, N, E> createSubKeyedListState(SubKeyedListStateDescriptor<K, N, E> subKeyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			SubKeyedListStateImpl.buildInternalStateDescriptor(subKeyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		return new SubKeyedListStateImpl<>(internalState);
	}

	@Override
	public <K, N, MK, MV> SubKeyedMapState<K, N, MK, MV> createSubKeyedMapState(SubKeyedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			SubKeyedMapStateImpl.createInternalStateDescriptor(subKeyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		return new SubKeyedMapStateImpl<>(internalState);
	}

	@Override
	public <K, N, MK, MV> SubKeyedSortedMapState<K, N, MK, MV> createSubKeyedSortedMapState(SubKeyedSortedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor) {
		InternalStateDescriptor internalStateDescriptor =
			SubKeyedSortedMapStateImpl.createInternalStateDescriptor(subKeyedStateDescriptor);
		InternalState internalState = getInternalState(internalStateDescriptor);

		return new SubKeyedSortedMapStateImpl<>(internalState);
	}

	//--------------------------------------------------------------------------

	@Override
	public Collection<InternalState> getInternalStates() {
		return states.values();
	}

	@Override
	public InternalState getInternalState(String stateName) {
		return states.get(stateName);
	}

	private void registQueryableStateIfNeeded(KeyedStateDescriptor descriptor, KeyedState state) {
		if (descriptor.isQueryable()) {
			if (kvStateRegistry == null) {
				throw new IllegalStateException("State backend has not been initialized for job.");
			}
			GroupRange groups = (GroupRange) getGroups();
			KeyGroupRange keyGroupRange = KeyGroupRange.of(groups.getStartGroup(), groups.getEndGroup() - 1);
			this.kvStateRegistry.registerKvState(keyGroupRange, descriptor.getQueryableStateName(), state);
		}
	}
}
