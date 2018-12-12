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

package org.apache.flink.runtime.state3;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state3.keyed.KeyedListState;
import org.apache.flink.runtime.state3.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state3.keyed.KeyedListStateImpl;
import org.apache.flink.runtime.state3.keyed.KeyedMapState;
import org.apache.flink.runtime.state3.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state3.keyed.KeyedMapStateImpl;
import org.apache.flink.runtime.state3.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state3.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state3.keyed.KeyedSortedMapStateImpl;
import org.apache.flink.runtime.state3.keyed.KeyedState;
import org.apache.flink.runtime.state3.keyed.KeyedStateBinder;
import org.apache.flink.runtime.state3.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state3.keyed.KeyedValueState;
import org.apache.flink.runtime.state3.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state3.keyed.KeyedValueStateImpl;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedListStateImpl;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedMapStateImpl;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedSortedMapState;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedSortedMapStateImpl;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedStateBinder;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedValueStateImpl;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

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

	/** Registry for all opened streams, so they can be closed if the task using this backend is closed. */
	protected CloseableRegistry cancelStreamRegistry;

	/**
	 * StateRegistry helper for this task.
	 */
	protected TaskKvStateRegistry kvStateRegistry;

	/**
	 * The state storages backend by the backend.
	 */
	protected transient Map<String, StateStorage> stateStorages;

	/**
	 * The keyed state backed by the backend.
	 */
	protected transient Map<String, KeyedState> keyedStates;

	/**
	 * The sub-keyed state backed by the backend.
	 */
	protected transient Map<String, SubKeyedState> subKeyedStates;

	/**
	 * Subclasses should implement this method to release unused resources.
	 */
	protected void closeImpl() {}

	/**
	 * Creates the state storage described by the given keyed descriptor.
	 *
	 * @param descriptor The descriptor of the state storage to be created.
	 * @return The state storage described by the given descriptor.
	 */
	protected abstract StateStorage createStateStorageForKeyedState(KeyedStateDescriptor descriptor);

	/**
	 * Creates the state storage described by the given sub-keyed descriptor.
	 *
	 * @param descriptor The descriptor of the state storage to be created.
	 * @return The state storage described by the given descriptor.
	 */
	protected abstract StateStorage createStateStorageForSubKeyedState(SubKeyedStateDescriptor descriptor);

	//--------------------------------------------------------------------------

	protected AbstractInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		TaskKvStateRegistry kvStateRegistry) {

		this.numberOfGroups = numberOfGroups;
		this.groups = Preconditions.checkNotNull(groups);
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		this.cancelStreamRegistry = new CloseableRegistry();
		this.kvStateRegistry = kvStateRegistry;

		this.stateStorages = new HashMap<>();
		this.keyedStates = new HashMap<>();
		this.subKeyedStates = new HashMap<>();
	}

	@Override
	public int getNumGroups() {
		return numberOfGroups;
	}

	@Override
	public GroupSet getGroups() {
		return groups;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userClassLoader;
	}

	@Override
	public Map<String, StateStorage> getStateStorages() {
		return stateStorages;
	}

	@Override
	public Map<String, KeyedState> getKeyedStates() {
		return keyedStates;
	}

	@Override
	public Map<String, SubKeyedState> getSubKeyedStates() {
		return subKeyedStates;
	}

	//--------------------------------------------------------------------------

	@Override
	public void dispose() {
		closeImpl();

		IOUtils.closeQuietly(cancelStreamRegistry);

		stateStorages.clear();
		keyedStates.clear();
		subKeyedStates.clear();
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
		StateStorage stateStorage = getStateStorageForKeyedState(keyedStateDescriptor);
		KeyedValueState<K, V> state = new KeyedValueStateImpl<>(this, keyedStateDescriptor, stateStorage);
		keyedStates.put(keyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, E> KeyedListState<K, E> createKeyedListState(KeyedListStateDescriptor<K, E> keyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForKeyedState(keyedStateDescriptor);
		KeyedListState<K, E> state = new KeyedListStateImpl<>(this, keyedStateDescriptor, stateStorage);
		keyedStates.put(keyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, MK, MV> KeyedMapState<K, MK, MV> createKeyedMapState(KeyedMapStateDescriptor<K, MK, MV> keyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForKeyedState(keyedStateDescriptor);
		KeyedMapState<K, MK, MV> state = new KeyedMapStateImpl<>(this, keyedStateDescriptor, stateStorage);
		keyedStates.put(keyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, MK, MV> KeyedSortedMapState<K, MK, MV> createKeyedSortedMapState(KeyedSortedMapStateDescriptor<K, MK, MV> keyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForKeyedState(keyedStateDescriptor);
		KeyedSortedMapState<K, MK, MV> state = new KeyedSortedMapStateImpl<>(this, keyedStateDescriptor, stateStorage);
		keyedStates.put(keyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, N, V> SubKeyedValueState<K, N, V> createSubKeyedValueState(SubKeyedValueStateDescriptor<K, N, V> subKeyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForSubKeyedState(subKeyedStateDescriptor);
		SubKeyedValueState<K, N, V> state = new SubKeyedValueStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
		subKeyedStates.put(subKeyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, N, E> SubKeyedListState<K, N, E> createSubKeyedListState(SubKeyedListStateDescriptor<K, N, E> subKeyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForSubKeyedState(subKeyedStateDescriptor);
		SubKeyedListState<K, N, E> state = new SubKeyedListStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
		subKeyedStates.put(subKeyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, N, MK, MV> SubKeyedMapState<K, N, MK, MV> createSubKeyedMapState(SubKeyedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForSubKeyedState(subKeyedStateDescriptor);
		SubKeyedMapState<K, N, MK, MV> state = new SubKeyedMapStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
		subKeyedStates.put(subKeyedStateDescriptor.getName(), state);

		return state;
	}

	@Override
	public <K, N, MK, MV> SubKeyedSortedMapState<K, N, MK, MV> createSubKeyedSortedMapState(SubKeyedSortedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor) {
		StateStorage stateStorage = getStateStorageForSubKeyedState(subKeyedStateDescriptor);
		SubKeyedSortedMapState<K, N, MK, MV> state = new SubKeyedSortedMapStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
		subKeyedStates.put(subKeyedStateDescriptor.getName(), state);

		return state;
	}

	//--------------------------------------------------------------------------

	private StateStorage getStateStorageForKeyedState(KeyedStateDescriptor stateDescriptor) {
		Preconditions.checkNotNull(stateDescriptor);

		String stateName = stateDescriptor.getName();
		StateStorage stateStorage = stateStorages.get(stateName);
		if (stateStorage != null) {
			KeyedState state = keyedStates.get(stateName);
			Preconditions.checkNotNull(state, "Expect a created keyed state");
			if (!state.getDescriptor().equals(stateDescriptor)) {
				throw new StateIncompatibleAccessException(state.getDescriptor(), stateDescriptor);
			}
		} else {
			stateStorage = createStateStorageForKeyedState(stateDescriptor);
			stateStorages.put(stateName, stateStorage);
		}

		return stateStorage;
	}

	private StateStorage getStateStorageForSubKeyedState(SubKeyedStateDescriptor stateDescriptor) {
		Preconditions.checkNotNull(stateDescriptor);

		String stateName = stateDescriptor.getName();
		StateStorage stateStorage = stateStorages.get(stateName);
		if (stateStorage != null) {
			SubKeyedState state = subKeyedStates.get(stateName);
			Preconditions.checkNotNull(state, "Expect a created keyed state");
			if (!state.getDescriptor().equals(stateDescriptor)) {
				throw new StateIncompatibleAccessException(state.getDescriptor(), stateDescriptor);
			}
		} else {
			stateStorage = createStateStorageForSubKeyedState(stateDescriptor);
			stateStorages.put(stateName, stateStorage);
		}

		return stateStorage;
	}

}
