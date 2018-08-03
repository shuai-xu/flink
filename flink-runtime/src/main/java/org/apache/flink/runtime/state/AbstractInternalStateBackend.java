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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base implementation for {@link InternalStateBackend}.
 */
public abstract class AbstractInternalStateBackend implements InternalStateBackend{
	private static final Logger LOG = LoggerFactory.getLogger(AbstractStateBackend.class);

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

	/** Registry for all opened streams, so they can be closed if the task using this backend is closed */
	protected CloseableRegistry cancelStreamRegistry;

	/**
	 * Subclasses should implement this method to release unused resources.
	 */
	protected abstract void closeImpl();

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
		ClassLoader userClassLoader) {

		this.numberOfGroups = numberOfGroups;
		this.groups = Preconditions.checkNotNull(groups);
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		this.states = new HashMap<>();
		this.cancelStreamRegistry = new CloseableRegistry();
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

	//--------------------------------------------------------------------------

	@Override
	public void close() {
		closeImpl();

		IOUtils.closeQuietly(cancelStreamRegistry);
		this.states.clear();
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
		}

		return state;
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
}
