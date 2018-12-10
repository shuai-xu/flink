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
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

/**
 * The base implementation for {@link InternalStateBackend}.
 */
public abstract class AbstractInternalStateBackend implements InternalStateBackend {

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
	 * Subclasses should implement this method to release unused resources.
	 */
	protected void closeImpl() {}

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

	//--------------------------------------------------------------------------

	@Override
	public void close() {
	}

	@Override
	public void dispose() {
		closeImpl();

		IOUtils.closeQuietly(cancelStreamRegistry);
	}

}
