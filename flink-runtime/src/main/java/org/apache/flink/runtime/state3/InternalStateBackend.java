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

import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.Snapshotable;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.util.Disposable;

import java.io.Closeable;
import java.util.Collection;

/**
 * The class provides access and manage methods to {@link StateStorage}. Each
 * execution instance of an operator will deploy a backend to manage its
 * state storage.
 */
public interface InternalStateBackend extends Snapshotable<SnapshotResult<StatePartitionSnapshot>, Collection<StatePartitionSnapshot>>, Closeable, Disposable {

	/**
	 * Dispose the backend. This method is called when the task completes its
	 * execution.
	 */
	@Override
	void dispose();

	/**
	 * Returns the total number of groups in all subtasks.
	 *
	 * @return The total number of groups in all subtasks.
	 */
	int getNumGroups();

	/**
	 * Returns the groups of the given scope in the backend.
	 *
	 * @return The groups of the given scope in the backend.
	 */
	GroupSet getGroups();

	/**
	 * Returns the class loader for the user code in this operator.
	 *
	 * @return The class loader for the user code in this operator.
	 */
	ClassLoader getUserClassLoader();

}
