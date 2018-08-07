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

/**
 * The interface for the snapshot of the states in an operator partition.
 */
public interface StatePartitionSnapshot extends CompositeStateHandle {

	/**
	 * Returns the groups(global&local) whose states are included in the snapshot.
	 *
	 * @return The groups(global&local) whose states are included in the snapshot.
	 */
	GroupSet getGroups();

	/**
	 * Returns the snapshot of the states in those groups that are the
	 * intersection of the given groups and the groups in the snapshot.
	 *
	 * @param groups The groups to be included in the returned snapshot.
	 * @return The snapshot of those groups that are in the intersection of the
	 *         given groups and the groups in the snapshot.
	 */
	StatePartitionSnapshot getIntersection(GroupSet groups);

	/**
	 * Whethre the snapshot has state.
	 *
	 * @return Returns true if this contains at least one {@link StateObject}.
	 */
	boolean hasStates();
}
