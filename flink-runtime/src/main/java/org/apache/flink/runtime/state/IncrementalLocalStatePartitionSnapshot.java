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

import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.Map;

/**
 * State handle for local copies of {@link IncrementalStatePartitionSnapshot}. Consists of a {@link DirectoryStateHandle} that
 * represents the directory of the native RocksDB snapshot, the key groups, and a stream state handle for Flink's state
 * meta data file.
 */
public class IncrementalLocalStatePartitionSnapshot implements StatePartitionSnapshot {

	private static final long serialVersionUID = 1L;

	/** Id of the checkpoint that created this state handle. */
	@Nonnegative
	private final long checkpointId;

	/** The groups in the snapshot. */
	@Nonnull
	private final GroupSet groups;

	/** Handle to Flink's state meta data. */
	@Nonnull
	private final StreamStateHandle metaStateHandle;

	/** The directory state handle. */
	@Nonnull
	private final DirectoryStateHandle directoryStateHandle;

	/** Map with the local state handle ID and unique gobal id of all shared state handles created by the checkpoint. */
	@Nonnull
	private final Map<StateHandleID, String> sharedStateHandleIDs;

	public IncrementalLocalStatePartitionSnapshot(
		@Nonnull GroupSet groups,
		@Nonnegative long checkpointId,
		@Nonnull StreamStateHandle metaStateHandle,
		@Nonnull DirectoryStateHandle directoryStateHandle,
		@Nonnull Map<StateHandleID, String> sharedStateHandleIDs
	) {
		this.groups = groups;
		this.checkpointId = checkpointId;
		this.metaStateHandle = metaStateHandle;
		this.directoryStateHandle = directoryStateHandle;
		this.sharedStateHandleIDs = sharedStateHandleIDs;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	@Nonnull
	public StreamStateHandle getMetaStateHandle() {
		return metaStateHandle;
	}

	@Nonnull
	public DirectoryStateHandle getDirectoryStateHandle() {
		return directoryStateHandle;
	}

	@Nonnull
	public Map<StateHandleID, String> getSharedStateHandleIDs() {
		return sharedStateHandleIDs;
	}

	@Override
	public GroupSet getGroups() {
		return groups;
	}

	@Override
	public StatePartitionSnapshot getIntersection(GroupSet groups) {
		return this.groups.intersect(groups).isEmpty() ? null : this;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		// Nothing to do, this is for local use only.
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		IncrementalLocalKeyedStateHandle that = (IncrementalLocalKeyedStateHandle) o;

		if (!getDirectoryStateHandle().equals(that.getDirectoryStateHandle())) {
			return false;
		}
		if (!getSharedStateHandleIDs().equals(that.getSharedStateHandleIDs())) {
			return false;
		}
		return getMetaStateHandle().equals(that.getMetaDataState());
	}

	@Override
	public void discardState() throws Exception {

		Exception collectedEx = null;

		try {
			directoryStateHandle.discardState();
		} catch (Exception e) {
			collectedEx = e;
		}

		try {
			metaStateHandle.discardState();
		} catch (Exception e) {
			collectedEx = ExceptionUtils.firstOrSuppressed(e, collectedEx);
		}

		if (collectedEx != null) {
			throw collectedEx;
		}
	}

	@Override
	public long getStateSize() {
		return directoryStateHandle.getStateSize() + metaStateHandle.getStateSize();
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + getMetaStateHandle().hashCode();
		result = 31 * result + getDirectoryStateHandle().hashCode();
		result = 31 * result + getSharedStateHandleIDs().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "IncrementalStatePartitionSnapshot{" +
			"metaStateHandle=" + metaStateHandle +
			"} " + super.toString();
	}
}
