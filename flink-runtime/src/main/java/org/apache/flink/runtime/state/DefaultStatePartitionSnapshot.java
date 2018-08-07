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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A default implementation of {@link StatePartitionSnapshot} which contains the
 * complete data in the state groups.
 */
public final class DefaultStatePartitionSnapshot implements StatePartitionSnapshot {

	private static final long serialVersionUID = 1L;

	/**
	 * The groups in the snapshot.
	 */
	private final GroupSet groups;

	/**
	 * The offsets and the number of entries of the groups in the snapshot.
	 */
	private final Map<Integer, Tuple2<Long, Integer>> metaInfos;

	/**
	 * The datum of the snapshot.
	 */
	@Nullable
	private final StreamStateHandle snapshotHandle;

	/**
	 * Constructor with the groups of the states, and the meta info of these
	 * groups in this snapshot.
	 *
	 * @param groups The global groups in the snapshot.
	 * @param metaInfos The offsets and the number of entries of the
	 *                        groups in the snapshot.
	 * @param snapshotHandle The data of the snapshot.
	 */
	public DefaultStatePartitionSnapshot(
		final GroupSet groups,
		final Map<Integer, Tuple2<Long, Integer>> metaInfos,
		final StreamStateHandle snapshotHandle
	) {
		Preconditions.checkNotNull(groups);
		Preconditions.checkNotNull(metaInfos);
		Preconditions.checkNotNull(snapshotHandle);

		this.groups = groups;
		this.metaInfos = metaInfos;
		this.snapshotHandle = snapshotHandle;
	}

	/**
	 * Constructor for empty state groups.
	 */
	public DefaultStatePartitionSnapshot(
		final GroupSet groups
	) {
		Preconditions.checkNotNull(groups);

		this.groups = groups;
		this.metaInfos = Collections.emptyMap();
		this.snapshotHandle = null;
	}

	/**
	 * Returns the meta info of the groups(global&local).
	 *
	 * @return The meta info of the groups(global&local).
	 */
	public Map<Integer, Tuple2<Long, Integer>> getMetaInfos() {
		return metaInfos;
	}

	/**
	 * Returns the data of the snapshot.
	 *
	 * @return The data of the snapshot.
	 */
	public StreamStateHandle getSnapshotHandle() {
		return snapshotHandle;
	}

	@Override
	public GroupSet getGroups() {
		return groups;
	}

	@Override
	public StatePartitionSnapshot getIntersection(
		final GroupSet otherGroups
	) {
		Preconditions.checkNotNull(otherGroups);

		GroupSet intersectGroups = groups.intersect(otherGroups);

		if (snapshotHandle == null) {
			return new DefaultStatePartitionSnapshot(intersectGroups);
		}

		Map<Integer, Tuple2<Long, Integer>> intersectMetaInfos = new HashMap<>();
		for (int group : intersectGroups) {
			Tuple2<Long, Integer> metaInfo = metaInfos.get(group);
			if (metaInfo != null) {
				intersectMetaInfos.put(group, metaInfo);
			}
		}

		return new DefaultStatePartitionSnapshot(
			groups,
			intersectMetaInfos,
			snapshotHandle
		);
	}

	@Override
	public void discardState() throws Exception {
		if (snapshotHandle != null) {
			snapshotHandle.discardState();
		}
	}

	@Override
	public long getStateSize() {
		return snapshotHandle == null ? 0 : snapshotHandle.getStateSize();
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		// No shared states
	}

	@Override
	public boolean hasStates() {
		return snapshotHandle != null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DefaultStatePartitionSnapshot that = (DefaultStatePartitionSnapshot) o;

		return Objects.equals(groups, that.groups) &&
			Objects.equals(metaInfos, that.metaInfos) &&
			Objects.equals(snapshotHandle, that.snapshotHandle);
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(groups);
		result = 31 * result + Objects.hashCode(metaInfos);
		result = 31 * result + Objects.hashCode(snapshotHandle);
		return result;
	}

	@Override
	public String toString() {
		return "DefaultStatePartitionSnapshot{" +
			"groups=" + groups +
			", metaInfos=" + metaInfos +
			", snapshotHandle=" + snapshotHandle +
			"}";
	}
}
