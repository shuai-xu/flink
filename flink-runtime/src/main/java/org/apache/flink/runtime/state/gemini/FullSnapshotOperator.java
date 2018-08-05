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

package org.apache.flink.runtime.state.gemini;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link SnapshotOperator} which makes a full snapshot.
 */
public final class FullSnapshotOperator implements SnapshotOperator {

	private static final Logger LOG = LoggerFactory.getLogger(FullSnapshotOperator.class);

	/**
	 * The state backend this {@link SnapshotOperator} belongs to.
	 */
	private final GeminiInternalStateBackend stateBackend;

	/**
	 * The ID of this checkpoint.
	 */
	private final long checkpointId;

	/**
	 * The factory to use for writing state to streams.
	 */
	private final CheckpointStreamFactory streamFactory;

	/**
	 * Map for holding the snapshots of all {@link StateStore}.
	 */
	private Map<String, StateStoreSnapshot> stateStoreSnapshotMap;


	private final CloseableRegistry cancelStreamRegistry;


	public FullSnapshotOperator(
		GeminiInternalStateBackend stateBackend,
		long checkpointId,
		CheckpointStreamFactory streamFactory,
		CloseableRegistry cancelStreamRegistry
	) {
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
		this.checkpointId = checkpointId;
		this.streamFactory = Preconditions.checkNotNull(streamFactory);
		this.cancelStreamRegistry = Preconditions.checkNotNull(cancelStreamRegistry);
		this.stateStoreSnapshotMap = new HashMap<>();
	}

	@Override
	public void takeSnapshot() {
		for (Map.Entry<String, StateStore> entry : stateBackend.getStateStoreMap().entrySet()) {
			stateStoreSnapshotMap.put(entry.getKey(),
				entry.getValue().createSnapshot(checkpointId));
		}
	}

	@Override
	public StatePartitionSnapshot materializeSnapshot() throws Exception {

		GroupSet groups = stateBackend.getGroups();

		if (stateBackend.getStates().isEmpty()) {
			return new DefaultStatePartitionSnapshot(groups);
		}

		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		try {
			outputStream = streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
			cancelStreamRegistry.registerCloseable(outputStream);

			DataOutputViewStreamWrapper outputView =
				new DataOutputViewStreamWrapper(outputStream);

			long asyncStartTime = System.currentTimeMillis();

			Map<String, InternalState> states = stateBackend.getStates();
			outputView.writeInt(states.size());
			for (InternalState state : states.values()) {
				InternalStateDescriptor stateDescriptor = state.getDescriptor();
				InstantiationUtil.serializeObject(outputStream, stateDescriptor);
			}

			Map<Integer, Tuple2<Long, Integer>> metaInfos =
				snapshotData(outputStream, outputView);

			StreamStateHandle snapshotHandle = outputStream.closeAndGetHandle();

			LOG.info("GeminiStateBackend snapshot asynchronous part took " +
				(System.currentTimeMillis() - asyncStartTime) + " ms.");

			return new DefaultStatePartitionSnapshot(
				groups,
				metaInfos,
				snapshotHandle
			);
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(outputStream)) {
				try {
					outputStream.close();
				} catch (Exception e) {
					LOG.warn("Could not properly close the output stream.", e);
				}
			}
		}
	}

	@Override
	public void releaseResources(boolean cancelled) {
		for (StateStoreSnapshot stateStoreSnapshot : stateStoreSnapshotMap.values()) {
			stateStoreSnapshot.releaseSnapshot();
		}
		stateStoreSnapshotMap.clear();
	}

	/**
	 * A helper method to take the snapshot of the states in the given scope.
	 *
	 * @param outputStream The output stream where the snapshot is written.
	 * @param outputView The output view of the output stream.
	 * @return A map composed of the offsets and the number of entries of the
	 *         groups in the snapshot.
	 * @throws IOException Thrown when the backend fails to serialize the state data or
	 * 						to write the snapshot into the snapshot.
	 */
	private Map<Integer, Tuple2<Long, Integer>> snapshotData(
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream,
		DataOutputView outputView
	) throws IOException {

		Map<Integer, Tuple2<Long, Integer>> metaInfos = new HashMap<>();
		Map<String, InternalState> states = stateBackend.getStates();

		GroupSet groups = stateBackend.getGroups();
		for (int group : groups) {

			long offset = outputStream.getPos();
			int numEntries = 0;

			for (InternalState state : states.values()) {
				InternalStateDescriptor stateDescriptor = state.getDescriptor();

				String stateName = stateDescriptor.getName();
				TypeSerializer<Row> keySerializer = stateDescriptor.getKeySerializer();
				TypeSerializer<Row> valueSerializer = stateDescriptor.getValueSerializer();
				StateStoreSnapshot stateStoreSnapshot = stateStoreSnapshotMap.get(stateName);

				if (stateStoreSnapshot == null) {
					continue;
				}

				Iterator<Pair<Row, Row>> groupIterator = stateStoreSnapshot.groupIterator(group);
				while (groupIterator.hasNext()) {
					Pair<Row, Row> pair = groupIterator.next();

					StringSerializer.INSTANCE.serialize(stateName, outputView);
					keySerializer.serialize(pair.getKey(), outputView);
					valueSerializer.serialize(pair.getValue(), outputView);

					numEntries++;
				}
			}

			if (numEntries != 0) {
				metaInfos.put(group, new Tuple2<>(offset, numEntries));
			}
		}

		return metaInfos;
	}

}
