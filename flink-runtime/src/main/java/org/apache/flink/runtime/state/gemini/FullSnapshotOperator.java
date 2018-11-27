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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.util.function.SupplierWithException;
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
	private final CheckpointStreamFactory primaryStreamFactory;

	/**
	 * States to take a snapshot.
	 */
	private final Map<String, InternalState> states;

	/**
	 * Duplicated key/value serializers.
	 */
	private Map<String, Tuple2<RowSerializer, RowSerializer>> duplicatedKVSerializers;

	 /**
	 * Map for holding the snapshots of all {@link StateStore}.
	 */
	private Map<String, StateStoreSnapshot> stateStoreSnapshotMap;

	private final CloseableRegistry cancelStreamRegistry;

	public FullSnapshotOperator(
		GeminiInternalStateBackend stateBackend,
		long checkpointId,
		CheckpointStreamFactory primaryStreamFactory,
		CloseableRegistry cancelStreamRegistry
	) {
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
		this.checkpointId = checkpointId;
		this.primaryStreamFactory = Preconditions.checkNotNull(primaryStreamFactory);
		this.cancelStreamRegistry = Preconditions.checkNotNull(cancelStreamRegistry);
		this.states = new HashMap<>();
		this.duplicatedKVSerializers = new HashMap<>();
		this.stateStoreSnapshotMap = new HashMap<>();
	}

	@Override
	public void takeSnapshot() {
		states.putAll(stateBackend.getStates());

		for (Map.Entry<String, InternalState> entry : stateBackend.getStates().entrySet()) {
			InternalStateDescriptor descriptor = entry.getValue().getDescriptor();
			RowSerializer keySerializer = (RowSerializer) descriptor.getKeySerializer().duplicate();
			RowSerializer valueSerializer = (RowSerializer) descriptor.getValueSerializer().duplicate();
			duplicatedKVSerializers.put(entry.getKey(), Tuple2.of(keySerializer, valueSerializer));
		}

		for (Map.Entry<String, StateStore> entry : stateBackend.getStateStoreMap().entrySet()) {
			stateStoreSnapshotMap.put(entry.getKey(),
				entry.getValue().createSnapshot(checkpointId));
		}
	}

	@Override
	public SnapshotResult<StatePartitionSnapshot> materializeSnapshot() throws Exception {

		if (states.isEmpty()) {
			return SnapshotResult.empty();
		}

		GroupSet groups = stateBackend.getGroups();

		SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =

				stateBackend.getLocalRecoveryConfig().isLocalRecoveryEnabled() ?

					() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory,
						stateBackend.getLocalRecoveryConfig().getLocalStateDirectoryProvider()) :

					() -> CheckpointStreamWithResultProvider.createSimpleStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory);

		CheckpointStreamWithResultProvider streamWithResultProvider = null;

		try {
			streamWithResultProvider = checkpointStreamSupplier.get();
			cancelStreamRegistry.registerCloseable(streamWithResultProvider);

			CheckpointStreamFactory.CheckpointStateOutputStream outputStream =
				streamWithResultProvider.getCheckpointOutputStream();

			DataOutputViewStreamWrapper outputView =
				new DataOutputViewStreamWrapper(outputStream);

			long asyncStartTime = System.currentTimeMillis();

			outputView.writeInt(states.size());
			for (InternalState state : states.values()) {
				InternalStateDescriptor stateDescriptor = state.getDescriptor();
				InstantiationUtil.serializeObject(outputStream, stateDescriptor);
			}

			Map<Integer, Tuple2<Long, Integer>> metaInfos =
				snapshotData(outputStream, outputView);

			LOG.info("GeminiInternalStateBackend snapshot asynchronous part took " +
				(System.currentTimeMillis() - asyncStartTime) + " ms.");

			SnapshotResult<StreamStateHandle> streamSnapshotResult =
				streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
			cancelStreamRegistry.unregisterCloseable(streamWithResultProvider);
			streamWithResultProvider = null;

			StreamStateHandle streamStateHandle = streamSnapshotResult.getJobManagerOwnedSnapshot();
			StatePartitionSnapshot snapshot =
				new DefaultStatePartitionSnapshot(
					groups, metaInfos, streamStateHandle);

			StreamStateHandle localStreamStateHandle = streamSnapshotResult.getTaskLocalSnapshot();
			if (localStreamStateHandle != null) {
				StatePartitionSnapshot localSnapshot =
					new DefaultStatePartitionSnapshot(
						groups, metaInfos, localStreamStateHandle);

				return SnapshotResult.withLocalState(snapshot, localSnapshot);
			} else {
				return SnapshotResult.of(snapshot);
			}

		} finally {
			if (streamWithResultProvider != null) {
				IOUtils.closeQuietly(streamWithResultProvider);
				cancelStreamRegistry.unregisterCloseable(streamWithResultProvider);
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

		GroupSet groups = stateBackend.getGroups();
		for (int group : groups) {

			long offset = outputStream.getPos();
			int numEntries = 0;

			for (InternalState state : states.values()) {
				InternalStateDescriptor stateDescriptor = state.getDescriptor();

				String stateName = stateDescriptor.getName();

				Tuple2<RowSerializer, RowSerializer> stateSerializer =
					duplicatedKVSerializers.get(stateName);
				Preconditions.checkNotNull(stateSerializer);

				RowSerializer keySerializer = stateSerializer.f0;
				RowSerializer valueSerializer = stateSerializer.f1;
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
