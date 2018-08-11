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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.GroupOutOfRangeException;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalColumnDescriptor;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.RunnableFuture;

/**
 * Implementation of {@link AbstractInternalStateBackend} which stores the key-value
 * pairs of internal states in a nested in-memory map and makes snapshots synchronously.
 */
public final class HeapInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(HeapInternalStateBackend.class);

	/**
	 * The nested map which stores the key-value pairs of internal states. The
	 * heap are successively partitioned by partition index and state name.
	 */
	private transient Map<Integer, Map<String, Map>> heap;

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * Sole constructor.
	 */
	public HeapInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry) {
		super(numberOfGroups, groups, userClassLoader, kvStateRegistry);

		this.heap = new HashMap<>();
		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);

		LOG.info("HeapInternalStateBackend is created.");
	}

	@Override
	public void closeImpl() {
		heap.clear();
	}

	@Override
	protected InternalState createInternalState(
		InternalStateDescriptor stateDescriptor
	) {
		return new HeapInternalState(this, stateDescriptor);
	}

	/**
	 * Returns the map containing the key-value pairs in the given group of
	 * the state. Create the map if it does not exist in the heap and
	 * {@code createIfMissing} is true.
	 *
	 * @param stateDescriptor The descriptor of the state.
	 * @param group The group of the state.
	 * @param createIfMissing True if needing to create the map in the cases
	 *                        where it is not present.
	 * @return The map containing the key-value pairs in the given group of
	 *         the state.
	 */
	@SuppressWarnings("unchecked")
	Map getRootMap(
		InternalStateDescriptor stateDescriptor,
		int group,
		boolean createIfMissing
	) {
		String stateName = stateDescriptor.getName();

		GroupSet groups = getGroups();
		if (!groups.contains(group)) {
			throw new GroupOutOfRangeException(stateName, groups, group);
		}

		Map<String, Map> stateMaps = heap.get(group);
		if (stateMaps == null) {
			if (!createIfMissing) {
				return null;
			}

			stateMaps = new HashMap<>();
			heap.put(group, stateMaps);
		}

		Map rootMap = stateMaps.get(stateName);
		if (rootMap == null) {
			if (!createIfMissing) {
				return null;
			}

			InternalColumnDescriptor<?> firstColumnDescriptor =
				stateDescriptor.getKeyColumnDescriptor(0);
			rootMap = firstColumnDescriptor.isOrdered() ?
				new TreeMap(firstColumnDescriptor.getComparator()) :
				new HashMap();

			stateMaps.put(stateName, rootMap);
		}

		return rootMap;
	}

	@Override
	public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions
	) throws Exception {
		GroupSet groups = getGroups();

		if (states.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =
			localRecoveryConfig.isLocalRecoveryEnabled() ?

					() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					() -> CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory);

		CheckpointStreamWithResultProvider streamWithResultProvider = null;

		// Writes state descriptors into the checkpoint stream
		try {
			streamWithResultProvider = checkpointStreamSupplier.get();
			cancelStreamRegistry.registerCloseable(streamWithResultProvider);

			CheckpointStreamFactory.CheckpointStateOutputStream outputStream =
				streamWithResultProvider.getCheckpointOutputStream();

			DataOutputViewStreamWrapper outputView =
				new DataOutputViewStreamWrapper(outputStream);

			long startTime = System.currentTimeMillis();

			// Writes state descriptors
			outputView.writeInt(states.size());
			for (InternalState state : states.values()) {
				InternalStateDescriptor stateDescriptor = state.getDescriptor();
				InstantiationUtil.serializeObject(outputStream, stateDescriptor);
			}

			// Writes data in state groups
			Map<Integer, Tuple2<Long, Integer>> metaInfos =
				snapshotData(outputStream, outputView);

			SnapshotResult<StreamStateHandle> streamSnapshotResult =
				streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
			streamWithResultProvider = null;

			LOG.info("Successfully complete the snapshot of the states in " +
				(System.currentTimeMillis() - startTime) + "ms");

			StreamStateHandle streamStateHandle = streamSnapshotResult.getJobManagerOwnedSnapshot();
			DefaultStatePartitionSnapshot snapshot =
				new DefaultStatePartitionSnapshot(
					groups,
					metaInfos,
					streamStateHandle
				);

			StreamStateHandle localStreamStateHandle = streamSnapshotResult.getTaskLocalSnapshot();
			if (localStreamStateHandle != null) {
				DefaultStatePartitionSnapshot localSnapshot =
					new DefaultStatePartitionSnapshot(
						groups,
						metaInfos,
						localStreamStateHandle
					);

				return DoneFuture.of(SnapshotResult.withLocalState(snapshot, localSnapshot));
			} else {
				return DoneFuture.of(SnapshotResult.of(snapshot));
			}

		} finally {
			if (streamWithResultProvider != null) {
				IOUtils.closeQuietly(streamWithResultProvider);
				cancelStreamRegistry.unregisterCloseable(streamWithResultProvider);
			}
		}
	}

	@Override
	public void restore(
		Collection<StatePartitionSnapshot> restoredSnapshots
	) throws Exception {
		if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
			return;
		}

		for (StatePartitionSnapshot rawSnapshot : restoredSnapshots) {
			Preconditions.checkState(rawSnapshot instanceof DefaultStatePartitionSnapshot);
			DefaultStatePartitionSnapshot snapshot =
				(DefaultStatePartitionSnapshot) rawSnapshot;

			StreamStateHandle snapshotHandle = snapshot.getSnapshotHandle();
			if (snapshotHandle == null) {
				continue;
			}

			FSDataInputStream inputStream = snapshotHandle.openInputStream();
			try {
				DataInputViewStreamWrapper inputView =
					new DataInputViewStreamWrapper(inputStream);

				int numRestoredStates = inputView.readInt();
				for (int i = 0; i < numRestoredStates; ++i) {
					InternalStateDescriptor restoredStateDescriptor =
						InstantiationUtil.deserializeObject(
							inputStream, getUserClassLoader());

					getInternalState(restoredStateDescriptor);
				}

				Map<Integer, Tuple2<Long, Integer>> metaInfos = snapshot.getMetaInfos();
				restoreData(metaInfos, inputStream, inputView);
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (Exception e) {
						LOG.warn("Could not properly close the input stream.", e);
					}
				}
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	/**
	 * A helper method to take the snapshot of the states in the given scope.
	 *
	 * @param outputStream The output stream where the snapshot is written.
	 * @param outputView The output view of the output stream.
	 * @return A map composed of the offsets and the number of entries of the
	 *         groups in the snapshot.
	 * @throws IOException Thrown when the backend fails to serialize the state
	 *                     data or to write the snapshot into the snapshot.
	 */
	private Map<Integer, Tuple2<Long, Integer>> snapshotData(
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream,
		DataOutputView outputView
	) throws IOException {
		Map<Integer, Tuple2<Long, Integer>> metaInfos = new HashMap<>();

		GroupSet groups = getGroups();
		for (int group : groups) {

			long offset = outputStream.getPos();
			int numEntries = 0;

			for (InternalState state : states.values()) {
				InternalStateDescriptor stateDescriptor = state.getDescriptor();

				String stateName = stateDescriptor.getName();
				Tuple2<RowSerializer, RowSerializer> stateSerializer = getDuplicatedKVSerializers().get(stateName);
				Preconditions.checkNotNull(stateSerializer);

				RowSerializer keySerializer = stateSerializer.f0;
				RowSerializer valueSerializer = stateSerializer.f1;

				Iterator<Pair<Row, Row>> iterator = iterator(group, stateDescriptor);
				while (iterator.hasNext()) {
					Pair<Row, Row> pair = iterator.next();

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

	/**
	 * A helper method to restore the data from the snapshot.
	 *
	 * @param metaInfos The offsets and the number of entries of the groups
	 *                  in the snapshot.
	 * @param inputStream The input stream where the snapshot is read.
	 * @param inputView The input view of the input stream.
	 * @throws IOException Thrown when the backend fails to read the snapshot or
	 *                     to deserialize the state data from the snapshot.
	 */
	private void restoreData(
		Map<Integer, Tuple2<Long, Integer>> metaInfos,
		FSDataInputStream inputStream,
		DataInputView inputView
	) throws IOException {
		GroupSet groups = getGroups();
		for (int group : groups) {
			Tuple2<Long, Integer> metaInfo = metaInfos.get(group);
			if (metaInfo == null) {
				continue;
			}

			long offset = metaInfo.f0;
			int numEntries = metaInfo.f1;

			inputStream.seek(offset);

			for (int i = 0; i < numEntries; ++i) {
				String stateName = StringSerializer.INSTANCE.deserialize(inputView);

				InternalState state = getInternalState(stateName);
				Preconditions.checkState(state != null);

				InternalStateDescriptor stateDescriptor = state.getDescriptor();
				TypeSerializer<Row> keySerializer = stateDescriptor.getKeySerializer();
				Row key = keySerializer.deserialize(inputView);

				TypeSerializer<Row> valueSerializer = stateDescriptor.getValueSerializer();
				Row value = valueSerializer.deserialize(inputView);

				state.put(key, value);
			}
		}
	}

	private Iterator<Pair<Row, Row>> iterator(int group, InternalStateDescriptor descriptor) {
		Map rootMap = getRootMap(descriptor, group, false);
		if (rootMap == null) {
			return Collections.emptyIterator();
		}

		return new PrefixHeapIterator(rootMap, descriptor.getNumKeyColumns(), null);
	}
}
