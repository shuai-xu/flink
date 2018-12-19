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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.heap.internal.StateTable;
import org.apache.flink.runtime.state.heap.internal.StateTableSnapshot;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

/**
 * Implementation of {@link AbstractInternalStateBackend} which stores the key-value
 * pairs of states on the Java Heap.
 */
public class HeapInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(HeapInternalStateBackend.class);

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * Whether this backend supports async snapshot.
	 */
	private final boolean asynchronousSnapshot;

	public HeapInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry
	) {
		this(numberOfGroups, groups, userClassLoader, localRecoveryConfig, kvStateRegistry, true);
	}

	public HeapInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry,
		boolean asynchronousSnapshot
	) {
		super(numberOfGroups, groups, userClassLoader, kvStateRegistry);

		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);
		this.asynchronousSnapshot = asynchronousSnapshot;

		LOG.info("HeapInternalStateBackend is created with {} mode.", (asynchronousSnapshot ? "async" : "sync"));
	}

	@Override
	public void closeImpl() {

	}

	@Override
	@SuppressWarnings("unchecked")
	protected StateStorage createStateStorageForKeyedState(KeyedStateDescriptor descriptor) {
		String stateName = descriptor.getName();
		StateStorage stateStorage = stateStorages.get(stateName);

		if (stateStorage == null) {
			stateStorage = new HeapStateStorage<>(
				this,
				descriptor.getKeySerializer(),
				VoidNamespaceSerializer.INSTANCE,
				descriptor.getValueSerializer(),
				VoidNamespace.INSTANCE,
				false,
				asynchronousSnapshot
			);
			stateStorages.put(stateName, stateStorage);
		}

		return stateStorage;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected StateStorage createStateStorageForSubKeyedState(SubKeyedStateDescriptor descriptor) {
		String stateName = descriptor.getName();
		StateStorage stateStorage = stateStorages.get(stateName);

		if (stateStorage == null) {
			stateStorage = new HeapStateStorage<>(
				this,
				descriptor.getKeySerializer(),
				descriptor.getNamespaceSerializer(),
				descriptor.getValueSerializer(),
				null,
				true,
				asynchronousSnapshot
			);
			stateStorages.put(stateName, stateStorage);
		}

		return stateStorage;
	}

	@Override
	public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions
	) throws Exception {

		if (stateStorages.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		long syncStartTime = System.currentTimeMillis();

		final List<KeyedStateDescriptor> keyedStateDescriptors = new ArrayList<>(keyedStates.size());
		final List<SubKeyedStateDescriptor> subKeyedStateDescriptors = new ArrayList<>(subKeyedStates.size());

		final Map<String, Integer> keyedStateToId = new HashMap<>(keyedStates.size());
		final Map<String, Integer> subKeyedStateToId = new HashMap<>(subKeyedStates.size());

		final Map<String, org.apache.flink.runtime.state.heap.internal.StateTableSnapshot> keyedStateStableSnapshots = new HashMap<>(keyedStates.size());
		final Map<String, org.apache.flink.runtime.state.heap.internal.StateTableSnapshot> subKeyedStateStableSnapshots = new HashMap<>(subKeyedStates.size());

		for (Map.Entry<String, KeyedState> entry : keyedStates.entrySet()) {
			String stateName = entry.getKey();
			KeyedState keyedState = entry.getValue();
			StateStorage stateStorage = stateStorages.get(stateName);

			keyedStateToId.put(stateName, keyedStateToId.size());
			keyedStateDescriptors.add(keyedState.getDescriptor());

			org.apache.flink.runtime.state.heap.internal.StateTable stateTable = ((HeapStateStorage) stateStorage).getStateTable();
			keyedStateStableSnapshots.put(stateName, stateTable.createSnapshot());
		}

		for (Map.Entry<String, SubKeyedState> entry : subKeyedStates.entrySet()) {
			String stateName = entry.getKey();
			SubKeyedState subKeyedState = entry.getValue();
			StateStorage stateStorage = stateStorages.get(stateName);

			subKeyedStateToId.put(stateName, subKeyedStateToId.size());
			subKeyedStateDescriptors.add(subKeyedState.getDescriptor());

			StateTable stateTable = ((HeapStateStorage) stateStorage).getStateTable();
			subKeyedStateStableSnapshots.put(stateName, stateTable.createSnapshot());
		}

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =
			localRecoveryConfig.isLocalRecoveryEnabled() ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		// implementation of the async IO operation, based on FutureTask
		final AbstractAsyncCallableWithResources<SnapshotResult<StatePartitionSnapshot>> ioCallable =
			new AbstractAsyncCallableWithResources<SnapshotResult<StatePartitionSnapshot>>() {

				CheckpointStreamWithResultProvider streamAndResultExtractor = null;

				@Override
				protected void acquireResources() throws Exception {
					streamAndResultExtractor = checkpointStreamSupplier.get();
					cancelStreamRegistry.registerCloseable(streamAndResultExtractor);
				}

				@Override
				protected void releaseResources() {

					unregisterAndCloseStreamAndResultExtractor();

					for (org.apache.flink.runtime.state.heap.internal.StateTableSnapshot tableSnapshot : keyedStateStableSnapshots.values()) {
						tableSnapshot.release();
					}

					for (org.apache.flink.runtime.state.heap.internal.StateTableSnapshot tableSnapshot : subKeyedStateStableSnapshots.values()) {
						tableSnapshot.release();
					}
				}

				@Override
				protected void stopOperation() {
					unregisterAndCloseStreamAndResultExtractor();
				}

				private void unregisterAndCloseStreamAndResultExtractor() {
					if (cancelStreamRegistry.unregisterCloseable(streamAndResultExtractor)) {
						IOUtils.closeQuietly(streamAndResultExtractor);
						streamAndResultExtractor = null;
					}
				}

				@Nonnull
				@Override
				protected SnapshotResult<StatePartitionSnapshot> performOperation() throws Exception {

					long asyncStartTime = System.currentTimeMillis();

					CheckpointStreamFactory.CheckpointStateOutputStream localStream =
						this.streamAndResultExtractor.getCheckpointOutputStream();

					DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);

					// writes keyed state descriptor
					outView.writeInt(keyedStateDescriptors.size());
					for (KeyedStateDescriptor descriptor : keyedStateDescriptors) {
						InstantiationUtil.serializeObject(outView, descriptor);
					}

					// writes sub-keyed state descriptor
					outView.writeInt(subKeyedStateDescriptors.size());
					for (SubKeyedStateDescriptor descriptor : subKeyedStateDescriptors) {
						InstantiationUtil.serializeObject(outView, descriptor);
					}

					Map<Integer, Tuple2<Long, Integer>> metaInfos = new HashMap<>();

					GroupSet groups = getGroups();

					for (int group : groups) {

						long offset = localStream.getPos();
						int numEntries = 0;

						outView.writeInt(group);

						// write keyed state
						for (Map.Entry<String, org.apache.flink.runtime.state.heap.internal.StateTableSnapshot> entry : keyedStateStableSnapshots.entrySet()) {
							String stateName = entry.getKey();
							outView.writeInt(keyedStateToId.get(stateName));
							numEntries += entry.getValue().writeMappingsInKeyGroup(outView, group);
						}

						// write sub-keyed state
						for (Map.Entry<String, StateTableSnapshot> entry : subKeyedStateStableSnapshots.entrySet()) {
							String stateName = entry.getKey();
							outView.writeInt(subKeyedStateToId.get(stateName));
							numEntries += entry.getValue().writeMappingsInKeyGroup(outView, group);
						}

						if (numEntries != 0) {
							metaInfos.put(group, new Tuple2<>(offset, numEntries));
						}
					}

					if (cancelStreamRegistry.unregisterCloseable(streamAndResultExtractor)) {
						SnapshotResult<StreamStateHandle> streamSnapshotResult =
							streamAndResultExtractor.closeAndFinalizeCheckpointStreamResult();
						streamAndResultExtractor = null;

						StreamStateHandle streamStateHandle = streamSnapshotResult.getJobManagerOwnedSnapshot();
						StatePartitionSnapshot snapshot =
							new DefaultStatePartitionSnapshot(
								groups, metaInfos, streamStateHandle);

						LOG.info("Heap backend snapshot (" + primaryStreamFactory + ", asynchronous part) in thread " +
							Thread.currentThread() + " took " + (System.currentTimeMillis() - asyncStartTime) + " ms.");

						StreamStateHandle localStreamStateHandle = streamSnapshotResult.getTaskLocalSnapshot();
						if (localStreamStateHandle != null) {
							StatePartitionSnapshot localSnapshot =
								new DefaultStatePartitionSnapshot(
									groups, metaInfos, localStreamStateHandle);

							return SnapshotResult.withLocalState(snapshot, localSnapshot);
						} else {
							return SnapshotResult.of(snapshot);
						}
					} else {
						throw new IOException("Stream already closed and cannot return a handle.");
					}
				}
			};

		AsyncStoppableTaskWithCallback<SnapshotResult<StatePartitionSnapshot>> task =
			AsyncStoppableTaskWithCallback.from(ioCallable);

		if (!asynchronousSnapshot) {
			task.run();
		}

		LOG.info("Heap backend snapshot (" + primaryStreamFactory + ", synchronous part) in thread " +
			Thread.currentThread() + " took " + (System.currentTimeMillis() - syncStartTime) + " ms.");

		return task;
	}

	@Override
	public void restore(
		Collection<StatePartitionSnapshot> restoredSnapshots
	) throws Exception {
		if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
			return;
		}

		LOG.info("Initializing heap internal state backend from snapshot.");

		for (StatePartitionSnapshot rawSnapshot : restoredSnapshots) {
			Preconditions.checkState(rawSnapshot instanceof DefaultStatePartitionSnapshot);
			DefaultStatePartitionSnapshot snapshot =
				(DefaultStatePartitionSnapshot) rawSnapshot;

			StreamStateHandle snapshotHandle = snapshot.getSnapshotHandle();
			if (snapshotHandle == null) {
				continue;
			}

			FSDataInputStream inputStream = snapshotHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);

			try {
				DataInputViewStreamWrapper inputView =
					new DataInputViewStreamWrapper(inputStream);

				int numRestoredKeyedStates = inputView.readInt();
				Map<Integer, KeyedState> keyedStatesById = new HashMap<>();
				for (int i = 0; i < numRestoredKeyedStates; i++) {
					KeyedStateDescriptor descriptor = InstantiationUtil.deserializeObject(
						inputStream, getUserClassLoader());
					KeyedState state = descriptor.bind(this);

					keyedStatesById.put(i, state);
				}

				int numRestoredSubKeyedStates = inputView.readInt();
				Map<Integer, SubKeyedState> subKeyedStatesById = new HashMap<>();
				for (int i = 0; i < numRestoredSubKeyedStates; ++i) {
					SubKeyedStateDescriptor descriptor = InstantiationUtil.deserializeObject(
						inputStream, getUserClassLoader());
					SubKeyedState state = descriptor.bind(this);
					subKeyedStatesById.put(i, state);
				}

				Map<Integer, Tuple2<Long, Integer>> metaInfos = snapshot.getMetaInfos();
				GroupSet groups = getGroups();

				for (int group : groups) {
					Tuple2<Long, Integer> tuple = metaInfos.get(group);

					if (tuple == null) {
						continue;
					}

					long offset = tuple.f0;
					int totalEntries = tuple.f1;

					inputStream.seek(offset);

					int writtenKeyGroupIndex = inputView.readInt();
					Preconditions.checkState(writtenKeyGroupIndex == group, "Unexpected key-group in restore.");

					int numEntries = 0;

					// restore keyed states
					for (int i = 0; i < numRestoredKeyedStates; i++) {
						int stateId = inputView.readInt();
						KeyedStateDescriptor descriptor = keyedStatesById.get(stateId).getDescriptor();
						HeapStateStorage stateStorage = (HeapStateStorage) stateStorages.get(descriptor.getName());
						numEntries += readMappingsInKeyGroupForKeyedState(inputView, descriptor, stateStorage);
					}

					// restore sub-keyed states
					for (int i = 0; i < numRestoredSubKeyedStates; i++) {
						int stateId = inputView.readInt();
						SubKeyedStateDescriptor descriptor = subKeyedStatesById.get(stateId).getDescriptor();
						HeapStateStorage stateStorage = (HeapStateStorage) stateStorages.get(descriptor.getName());
						numEntries += readMappingsInKeyGroupForSubKeyedState(inputView, descriptor, stateStorage);
					}

					Preconditions.checkState(totalEntries == numEntries, "Unexpected number of entries");
				}
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
					IOUtils.closeQuietly(inputStream);
				}
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private int readMappingsInKeyGroupForKeyedState(
		DataInputView inView,
		KeyedStateDescriptor descriptor,
		HeapStateStorage stateStorage
	) throws Exception {

		final TypeSerializer keySerializer = descriptor.getKeySerializer();
		final TypeSerializer stateSerializer = descriptor.getValueSerializer();

		int numKeys = inView.readInt();
		for (int i = 0; i < numKeys; ++i) {
			Object key = keySerializer.deserialize(inView);
			Object state = stateSerializer.deserialize(inView);
			stateStorage.put(key, state);
		}

		return numKeys;
	}

	private int readMappingsInKeyGroupForSubKeyedState(
		DataInputView inView,
		SubKeyedStateDescriptor descriptor,
		HeapStateStorage stateStorage
	) throws Exception {

		final TypeSerializer keySerializer = descriptor.getKeySerializer();
		final TypeSerializer namespaceSerializer = descriptor.getNamespaceSerializer();
		final TypeSerializer stateSerializer = descriptor.getValueSerializer();

		int numKeys = inView.readInt();
		for (int i = 0; i < numKeys; ++i) {
			Object key = keySerializer.deserialize(inView);
			Object namespace = namespaceSerializer.deserialize(inView);
			Object state = stateSerializer.deserialize(inView);
			stateStorage.setCurrentNamespace(namespace);
			stateStorage.put(key, state);
		}

		return numKeys;
	}

}
