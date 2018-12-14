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

package org.apache.flink.contrib.streaming.state3;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state3.StateSerializerUtil;
import org.apache.flink.runtime.state3.keyed.KeyedState;
import org.apache.flink.runtime.state3.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.types.Pair;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state3.StateSerializerUtil.GROUP_WRITE_BYTES;

/**
 * Full snapshot related operations of RocksDB state backend.
 */
public class RocksDBFullSnapshotOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBFullSnapshotOperation.class);

	/** State backend who starts the Snapshot.*/
	private final RocksDBInternalStateBackend stateBackend;

	/** Current Checkpoint ID.*/
	private final long checkpointId;

	/** Checkpoint Stream Supplier of current snapshot.*/
	private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;

	private final CloseableRegistry snapshotCloseableRegistry;

	private final ResourceGuard.Lease dbLease;

	private CheckpointStreamWithResultProvider checkpointStreamWithResultProvider;

	private DataOutputView outputView;

	/** All keyed states will be snapshot.*/
	private final Map<String, KeyedState> keyedStates;

	/** All subkeyed states will be snapshot.*/
	private final Map<String, SubKeyedState> subKeyedStates;

	private Map<Integer, Tuple2<Long, Integer>> metaInfo;

	private final Map<String, Integer> stateName2Id;

	/**
	 * The snapshot directory containing all the data.
	 */
	private SnapshotDirectory snapshotDirectory;

	RocksDBFullSnapshotOperation(
		RocksDBInternalStateBackend stateBackend,
		long checkpointId,
		SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
		CloseableRegistry registry) throws IOException {

		this.stateBackend = stateBackend;
		this.checkpointId = checkpointId;
		this.checkpointStreamSupplier = checkpointStreamSupplier;
		this.snapshotCloseableRegistry = registry;
		this.dbLease = stateBackend.rocksDBResourceGuard.acquireResource();
		this.keyedStates = new HashMap<>();
		this.subKeyedStates = new HashMap<>();
		this.stateName2Id = new HashMap<>();
	}

	/**
	 * 1) Create a snapshot object from RocksDB.
	 */
	void takeDBSnapShot() throws IOException, RocksDBException {
		Preconditions.checkArgument(snapshotDirectory == null, "Only one ongoing snapshot allowed!");

		keyedStates.putAll(stateBackend.getKeyedStates());
		subKeyedStates.putAll(stateBackend.getSubKeyedStates());
		int id = 0;
		for (String stateName : keyedStates.keySet()) {
			stateName2Id.put(stateName, id++);
		}
		for (String stateName : subKeyedStates.keySet()) {
			stateName2Id.put(stateName, id++);
		}

		// create a "temporary" snapshot directory because local recovery is inactive.
		Path path = new Path(stateBackend.getInstanceBasePath().getAbsolutePath(), "chk-" + checkpointId);
		snapshotDirectory = SnapshotDirectory.temporary(path);
		LOG.info("Taking snapshot for RocksDB instance at {}.", snapshotDirectory.toString());
		stateBackend.takeDbSnapshot(snapshotDirectory.getDirectory().getPath());
	}

	/**
	 * 2) Open CheckpointStateOutputStream through the checkpointStreamFactory into which we will write.
	 *
	 * @throws Exception
	 */
	void openCheckpointStream() throws Exception {
		Preconditions.checkArgument(checkpointStreamWithResultProvider == null,
			"Output stream for snapshot is already set.");

		checkpointStreamWithResultProvider = checkpointStreamSupplier.get();
		snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
		outputView = new DataOutputViewStreamWrapper(
			checkpointStreamWithResultProvider.getCheckpointOutputStream());
	}

	/**
	 * 3) Write the actual data from RocksDB from the time we took the snapshot object in (1).
	 *
	 * @throws IOException
	 */
	void writeDBSnapshot() throws Exception {

		if (null == snapshotDirectory) {
			throw new IOException("No snapshot available. Might be released due to cancellation.");
		}

		Preconditions.checkNotNull(checkpointStreamWithResultProvider, "No output stream to write snapshot.");
		materializeMetaData();
		materializeKVStateData();
	}

	/**
	 * 4) Returns a state partition snapshot for the completed snapshot.
	 *
	 * @return state partition snapshot for the completed snapshot.
	 */
	@Nonnull
	SnapshotResult<StatePartitionSnapshot> getStatePartitionSnapshot() throws IOException {

		Preconditions.checkNotNull(metaInfo);

		SnapshotResult<StreamStateHandle> snapshotResult =
			checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult();

		LOG.info("Successfully complete the snapshot of the states");

		StreamStateHandle snapshotHandle = snapshotResult.getJobManagerOwnedSnapshot();
		StatePartitionSnapshot snapshot =
			new DefaultStatePartitionSnapshot(
				stateBackend.getGroups(), metaInfo, snapshotHandle);

		StreamStateHandle localSnapshotHandle = snapshotResult.getTaskLocalSnapshot();
		if (localSnapshotHandle != null) {
			StatePartitionSnapshot localSnapshot =
				new DefaultStatePartitionSnapshot(
					stateBackend.getGroups(), metaInfo, localSnapshotHandle);

			return SnapshotResult.withLocalState(snapshot, localSnapshot);
		} else {
			return SnapshotResult.of(snapshot);
		}
	}

	/**
	 * 5) Release the snapshot object for RocksDB and clean up.
	 */
	void releaseSnapshotResources() {

		checkpointStreamWithResultProvider = null;
		try {
			snapshotDirectory.cleanup();
		} catch (IOException e) {
			LOG.warn("Fail to clean up the snapshot directory {}.", snapshotDirectory.getDirectory());
		}
		snapshotDirectory = null;

		this.dbLease.close();
	}

	private void materializeMetaData() throws Exception {
		// Writes state descriptors
		outputView.writeInt(keyedStates.size());
		for (KeyedState state : keyedStates.values()) {
			KeyedStateDescriptor stateDescriptor = state.getDescriptor();
			InstantiationUtil.serializeObject(checkpointStreamWithResultProvider.getCheckpointOutputStream(), stateDescriptor);
		}
		outputView.writeInt(subKeyedStates.size());
		for (SubKeyedState state : subKeyedStates.values()) {
			SubKeyedStateDescriptor stateDescriptor = state.getDescriptor();
			InstantiationUtil.serializeObject(checkpointStreamWithResultProvider.getCheckpointOutputStream(), stateDescriptor);
		}
		outputView.writeInt(stateName2Id.size());
		for (Map.Entry<String, Integer> entry : stateName2Id.entrySet()) {
			InstantiationUtil.serializeObject(checkpointStreamWithResultProvider.getCheckpointOutputStream(), entry.getKey());
			InstantiationUtil.serializeObject(checkpointStreamWithResultProvider.getCheckpointOutputStream(), entry.getValue());
		}
	}

	private void materializeKVStateData() throws IOException {
		Map<String, ColumnFamilyDescriptor> allColumnFamilyDescriptors = stateBackend.getColumnFamilyDescriptors();
		Map<String, ColumnFamilyHandle> allColumnFamilyHandles = stateBackend.getColumnFamilyHandles();
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream =
			checkpointStreamWithResultProvider.getCheckpointOutputStream();

		this.metaInfo = new HashMap<>();
		GroupSet groups = stateBackend.getGroups();
		for (int group : groups) {
			long offset = outputStream.getPos();
			int numEntries = 0;

			ByteArrayOutputStreamWithPos innerStream = new ByteArrayOutputStreamWithPos(GROUP_WRITE_BYTES + 1);
			StateSerializerUtil.writeGroup(innerStream, group);
			byte[] groupPrefix = innerStream.toByteArray();

			for (Map.Entry<String, ColumnFamilyHandle> entry : allColumnFamilyHandles.entrySet()) {
				RocksDBStorageInstance storageInstance = new RocksDBStorageInstance(stateBackend.getDbInstance(), entry.getValue(), stateBackend.getWriteOptions());
					RocksDBStoragePrefixIterator iterator = new RocksDBStoragePrefixIterator(storageInstance, groupPrefix);
					while (iterator.hasNext()) {
						Pair<byte[], byte[]> pair = iterator.next();
						IntSerializer.INSTANCE.serialize(stateName2Id.get(entry.getKey()), outputView);
						BytePrimitiveArraySerializer.INSTANCE.serialize(pair.getKey(), outputView);
						BytePrimitiveArraySerializer.INSTANCE.serialize(pair.getValue(), outputView);
						numEntries++;
					}
			}

			if (numEntries != 0) {
				metaInfo.put(group, new Tuple2<>(offset, numEntries));
			}
		}
	}
}
