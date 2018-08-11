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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.contrib.streaming.state.RocksDBInternalState.KEY_END_BYTE;

/**
 * Full snapshot related operations of RocksDB state backend.
 */
public class RocksDBFullSnapshotOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBFullSnapshotOperation.class);

	private final RocksDBInternalStateBackend stateBackend;

	private final long checkpointId;

	private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;

	private final CloseableRegistry snapshotCloseableRegistry;

	private final ResourceGuard.Lease dbLease;

	private CheckpointStreamWithResultProvider checkpointStreamWithResultProvider;

	private DataOutputView outputView;

	private Map<Integer, Tuple2<Long, Integer>> metaInfo;

	/** The snapshot directory containing all the data. */
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
	}

	/**
	 * 1) Create a snapshot object from RocksDB.
	 *
	 */
	public void takeDBSnapShot() throws IOException, RocksDBException {
		Preconditions.checkArgument(snapshotDirectory == null, "Only one ongoing snapshot allowed!");

		// create a "temporary" snapshot directory because local recovery is inactive.
		Path path = new Path(stateBackend.getInstanceBasePath().getAbsolutePath(), "chk-" + checkpointId);
		snapshotDirectory = SnapshotDirectory.temporary(path);
		LOG.info("Taking snapshot for RocksDB instance at {}.", snapshotDirectory.toString());
		stateBackend.getDbInstance().snapshot(snapshotDirectory.getDirectory().getPath());
	}

	/**
	 * 2) Open CheckpointStateOutputStream through the checkpointStreamFactory into which we will write.
	 *
	 * @throws Exception
	 */
	public void openCheckpointStream() throws Exception {
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
	public void writeDBSnapshot() throws Exception {

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
	public SnapshotResult<StatePartitionSnapshot> getStatePartitionSnapshot() throws IOException {

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
	public void releaseSnapshotResources() {

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
		outputView.writeInt(stateBackend.getStates().size());
		for (InternalState state : stateBackend.getStates().values()) {
			InternalStateDescriptor stateDescriptor = state.getDescriptor();
			InstantiationUtil.serializeObject(checkpointStreamWithResultProvider.getCheckpointOutputStream(), stateDescriptor);
		}
	}

	private void materializeKVStateData() throws RocksDBException, IOException {
		try (RocksDBInstance backupInstance = new RocksDBInstance(
				Preconditions.checkNotNull(stateBackend.dbOptions),
				Preconditions.checkNotNull(stateBackend.columnOptions),
				new File(snapshotDirectory.getDirectory().getPath()))) {

			CheckpointStreamFactory.CheckpointStateOutputStream outputStream =
				checkpointStreamWithResultProvider.getCheckpointOutputStream();

			this.metaInfo = new HashMap<>();

			GroupSet groups = stateBackend.getGroups();
			for (int group : groups) {

				long offset = outputStream.getPos();
				int numEntries = 0;

				for (InternalState state : stateBackend.getStates().values()) {
					InternalStateDescriptor stateDescriptor = state.getDescriptor();
					Tuple2<RowSerializer, RowSerializer> stateSerializer =
						stateBackend.getDuplicatedKVSerializers().get(stateDescriptor.getName());
					Preconditions.checkNotNull(stateSerializer);

					RowSerializer keySerializer = stateSerializer.f0;
					RowSerializer valueSerializer = stateSerializer.f1;

					byte[] stateNameBytes = ((RocksDBInternalState) state).stateNameBytes;

					byte[] groupPrefix = serializeGroupPrefix(group, stateNameBytes);
					byte[] groupPrefixEnd = serializeGroupPrefixEnd(group, stateNameBytes);

					Iterator<Pair<Row, Row>> iterator =
						new RocksDBStateRangeIterator<Pair<Row, Row>>(backupInstance, groupPrefix, groupPrefixEnd) {
							@Override
							public Pair<Row, Row> next() {
								return getNextEntry().getRowPair(stateDescriptor, keySerializer, valueSerializer);
							}
						};

					while (iterator.hasNext()) {
						Pair<Row, Row> pair = iterator.next();

						outputView.write(stateNameBytes);
						keySerializer.serialize(pair.getKey(), outputView);
						valueSerializer.serialize(pair.getValue(), outputView);

						numEntries++;
					}
				}

				if (numEntries != 0) {
					metaInfo.put(group, new Tuple2<>(offset, numEntries));
				}
			}
		}

	}

	private static byte[] serializeGroupPrefix(int group, byte[] stateNameBytes) {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

			RocksDBInternalState.writeInt(outputStream, group);
			outputView.write(stateNameBytes);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	private static byte[] serializeGroupPrefixEnd(int group, byte[] stateNameBytes) {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();

			RocksDBInternalState.writeInt(outputStream, group);
			outputStream.write(stateNameBytes);
			outputStream.write(KEY_END_BYTE);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}
}
