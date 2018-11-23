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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.IncrementalLocalStatePartitionSnapshot;
import org.apache.flink.runtime.state.IncrementalStatePartitionSnapshot;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Incremental restore operation for RocksDB InternalStateBackend.
 */
public class RocksDBIncrementalRestoreOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final RocksDBInternalStateBackend stateBackend;

	RocksDBIncrementalRestoreOperation(RocksDBInternalStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	private final CloseableRegistry closeableRegistry = new CloseableRegistry();

	void restore(Collection<StatePartitionSnapshot> restoredSnapshots) throws Exception {
		if (restoredSnapshots.size() == 1) {
			StatePartitionSnapshot rawStateSnapshot = restoredSnapshots.iterator().next();

			if (rawStateSnapshot.getGroups().equals(stateBackend.getGroups())) {
				stateBackend.setDbInstance(restoreIntegratedTabletInstance(rawStateSnapshot));
				return;
			}
		}

		long restoreSize = 0;
		for (StatePartitionSnapshot restoreStateSnapshot : restoredSnapshots) {
			restoreSize += restoreStateSnapshot.getStateSize();
		}

		LOG.info("The group range is changed. Constructing the backend " +
			"from {} instances, size {} bytes.", restoredSnapshots.size(), restoreSize);
		stateBackend.getCancelStreamRegistry().registerCloseable(closeableRegistry);

		RocksDBInstance dbInstance = null;
		Collection<Tuple2<RocksDBInstance, GroupSet>> extraInstanceAndGroups = new ArrayList<>();
		Set<Path> extraInstancePaths = new HashSet<>();

		try {
			GroupRange groups = (GroupRange) stateBackend.getGroups();
			for (StatePartitionSnapshot rawStateSnapshot: restoredSnapshots) {
				if (!(rawStateSnapshot instanceof IncrementalStatePartitionSnapshot)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected: " + IncrementalStatePartitionSnapshot.class +
						", but found: " + rawStateSnapshot.getClass());
				}
				IncrementalStatePartitionSnapshot stateSnapshot = (IncrementalStatePartitionSnapshot) rawStateSnapshot;

				GroupRange snapshotGroups = (GroupRange) stateSnapshot.getGroups();
				if (dbInstance == null &&
					snapshotGroups.getStartGroup() >= groups.getStartGroup() &&
					snapshotGroups.getEndGroup() <= groups.getEndGroup()) {

					Path localDataPath = new Path(stateBackend.getInstanceRocksDBPath().getAbsolutePath());
					dbInstance = restoreFragmentedTabletInstance(stateSnapshot, localDataPath);
				} else {
					Path localRestorePath = stateBackend.getLocalRestorePath(snapshotGroups);
					RocksDBInstance extraInstance = restoreFragmentedTabletInstance(stateSnapshot, localRestorePath);
					extraInstancePaths.add(localRestorePath);
					extraInstanceAndGroups.add(Tuple2.of(extraInstance, snapshotGroups));
				}
			}

			if (dbInstance == null) {
				File localDataPath = stateBackend.getInstanceRocksDBPath();
				dbInstance = createRocksDBInstance(localDataPath);
			}

			for (Tuple2<RocksDBInstance, GroupSet> instanceAndGroups : extraInstanceAndGroups) {
				RocksDBInstance instance = instanceAndGroups.f0;
				GroupSet snapshotGroups = instanceAndGroups.f1;
				restoreStateData(dbInstance, instance, snapshotGroups);
			}
			stateBackend.setDbInstance(dbInstance);
		} catch (Exception e) {
			LOG.error("Error while restoring rocksDB instance ", e);
			if (dbInstance != null) {
				dbInstance.close();
			}
			throw e;
		} finally {
			for (Tuple2<RocksDBInstance, GroupSet> instanceAndGroups : extraInstanceAndGroups) {
				instanceAndGroups.f0.close();
			}

			for (Path extraInstancePath : extraInstancePaths) {
				try {
					FileSystem fileSystem = extraInstancePath.getFileSystem();
					if (fileSystem.exists(extraInstancePath)) {
						fileSystem.delete(extraInstancePath, true);
					}
				} catch (Exception ignored) {
					// ignore exception when deleting candidate tablet paths.
				}
			}
			stateBackend.getCancelStreamRegistry().unregisterCloseable(closeableRegistry);
		}
	}

	private RocksDBInstance restoreIntegratedTabletInstance(
		StatePartitionSnapshot rawStateSnapshot
	) throws Exception {

		Map<StateHandleID, Tuple2<String, StreamStateHandle>> sstFiles = null;
		StreamStateHandle metaStateHandle = null;
		long checkpointID = -1;

		Path localDataPath = new Path(stateBackend.getInstanceRocksDBPath().getAbsolutePath());
		FileSystem localFileSystem = localDataPath.getFileSystem();

		try {
			if (rawStateSnapshot instanceof IncrementalStatePartitionSnapshot) {
				LOG.info("Restoring from the remote file system.");

				IncrementalStatePartitionSnapshot restoredStateSnapshot = (IncrementalStatePartitionSnapshot) rawStateSnapshot;

				metaStateHandle = restoredStateSnapshot.getMetaStateHandle();
				Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandle = restoredStateSnapshot.getSharedState();

				// download the files into the local data path
				for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandleEntry : sharedStateHandle.entrySet()) {
					StateHandleID stateHandleID = sharedStateHandleEntry.getKey();
					String fileName = stateHandleID.getKeyString();
					StreamStateHandle stateHandle = sharedStateHandleEntry.getValue().f1;

					restoreFile(localDataPath, fileName, stateHandle);
				}

				for (Map.Entry<StateHandleID, StreamStateHandle> privateStateHandleEntry : restoredStateSnapshot.getPrivateState().entrySet()) {
					String stateName = privateStateHandleEntry.getKey().getKeyString();
					StreamStateHandle stateHandle = privateStateHandleEntry.getValue();
					restoreFile(localDataPath, stateName, stateHandle);
				}

				sstFiles = restoredStateSnapshot.getSharedState();
				checkpointID = restoredStateSnapshot.getCheckpointId();

			} else if (rawStateSnapshot instanceof IncrementalLocalStatePartitionSnapshot) {

				// Recovery from local incremental state.
				IncrementalLocalStatePartitionSnapshot restoredStateSnapshot = (IncrementalLocalStatePartitionSnapshot) rawStateSnapshot;
				LOG.info("Restoring from local recovery path {}.", restoredStateSnapshot.getDirectoryStateHandle().getDirectory());

				sstFiles = new HashMap<>();

				metaStateHandle = restoredStateSnapshot.getMetaStateHandle();

				for (Map.Entry<StateHandleID, String> entry : restoredStateSnapshot.getSharedStateHandleIDs().entrySet()) {
					StateHandleID stateHandleID = entry.getKey();
					String uniqueId = entry.getValue();

					sstFiles.put(stateHandleID, Tuple2.of(uniqueId, new PlaceholderStreamStateHandle()));
				}

				Path localRecoverDirectory = restoredStateSnapshot.getDirectoryStateHandle().getDirectory();
				FileStatus[] fileStatuses = localFileSystem.listStatus(localRecoverDirectory);

				if (!localFileSystem.mkdirs(localDataPath)) {
					throw new IOException("Cannot create local base path for RocksDB.");
				}

				if (fileStatuses == null) {
					throw new IOException("Cannot list file statues. Local recovery directory " + localRecoverDirectory + " does not exist.");
				}
				for (FileStatus fileStatus : fileStatuses) {
					String fileName = fileStatus.getPath().getName();

					File restoreFile = new File(localRecoverDirectory.getPath(), fileName);
					File targetFile = new File(localDataPath.getPath(), fileName);
					Files.createLink(targetFile.toPath(), restoreFile.toPath());
				}

				checkpointID = restoredStateSnapshot.getCheckpointId();
			}
		} catch (Exception e) {
			LOG.info("Fail to restore rocksDB instance at {}, and try to remove it if existed.", localDataPath);
			try {
				if (localFileSystem.exists(localDataPath)) {
					localFileSystem.delete(localDataPath, true);
				}
			} catch (IOException e1) {
				LOG.warn("Fail to remove local data path {} after restore operation failure.", localDataPath);
			}
			throw e;
		}

		// restore the state descriptors
		restoreMetaData(metaStateHandle);

		synchronized (stateBackend.materializedSstFiles) {
			stateBackend.materializedSstFiles.put(checkpointID, sstFiles);
		}
		stateBackend.lastCompletedCheckpointId = checkpointID;

		return createRocksDBInstance(localDataPath);
	}

	private void restoreFile(Path localRestorePath, String fileName, StreamStateHandle restoreStateHandle) throws IOException {
		Path localFilePath = new Path(localRestorePath, fileName);
		FileSystem localFileSystem = localFilePath.getFileSystem();

		FSDataInputStream inputStream = null;
		FSDataOutputStream outputStream = null;

		try {
			long startMillis = System.currentTimeMillis();

			inputStream = restoreStateHandle.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			outputStream = localFileSystem.create(localFilePath, FileSystem.WriteMode.OVERWRITE);
			closeableRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[64 * 1024];
			while (true) {
				int numBytes = inputStream.read(buffer);

				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}

			long endMillis = System.currentTimeMillis();
			LOG.debug("Successfully restored file {} from {}, {} bytes, {} ms",
				localFilePath, restoreStateHandle, restoreStateHandle.getStateSize(),
				(endMillis - startMillis));

			outputStream.close();
			closeableRegistry.unregisterCloseable(outputStream);
			outputStream = null;

			inputStream.close();
			closeableRegistry.unregisterCloseable(inputStream);
			inputStream = null;
		} finally {
			if (inputStream != null) {
				inputStream.close();
				closeableRegistry.unregisterCloseable(inputStream);
			}

			if (outputStream != null) {
				outputStream.close();
				closeableRegistry.unregisterCloseable(outputStream);
			}
		}
	}

	private void restoreMetaData(StreamStateHandle metaStateDatum) throws Exception {
		FSDataInputStream inputStream = null;

		try {
			inputStream = metaStateDatum.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			int numRestoredStates = inputView.readInt();
			for (int i = 0; i < numRestoredStates; ++i) {
				InternalStateDescriptor restoredStateDescriptor =
					InstantiationUtil.deserializeObject(
						inputStream, stateBackend.getUserClassLoader());

				stateBackend.getInternalState(restoredStateDescriptor);
			}

			inputStream.close();
			closeableRegistry.unregisterCloseable(inputStream);
			inputStream = null;
		} finally {
			if (inputStream != null) {
				inputStream.close();
				closeableRegistry.unregisterCloseable(inputStream);
			}
		}
	}

	private void restoreStateData(
		RocksDBInstance dbInstance,
		RocksDBInstance extraInstance,
		GroupSet groups
	) throws RocksDBException {
		long numEntries = 0;
		long startMillis = System.currentTimeMillis();

		try (RocksIterator iterator = extraInstance.iterator();
			RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(dbInstance.getDb(), dbInstance.getWriteOptions())) {
			for (int group : stateBackend.getGroups().intersect(groups)) {

				byte[] keyGroupPrefix = serializeGroupPrefix(group);
				iterator.seek(keyGroupPrefix);

				while (iterator.isValid()) {
					if (!RocksDBInstance.isPrefixWith(iterator.key(), keyGroupPrefix)) {
						break;
					}

					writeBatchWrapper.put(dbInstance.getDefaultColumnFamily(), iterator.key(), iterator.value());
					numEntries++;

					iterator.next();
				}
			}
		}

		long endMillis = System.currentTimeMillis();
		LOG.info("Successfully loaded {} state of {} entries from restore instance, {} ms.",
			stateBackend.getStates().size(), numEntries, (endMillis - startMillis));
	}

	private RocksDBInstance restoreFragmentedTabletInstance(
		IncrementalStatePartitionSnapshot stateSnapshot,
		Path localRestorePath
	) throws Exception {

		long startMillis = System.currentTimeMillis();

		FileSystem localFileSystem = localRestorePath.getFileSystem();
		if (localFileSystem.exists(localRestorePath)) {
			localFileSystem.delete(localRestorePath, true);
		}
		localFileSystem.mkdirs(localRestorePath);

		try {
			StreamStateHandle metaStateHandle = stateSnapshot.getMetaStateHandle();

			if (metaStateHandle == null) {
				return createRocksDBInstance(localRestorePath);
			}

			restoreMetaData(metaStateHandle);

			Map<StateHandleID, Tuple2<String, StreamStateHandle>> newFileStateHandles = stateSnapshot.getSharedState();
			for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> stateHandleEntry : newFileStateHandles.entrySet()) {
				String stateName = stateHandleEntry.getKey().getKeyString();
				StreamStateHandle stateHandle = stateHandleEntry.getValue().f1;
				restoreFile(localRestorePath, stateName, stateHandle);
			}

			Map<StateHandleID, StreamStateHandle> oldFileStateHandles = stateSnapshot.getPrivateState();
			for (Map.Entry<StateHandleID, StreamStateHandle> oldFileEntry : oldFileStateHandles.entrySet()) {
				String oldFileName = oldFileEntry.getKey().getKeyString();
				StreamStateHandle oldFileStateHandle = oldFileEntry.getValue();
				restoreFile(localRestorePath, oldFileName, oldFileStateHandle);
			}

			long endMillis = System.currentTimeMillis();
			LOG.info("Successfully restored the instance at {}, {} bytes, {} ms.",
				localRestorePath, stateSnapshot.getStateSize(), (endMillis - startMillis));

			return createRocksDBInstance(localRestorePath);
		} catch (Exception e) {
			if (localFileSystem.exists(localRestorePath)) {
				localFileSystem.delete(localRestorePath, true);
			}
			throw e;
		}
	}

	private static byte[] serializeGroupPrefix(int group) {
		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
		RocksDBInternalState.writeInt(outputStream, group);

		return outputStream.toByteArray();
	}

	private RocksDBInstance createRocksDBInstance(File instanceRocksDBPath) throws RocksDBException {
		return new RocksDBInstance(
			Preconditions.checkNotNull(stateBackend.dbOptions),
			Preconditions.checkNotNull(stateBackend.columnOptions),
			instanceRocksDBPath);
	}

	private RocksDBInstance createRocksDBInstance(Path instanceRocksDBPath) throws RocksDBException {
		return createRocksDBInstance(new File(instanceRocksDBPath.getPath()));
	}
}

