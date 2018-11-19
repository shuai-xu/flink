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

package com.alibaba.blink.state.niagara;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.IncrementalLocalStatePartitionSnapshot;
import org.apache.flink.runtime.state.IncrementalStatePartitionSnapshot;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.InstantiationUtil;

import com.alibaba.niagara.NiagaraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Incremental restore operation for Niagara InternalStateBackend.
 */
public class NiagaraIncrementalRestoreOperation {

	private static final Logger LOG = LoggerFactory.getLogger(NiagaraIncrementalRestoreOperation.class);

	private final NiagaraInternalStateBackend stateBackend;

	NiagaraIncrementalRestoreOperation(NiagaraInternalStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	private final CloseableRegistry closeableRegistry = new CloseableRegistry();

	void restore(Collection<StatePartitionSnapshot> restoredSnapshots) throws Exception {
		NiagaraTabletInstance tabletInstance = null;

		List<Path> candidatePaths = new ArrayList<>();

		try {
			long restoreSize = 0;
			for (StatePartitionSnapshot restoreStateSnapshot : restoredSnapshots) {
				restoreSize += restoreStateSnapshot.getStateSize();
			}

			LOG.info("Constructing the backend from {} instances, size {} bytes.", restoredSnapshots.size(), restoreSize);

			stateBackend.getCancelStreamRegistry().registerCloseable(closeableRegistry);

			if (restoredSnapshots.size() == 1) {
				StatePartitionSnapshot rawStateSnapshot = restoredSnapshots.iterator().next();

				// We must ensure newly-created state-backend have exactly the same groups as previous cached one.
				// For newly-created state-backend which have sub-group range of previous cache one, we cannot just link
				// files from backup one and then open it. This is due to we might include files having data not in this
				// state-backend's group range, the best solution is using Niagara's restoreCheckpoint API to restore which
				// will record its bytes' range in meta information.
				if (rawStateSnapshot.getGroups().equals(stateBackend.getGroups())) {
					stateBackend.setDbInstance(restoreIntegratedTabletInstance(rawStateSnapshot));
					return;
				}
			}

			for (StatePartitionSnapshot rawStateSnapshot : restoredSnapshots) {
				IncrementalStatePartitionSnapshot stateSnapshot = (IncrementalStatePartitionSnapshot) rawStateSnapshot;

				Path localRestorePath = stateBackend.getLocalRestorePath((GroupRange) stateSnapshot.getGroups());
				if (restoreFragmentedTablet(stateSnapshot, localRestorePath)) {
					candidatePaths.add(localRestorePath);
				}
			}
			File localDataPath = stateBackend.getInstanceDBPath();
			tabletInstance = new NiagaraTabletInstance(
				stateBackend.db.getNiagaraJNI(),
				localDataPath,
				localDataPath.toString(),
				stateBackend.getConfiguration(),
				(GroupRange) stateBackend.getGroups(),
				candidatePaths);
			stateBackend.setDbInstance(tabletInstance);

		} catch (Exception e) {
			LOG.error("Error while restoring Niagara instance ", e);
			if (tabletInstance != null) {
				tabletInstance.close();
			}
			throw e;
		} finally {
			for (Path candidatePath : candidatePaths) {
				try {
					FileSystem fileSystem = candidatePath.getFileSystem();
					if (fileSystem.exists(candidatePath)) {
						fileSystem.delete(candidatePath, true);
					}
				} catch (Exception ignored) {
					// ignore exception when deleting candidate tablet paths.
				}
			}
			stateBackend.getCancelStreamRegistry().unregisterCloseable(closeableRegistry);
			try {
				closeableRegistry.close();
			} catch (IOException e) {
				LOG.warn("Exception on closing registry.", e);
			}
		}

	}

	private NiagaraTabletInstance restoreIntegratedTabletInstance(
		StatePartitionSnapshot rawStateSnapshot
	) throws Exception {

		Map<StateHandleID, Tuple2<String, StreamStateHandle>> sstFiles = null;
		StreamStateHandle metaStateHandle = null;
		long checkpointID = -1;

		Path localDataPath = new Path(stateBackend.getInstanceDBPath().getAbsolutePath());
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
				// TODO support recovery from local.
			}
		} catch (Exception e) {
			LOG.info("Fail to restore Niagara instance at {}, and try to remove it if existed.", localDataPath);
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

		return createNiagaraInstance(localDataPath);
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
			if (closeableRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}

			if (closeableRegistry.unregisterCloseable(outputStream)) {
				outputStream.close();
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
			if (closeableRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}

	private boolean restoreFragmentedTablet(
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
				return false;
			}

			restoreMetaData(metaStateHandle);

			Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandles = stateSnapshot.getSharedState();
			for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> stateHandleEntry : sharedStateHandles.entrySet()) {
				String stateName = stateHandleEntry.getKey().getKeyString();
				StreamStateHandle stateHandle = stateHandleEntry.getValue().f1;
				restoreFile(localRestorePath, stateName, stateHandle);
			}

			Map<StateHandleID, StreamStateHandle> privateStateHandles = stateSnapshot.getPrivateState();
			for (Map.Entry<StateHandleID, StreamStateHandle> stateHandleEntry : privateStateHandles.entrySet()) {
				String oldFileName = stateHandleEntry.getKey().getKeyString();
				StreamStateHandle oldFileStateHandle = stateHandleEntry.getValue();
				restoreFile(localRestorePath, oldFileName, oldFileStateHandle);
			}

			long endMillis = System.currentTimeMillis();
			LOG.info("Successfully restored the instance at {}, {} ms.",
				localRestorePath, (endMillis - startMillis));
			return true;
		} catch (Exception e) {
			if (localFileSystem.exists(localRestorePath)) {
				localFileSystem.delete(localRestorePath, true);
			}

			throw e;
		}
	}

	private NiagaraTabletInstance createNiagaraInstance(File instanceNiagaraPath) throws NiagaraException {
		return new NiagaraTabletInstance(
			stateBackend.db.getNiagaraJNI(),
			instanceNiagaraPath,
			instanceNiagaraPath.toString(),
			stateBackend.getConfiguration());
	}

	private NiagaraTabletInstance createNiagaraInstance(Path instanceNiagaraPath) throws NiagaraException {
		return createNiagaraInstance(new File(instanceNiagaraPath.getPath()));
	}
}
