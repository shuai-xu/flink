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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.IncrementalLocalStatePartitionSnapshot;
import org.apache.flink.runtime.state.IncrementalStatePartitionSnapshot;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import com.alibaba.niagara.NiagaraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * A State Backend that stores its state in {@code Niagara}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of Niagara.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * NIagara database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 */
public class NiagaraInternalStateBackend extends AbstractInternalStateBackend implements CheckpointListener {
	private static final Logger LOG = LoggerFactory.getLogger(NiagaraInternalStateBackend.class);

	NiagaraDBInstance db;

	NiagaraTabletInstance tabletInstance;

	/** Path where this configured instance stores its data directory. */
	private File instanceBasePath;

	/** Path where this configured instance stores its Niagara database. */
	private File instanceDBPath;

	/** The configuration for the Niagara backend. */
	private final NiagaraConfiguration configuration;

	/**
	 * Protects access to Niagara in other threads, like the checkpointing thread from parallel call that disposes the
	 * Niagara object.
	 */
	final ResourceGuard niagaraResourceGuard;

	/** True if incremental checkpointing is enabled. */
	private final boolean enableIncrementalCheckpointing;

	/**
	 * The state handle ids of all sst files materialized in snapshots for previous checkpoints.
	 * This sortedMap contains checkpointId as key, another map as value.
	 * And the value-map contains local {@link StateHandleID} as key,
	 * a {@link Tuple2} of (unique global id, {@link StreamStateHandle}) as value.
	 */
	final SortedMap<Long, Map<StateHandleID, Tuple2<String, StreamStateHandle>>> materializedSstFiles;

	/** The identifier of the last completed checkpoint. */
	long lastCompletedCheckpointId = -1L;

	/** The configuration of local recovery. */
	final LocalRecoveryConfig localRecoveryConfig;

	/** The snapshot strategy, e.g., if we use full or incremental checkpoints, local state, and so on. */
	private final SnapshotStrategy<SnapshotResult<StatePartitionSnapshot>> snapshotStrategy;

	NiagaraInternalStateBackend(
		ClassLoader userClassLoader,
		File instanceBasePath,
		NiagaraConfiguration configuration,
		int numberOfGroups,
		GroupSet groups,
		boolean enableIncrementalCheckpointing,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry) throws IOException {

		super(numberOfGroups, groups, userClassLoader, kvStateRegistry);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);
		this.instanceDBPath = new File(instanceBasePath, "db");

		checkAndCreateDirectory(instanceBasePath);

		if (instanceDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			cleanInstanceBasePath();
		}
		this.configuration = configuration;
		this.localRecoveryConfig = localRecoveryConfig;

		this.niagaraResourceGuard = new ResourceGuard();
		this.materializedSstFiles = new TreeMap<>();

		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.snapshotStrategy = enableIncrementalCheckpointing ?
			new NiagaraInternalStateBackend.IncrementalSnapshotStrategy() :
			new NiagaraInternalStateBackend.FullSnapshotStrategy();

		this.db = createNiagaraDBInstance(configuration);
	}

	@Override
	protected InternalState createInternalState(InternalStateDescriptor stateDescriptor) {
		return new NiagaraInternalState(this, stateDescriptor);
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {
		if (!enableIncrementalCheckpointing) {
			return;
		}

		synchronized (materializedSstFiles) {

			if (completedCheckpointId < lastCompletedCheckpointId) {
				return;
			}

			materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);

			lastCompletedCheckpointId = completedCheckpointId;
		}
	}

	@Override
	public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		return snapshotStrategy.performSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@Override
	public void restore(Collection<StatePartitionSnapshot> restoredSnapshots) throws Exception {
		LOG.info("Initializing Niagara internal state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoredSnapshots);
		}

		try {
			if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
				createDB();
				LOG.info("Successfully created Niagara internal state backend at {}.", instanceDBPath);
			} else {
				LOG.info("Restoring Niagara internal state backend at {}.", instanceDBPath);
				long startMillis = System.currentTimeMillis();

				StatePartitionSnapshot stateSnapshot = restoredSnapshots.iterator().next();
				if (stateSnapshot instanceof DefaultStatePartitionSnapshot) {
					NiagaraFullRestoreOperation operation = new NiagaraFullRestoreOperation(this);
					operation.restore(restoredSnapshots);
				} else if (stateSnapshot instanceof IncrementalStatePartitionSnapshot || stateSnapshot instanceof IncrementalLocalStatePartitionSnapshot) {
					NiagaraIncrementalRestoreOperation restoreOperation = new NiagaraIncrementalRestoreOperation(this);
					restoreOperation.restore(restoredSnapshots);
				} else {
					throw new UnsupportedOperationException("Unknown statePartitionSnapshot for Niagara internal state-backend to restore.");
				}

				long endMillis = System.currentTimeMillis();
				LOG.info("Successfully restored Niagara internal state-backend at {}, duration {} ms.", instanceDBPath, (endMillis - startMillis));
			}
		} catch (Exception ex) {
			closeImpl();
			throw ex;
		}
	}

	@Override
	protected void closeImpl() {
		// This call will block until all clients that still acquire access to the Niagara instance have released it,
		// so that we cannot release the native resources while clients are still working with it in parallel.
		niagaraResourceGuard.close();

		if (tabletInstance != null) {
			tabletInstance.close();
			tabletInstance = null;
		}

		if (db != null) {
			db.close();

			// invalidate the reference
			db = null;
		}

		cleanInstanceBasePath();
	}

	/**
	 * Returns the Niagara tablet using configuration.
	 *
	 * @return The Niagara tablet using configuration.
	 */
	public NiagaraConfiguration getConfiguration() {
		return configuration;
	}

	File getInstanceBasePath() {
		if (instanceBasePath == null) {
			throw new IllegalStateException("NiagaraInternalStateBackend has not been initialized," +
				" it's illegal to get the instance base path.");
		}
		return instanceBasePath;
	}

	public File getInstanceDBPath() {
		if (instanceDBPath == null) {
			throw new IllegalStateException("NiagaraInternalStateBackend has not been initialized," +
				" it's illegal to get the instance DB path.");
		}
		return instanceDBPath;
	}

	Path getLocalRestorePath(GroupRange groupRange) {
		Preconditions.checkNotNull(instanceBasePath);
		String dirName = String.format("%s-%d-%d",
			"restore",
			groupRange.getStartGroup(),
			groupRange.getEndGroup());
		return new Path(instanceBasePath.getAbsolutePath(), dirName);
	}

	CloseableRegistry getCancelStreamRegistry() {
		return cancelStreamRegistry;
	}

	NiagaraDBInstance createNiagaraDBInstance(NiagaraConfiguration configuration) {
		return new NiagaraDBInstance(configuration);
	}

	private void cleanInstanceBasePath() {
		LOG.info("Deleting existing instance base directory {}.", instanceBasePath);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete instance base path for Niagara: " + instanceBasePath, ex);
		}
	}

	public NiagaraTabletInstance getDbInstance() {
		if (tabletInstance == null) {
			try {
				createDB();
			} catch (IOException e) {
				throw new NiagaraInitException(e.getMessage());
			}
		}
		return tabletInstance;
	}

	void setDbInstance(NiagaraTabletInstance restoredDbInstance) {
		if (this.tabletInstance != null) {
			throw new IllegalStateException("It's illegal to set DB instance when the instance is already created, " +
				"it's only allowed to set DB instance when restoring the state backend.");
		}
		this.tabletInstance = restoredDbInstance;
	}

	private void createDB() throws IOException {
		try {
			Preconditions.checkArgument(this.tabletInstance == null, "Before creating Niagara tablet instance, it was created unexpectedly.");
			this.tabletInstance = new NiagaraTabletInstance(db.getNiagaraJNI(), instanceDBPath, instanceBasePath.toString(), configuration);
		} catch (NiagaraException e) {
			throw new IOException("Error while opening Niagara tablet instance at " + instanceDBPath, e);
		}
	}

	private static void checkAndCreateDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException("Not a directory: " + directory);
			}
		} else {
			if (!directory.mkdirs()) {
				throw new IOException(
					String.format("Could not create Niagara data directory at %s.", directory));
			}
		}
	}

	private class IncrementalSnapshotStrategy implements SnapshotStrategy<SnapshotResult<StatePartitionSnapshot>> {

		private final SnapshotStrategy<SnapshotResult<StatePartitionSnapshot>> savepointDelegate;

		public IncrementalSnapshotStrategy() {
			this.savepointDelegate = new NiagaraInternalStateBackend.FullSnapshotStrategy();
		}

		@Override
		public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> performSnapshot(
			long checkpointId,
			long checkpointTimestamp,
			CheckpointStreamFactory checkpointStreamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

			// for savepoints, we delegate to the full snapshot strategy because savepoints are always self-contained.
			if (CheckpointType.SAVEPOINT == checkpointOptions.getCheckpointType()) {
				return savepointDelegate.performSnapshot(
					checkpointId,
					checkpointTimestamp,
					checkpointStreamFactory,
					checkpointOptions);
			}

			if (getStates().isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Asynchronous Niagara snapshot performed on empty keyed state at {}. Returning null.", checkpointTimestamp);
				}
				return DoneFuture.of(SnapshotResult.empty());
			}

			SnapshotDirectory snapshotDirectory;

			if (localRecoveryConfig.isLocalRecoveryEnabled()) {
				// create a "permanent" snapshot directory for local recovery.
				LocalRecoveryDirectoryProvider directoryProvider = localRecoveryConfig.getLocalStateDirectoryProvider();
				File directory = directoryProvider.subtaskSpecificCheckpointDirectory(checkpointId);

				if (directory.exists()) {
					FileUtils.deleteDirectory(directory);
				}

				if (!directory.mkdirs()) {
					throw new IOException("Local state base directory for checkpoint " + checkpointId +
						" already exists: " + directory);
				}

				// introduces an extra directory because Niagara wants a non-existing directory for native checkpoints.
				File rdbSnapshotDir = new File(directory, "niagara_db");
				Path path = new Path(rdbSnapshotDir.toURI());
				// create a "permanent" snapshot directory because local recovery is active.
				snapshotDirectory = SnapshotDirectory.permanent(path);
			} else {
				// create a "temporary" snapshot directory because local recovery is inactive.
				Path path = new Path(instanceBasePath.getAbsolutePath(), "chk-" + checkpointId);
				snapshotDirectory = SnapshotDirectory.temporary(path);
			}

			final NiagaraIncrementalSnapshotOperation snapshotOperation =
				new NiagaraIncrementalSnapshotOperation(
					NiagaraInternalStateBackend.this,
					checkpointStreamFactory,
					snapshotDirectory,
					checkpointId);

			try {
				snapshotOperation.takeSnapshot();
			} catch (Exception e) {
				snapshotOperation.stop();
				snapshotOperation.releaseResources(true);
				throw e;
			}

			return new FutureTask<SnapshotResult<StatePartitionSnapshot>>(
				snapshotOperation::runSnapshot
			) {
				@Override
				public boolean cancel(boolean mayInterruptIfRunning) {
					snapshotOperation.stop();
					return super.cancel(mayInterruptIfRunning);
				}

				@Override
				protected void done() {
					snapshotOperation.releaseResources(isCancelled());
				}
			};
		}
	}

	private class FullSnapshotStrategy implements SnapshotStrategy<SnapshotResult<StatePartitionSnapshot>> {

		@Override
		public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> performSnapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory primaryStreamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

			long startTime = System.currentTimeMillis();

			if (getStates().isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Asynchronous Niagara snapshot performed on empty keyed state at {}. Returning null.",
						timestamp);
				}

				return DoneFuture.of(SnapshotResult.empty());
			}

			final SupplierWithException<CheckpointStreamWithResultProvider, Exception> supplier =

				// TODO support checkpoint with local restore
				localRecoveryConfig.isLocalRecoveryEnabled() &&
					(CheckpointType.SAVEPOINT != checkpointOptions.getCheckpointType()) ?

					() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					() -> CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory);

			final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();

			final NiagaraFullSnapshotOperation snapshotOperation =
				new NiagaraFullSnapshotOperation(
					NiagaraInternalStateBackend.this,
					checkpointId,
					supplier,
					snapshotCloseableRegistry);

			snapshotOperation.takeDBSnapShot();

			// implementation of the async IO operation, based on FutureTask
			AbstractAsyncCallableWithResources<SnapshotResult<StatePartitionSnapshot>> ioCallable =
				new AbstractAsyncCallableWithResources<SnapshotResult<StatePartitionSnapshot>>() {

					@Override
					protected void acquireResources() throws Exception {
						cancelStreamRegistry.registerCloseable(snapshotCloseableRegistry);
						snapshotOperation.openCheckpointStream();
					}

					@Override
					protected void releaseResources() throws Exception {
						closeLocalRegistry();
						releaseSnapshotOperationResources();
					}

					private void releaseSnapshotOperationResources() {
						// hold the db lock while operation on the db to guard us against async db disposal
						snapshotOperation.releaseSnapshotResources();
					}

					@Override
					protected void stopOperation() throws Exception {
						closeLocalRegistry();
					}

					private void closeLocalRegistry() {
						if (cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
							try {
								snapshotCloseableRegistry.close();
							} catch (Exception ex) {
								LOG.warn("Error closing local registry", ex);
							}
						}
					}

					@Nonnull
					@Override
					public SnapshotResult<StatePartitionSnapshot> performOperation() throws Exception {
						long startTime = System.currentTimeMillis();

						if (isStopped()) {
							throw new IOException("Niagara closed.");
						}

						snapshotOperation.writeDBSnapshot();

						LOG.info("Asynchronous Niagara snapshot ({}, asynchronous part) in thread {} took {} ms.",
							primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));

						return snapshotOperation.getStatePartitionSnapshot();
					}
				};

			LOG.info("Asynchronous Niagara snapshot ({}, synchronous part) in thread {} took {} ms.",
				primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));
			return AsyncStoppableTaskWithCallback.from(ioCallable);
		}
	}

	/**
	 * The exceptions thrown when the internal Niagara instance cannot be created.
	 */
	private class NiagaraInitException extends RuntimeException{
		private static final long serialVersionUID = 1L;

		NiagaraInitException(String message) {
			super("The Niagara init failed with reported message: " + message);
		}
	}
}
