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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DummyKeyedStateBackend;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.TernaryBoolean;

import com.alibaba.niagara.NiagaraJNI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A State Backend that stores its state in {@code Niagara}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of Niagara.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * Niagara database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 *
 * <p>The behavior of the Niagara instances can be parametrized by setting Niagara configuration
 * via {@link NiagaraConfiguration}.
 */
public class NiagaraStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {
	private static final Logger LOG = LoggerFactory.getLogger(NiagaraStateBackend.class);

	// ------------------------------------------------------------------------
	//  Static configuration values
	// ------------------------------------------------------------------------

	private static final int NIAGARA_LIB_LOADING_ATTEMPTS = 3;

	private static boolean niagaraInitialized = false;

	// ------------------------------------------------------------------------

	// -- configuration values, set in the application / configuration

	/** The state backend that we use for creating checkpoint streams. */
	private final StateBackend checkpointStreamBackend;

	/** This determines if incremental checkpointing is enabled. */
	private final TernaryBoolean enableIncrementalCheckpointing;

	private NiagaraConfiguration configuration;

	// -- runtime values, set on TaskManager when initializing / using the backend

	/** Base paths for Niagara directory, as configured.
	 * Null if not yet set, in which case the configuration values will be used.
	 * The configuration defaults to the TaskManager's temp directories. */
	@Nullable
	private File[] localNiagaraDbDirectories;

	/** Base paths for Niagara directory, as initialized. */
	private transient File[] initializedDbBasePaths;

	/** JobID for uniquifying backup paths. */
	private transient JobID jobId;

	/** The index of the next directory to be used from {@link #initializedDbBasePaths}.*/
	private transient int nextDirectory;

	/** Whether we already lazily initialized our local storage directories. */
	private transient boolean isInitialized;

	/**
	 * Creates a new {@code NiagaraStateBackend} that uses the given state backend to store its
	 * checkpoint data streams. Typically, one would supply a filesystem or database state backend
	 * here where the snapshots from Niagara would be stored.
	 *
	 * <p>The snapshots of the Niagara state will be stored using the given backend's
	 * {@link StateBackend#createCheckpointStorage(JobID)}.
	 *
	 * @param checkpointStreamBackend The backend write the checkpoint streams to.
	 */
	public NiagaraStateBackend(StateBackend checkpointStreamBackend) {
		this(checkpointStreamBackend, TernaryBoolean.UNDEFINED, new NiagaraConfiguration());
	}

	public NiagaraStateBackend(StateBackend checkpointStreamBackend, boolean enableIncrementalCheckpointing) {
		this(checkpointStreamBackend, TernaryBoolean.fromBoolean(enableIncrementalCheckpointing), new NiagaraConfiguration());
	}

	public NiagaraStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing) {
		this(new FsStateBackend(new Path(checkpointDataUri).toUri()), TernaryBoolean.fromBoolean(enableIncrementalCheckpointing), new NiagaraConfiguration());
	}

	public NiagaraStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing, NiagaraConfiguration configuration) {
		this(new FsStateBackend(new Path(checkpointDataUri).toUri()), TernaryBoolean.fromBoolean(enableIncrementalCheckpointing), configuration);
	}

	/**
	 * Creates a new {@code NiagaraStateBackend} that uses the given state backend to store its
	 * checkpoint data streams. Typically, one would supply a filesystem or database state backend
	 * here where the snapshots from Niagara would be stored.
	 *
	 * <p>The snapshots of the Niagara state will be stored using the given backend's
	 * {@link StateBackend#createCheckpointStorage(JobID)}.
	 *
	 * @param checkpointStreamBackend The backend write the checkpoint streams to.
	 * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
	 */
	public NiagaraStateBackend(StateBackend checkpointStreamBackend, TernaryBoolean enableIncrementalCheckpointing, NiagaraConfiguration configuration) {
		this.checkpointStreamBackend = checkNotNull(checkpointStreamBackend);
		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.configuration = configuration;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint initialization and persistent storage
	// ------------------------------------------------------------------------

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
		return checkpointStreamBackend.resolveCheckpoint(pointer);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		return checkpointStreamBackend.createCheckpointStorage(jobId);
	}

	// ------------------------------------------------------------------------
	//  State holding data structures
	// ------------------------------------------------------------------------

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry) throws IOException {

		return new DummyKeyedStateBackend<>(kvStateRegistry, keySerializer, env.getUserClassLoader(), numberOfKeyGroups, keyGroupRange, env.getExecutionConfig());
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier) throws Exception {

		//the default for Niagara; eventually there can be a operator state backend based on Niagara, too.
		final boolean asyncSnapshots = true;
		return new DefaultOperatorStateBackend(
			env.getUserClassLoader(),
			env.getExecutionConfig(),
			asyncSnapshots);
	}

	@Override
	public AbstractInternalStateBackend createInternalStateBackend(
		Environment env,
		String operatorIdentifier,
		int numberOfGroups,
		GroupSet groups) throws Exception {

		// first, make sure that the Niagara JNI library is loaded
		// we do this explicitly here to have better error handling
		// TODO: Niagara must ensure the first loaded library path existing until whole process exited,
		// use current working dir to store Niagara's native library.
		String tempDir = System.getProperty("user.dir");
		ensureNiagaraIsLoaded(tempDir);

		// replace all characters that are not legal for filenames with underscore
		String fileCompatibleIdentifier = operatorIdentifier.replaceAll("[^a-zA-Z0-9\\-]", "_");

		lazyInitializeForJob(env, fileCompatibleIdentifier);

		File instanceBasePath = new File(
			getNextStoragePath(),
			"job_" + jobId + "_op_" + fileCompatibleIdentifier + "_uuid_" + UUID.randomUUID());

		LocalRecoveryConfig localRecoveryConfig =
			env.getTaskStateManager().createLocalRecoveryConfig();

		// Update configuration with environment
		this.configuration = configuration.loadConfiguration(env.getTaskManagerInfo().getConfiguration());

		return new NiagaraInternalStateBackend(
			env.getUserClassLoader(),
			instanceBasePath,
			this.configuration,
			numberOfGroups,
			groups,
			isIncrementalCheckpointsEnabled(),
			localRecoveryConfig,
			env.getTaskKvStateRegistry());
	}

	@Override
	public NiagaraStateBackend configure(Configuration config) throws IllegalConfigurationException {
		return new NiagaraStateBackend(this, config);
	}

	/**
	 * Gets the state backend that this Niagara state backend uses to persist
	 * its bytes to.
	 *
	 * <p>This Niagara state backend only implements the Niagara specific parts, it
	 * relies on the 'CheckpointBackend' to persist the checkpoint and savepoint bytes
	 * streams.
	 */
	public StateBackend getCheckpointBackend() {
		return checkpointStreamBackend;
	}

	/**
	 * Gets the configured local DB storage paths, or null, if none were configured.
	 *
	 * <p>Under these directories on the TaskManager, Niagara stores its SST files and
	 * metadata files. These directories do not need to be persistent, they can be ephermeral,
	 * meaning that they are lost on a machine failure, because state in Niagara is persisted
	 * in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 */
	public String[] getDbStoragePaths() {
		if (localNiagaraDbDirectories == null) {
			return null;
		} else {
			String[] paths = new String[localNiagaraDbDirectories.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = localNiagaraDbDirectories[i].toString();
			}
			return paths;
		}
	}

	/**
	 * Gets whether incremental checkpoints are enabled for this state backend.
	 */
	public boolean isIncrementalCheckpointsEnabled() {
		return enableIncrementalCheckpointing.getOrDefault(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue());
	}

	/**
	 * Private constructor that creates a re-configured copy of the state backend.
	 *
	 * @param original The state backend to re-configure.
	 * @param config The configuration.
	 */
	private NiagaraStateBackend(NiagaraStateBackend original, Configuration config) {
		// reconfigure the state backend backing the streams
		final StateBackend originalStreamBackend = original.checkpointStreamBackend;
		this.checkpointStreamBackend = originalStreamBackend instanceof ConfigurableStateBackend ?
			((ConfigurableStateBackend) originalStreamBackend).configure(config) :
			originalStreamBackend;

		// configure incremental checkpoints
		this.enableIncrementalCheckpointing = original.enableIncrementalCheckpointing.resolveUndefined(
			config.getBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS));

		// configure local directories
		if (original.localNiagaraDbDirectories != null) {
			this.localNiagaraDbDirectories = original.localNiagaraDbDirectories;
		}
		else {
			final String niagaraLocalPaths = config.getString(NiagaraConfiguration.LOCAL_DIRECTORIES);
			if (niagaraLocalPaths != null) {
				String[] directories = niagaraLocalPaths.split(",|" + File.pathSeparator);

				try {
					setDbStoragePaths(directories);
				}
				catch (IllegalArgumentException e) {
					throw new IllegalConfigurationException("Invalid configuration for Niagara state " +
						"backend's local storage directories: " + e.getMessage(), e);
				}
			}
		}

		this.configuration = original.configuration;
	}

	private void ensureNiagaraIsLoaded(String tempDirectory){
		// lock on something that cannot be in the user JAR
		synchronized (org.apache.flink.runtime.taskmanager.Task.class) {
			if (!niagaraInitialized) {

				final File tempDirParent = new File(tempDirectory).getAbsoluteFile();

				Throwable lastException = null;
				for (int attempt = 1; attempt <= NIAGARA_LIB_LOADING_ATTEMPTS; attempt++) {
					try {

						final File tempDirFile = new File(tempDirParent, "niagara-lib-" + UUID.randomUUID().toString());

						// make sure the temp path exists
						// noinspection ResultOfMethodCallIgnored
						tempDirFile.mkdirs();
						// TODO currently let YARN to cleanup this folder after application finishes.
						// tempDirFile.deleteOnExit();

						LOG.info("Attempting to load Niagara native library and store it at '{}', using classloader '{}'", tempDirFile, this.getClass().getClassLoader());

						// this initialization here should validate that the loading succeeded
						NiagaraJNI.loadLibrary(tempDirFile.getAbsolutePath());

						// seems to have worked
						LOG.info("Successfully loaded Niagara native library");
						niagaraInitialized = true;
						return;
					} catch (Throwable t) {
						lastException = t;
						LOG.debug("Niagara JNI library loading attempt {} failed", attempt, t);

						// try to force Niagara to attempt reloading the library
						try {
							resetNiagaraLoadedFlag();
						} catch (Throwable tt) {
							LOG.debug("Failed to reset 'initialized' flag in Niagara native code loader", tt);
						}
					}
				}

				throw new RuntimeException("Could not load the native Niagara library", lastException);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Parameters
	// ------------------------------------------------------------------------

	/**
	 * Sets the path where the Niagara local database files should be stored on the local
	 * file system. Setting this path overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param path The path where the local Niagara database files are stored.
	 */
	public void setDbStoragePath(String path) {
		setDbStoragePaths(path == null ? null : new String[] { path });
	}

	/**
	 * Sets the directories in which the local Niagara database puts its files (like SST and
	 * metadata files). These directories do not need to be persistent, they can be ephemeral,
	 * meaning that they are lost on a machine failure, because state in Niagara is persisted
	 * in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 *
	 * <p>Each distinct state will be stored in one path, but when the state backend creates
	 * multiple states, they will store their files on different paths.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param paths The paths across which the local Niagara database files will be spread.
	 */
	public void setDbStoragePaths(String... paths) {
		if (paths == null) {
			localNiagaraDbDirectories = null;
		}
		else if (paths.length == 0) {
			throw new IllegalArgumentException("empty paths");
		}
		else {
			File[] pp = new File[paths.length];

			for (int i = 0; i < paths.length; i++) {
				final String rawPath = paths[i];
				final String path;

				if (rawPath == null) {
					throw new IllegalArgumentException("null path");
				}
				else {
					// we need this for backwards compatibility, to allow URIs like 'file:///'...
					URI uri = null;
					try {
						uri = new Path(rawPath).toUri();
					}
					catch (Exception e) {
						// cannot parse as a path
					}

					if (uri != null && uri.getScheme() != null) {
						if ("file".equalsIgnoreCase(uri.getScheme())) {
							path = uri.getPath();
						}
						else {
							throw new IllegalArgumentException("Path " + rawPath + " has a non-local scheme");
						}
					}
					else {
						path = rawPath;
					}
				}

				pp[i] = new File(path);
				if (!pp[i].isAbsolute()) {
					throw new IllegalArgumentException("Relative paths are not supported");
				}
			}

			localNiagaraDbDirectories = pp;
		}
	}

	private void lazyInitializeForJob(
		Environment env,
		@SuppressWarnings("unused") String operatorIdentifier) throws IOException {

		if (isInitialized) {
			return;
		}

		this.jobId = env.getJobID();

		// initialize the paths where the local Niagara files should be stored
		if (localNiagaraDbDirectories == null) {
			// initialize from the temp directories
			initializedDbBasePaths = env.getIOManager().getSpillingDirectories();
		}
		else {
			List<File> dirs = new ArrayList<>(localNiagaraDbDirectories.length);
			StringBuilder errorMessage = new StringBuilder();

			for (File f : localNiagaraDbDirectories) {
				File testDir = new File(f, UUID.randomUUID().toString());
				if (!testDir.mkdirs()) {
					String msg = "Local DB files directory '" + f
						+ "' does not exist and cannot be created. ";
					LOG.error(msg);
					errorMessage.append(msg);
				} else {
					dirs.add(f);
				}
				//noinspection ResultOfMethodCallIgnored
				testDir.delete();
			}

			if (dirs.isEmpty()) {
				throw new IOException("No local storage directories available. " + errorMessage);
			} else {
				initializedDbBasePaths = dirs.toArray(new File[dirs.size()]);
			}
		}

		nextDirectory = new Random().nextInt(initializedDbBasePaths.length);

		isInitialized = true;
	}

	private File getNextStoragePath() {
		int ni = nextDirectory + 1;
		ni = ni >= initializedDbBasePaths.length ? 0 : ni;
		nextDirectory = ni;

		return initializedDbBasePaths[ni];
	}

	private static void resetNiagaraLoadedFlag() throws Exception {
		final Field initField = com.alibaba.niagara.NativeLibraryLoader.class.getDeclaredField("initialized");
		initField.setAccessible(true);
		initField.setBoolean(null, false);
	}
}
