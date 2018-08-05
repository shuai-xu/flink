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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.RunnableFuture;

/**
 * A State Backend that stores its state in {@code RocksDB}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of rocksDB.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * rocksDB database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 */
public class RocksDBInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBInternalStateBackend.class);

	/** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
	public static final String MERGE_OPERATOR_NAME = "stringappendtest";

	/**
	 * Separator of StringAppendTestOperator in RocksDB.
	 */
	static final byte DELIMITER = ',';

	/** The DB options from the options factory. */
	private final DBOptions dbOptions;

	/** The column family options from the options factory. */
	private final ColumnFamilyOptions columnOptions;

	/**
	 * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call that disposes the
	 * RocksDb object.
	 */
	private final ResourceGuard rocksDBResourceGuard;

	private RocksDBInstance dbInstance;

	/** Path where this configured instance stores its data directory. */
	private File instanceBasePath;

	/** Path where this configured instance stores its RocksDB database. */
	private File instanceRocksDBPath;

	RocksDBInternalStateBackend(
		ClassLoader userClassLoader,
		File instanceBasePath,
		DBOptions dbOptions,
		ColumnFamilyOptions columnOptions,
		int numberOfGroups,
		GroupSet groups) throws IOException {

		super(numberOfGroups, groups, userClassLoader);

		this.dbOptions = Preconditions.checkNotNull(dbOptions);
		// ensure that we use the right merge operator, because other code relies on this
		this.columnOptions = Preconditions.checkNotNull(columnOptions)
			.setMergeOperatorName(MERGE_OPERATOR_NAME);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		checkAndCreateDirectory(instanceBasePath);

		if (instanceRocksDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			cleanInstanceBasePath();
		}

		this.rocksDBResourceGuard = new ResourceGuard();
	}

	@Override
	protected void closeImpl() {
		// This call will block until all clients that still acquire access to the RocksDB instance have released it,
		// so that we cannot release the native resources while clients are still working with it in parallel.
		rocksDBResourceGuard.close();

		// IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
		// working on the disposed object results in SEGFAULTS.
		if (dbInstance != null) {
			dbInstance.close();

			// invalidate the reference
			dbInstance = null;

			IOUtils.closeQuietly(columnOptions);
			IOUtils.closeQuietly(dbOptions);

			cleanInstanceBasePath();
		}

	}

	private void cleanInstanceBasePath() {
		LOG.info("Deleting existing instance base directory {}.", instanceBasePath);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
		}
	}

	@Override
	protected InternalState createInternalState(InternalStateDescriptor stateDescriptor) {
		return new RocksDBInternalState(this, stateDescriptor);
	}

	@Override
	public RunnableFuture<StatePartitionSnapshot> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		return null;
	}

	@Override
	public void restore(Collection<StatePartitionSnapshot> restoredSnapshots) throws Exception {
		LOG.info("Initializing RocksDB internal state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoredSnapshots);
		}

		try {
			if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
				createDB();
				LOG.info("Successfully created RocksDB state backend at {}.", instanceRocksDBPath);
			}
		} catch (Exception ex) {
			closeImpl();
			throw ex;
		}

	}

	public RocksDBInstance getDbInstance() {
		return dbInstance;
	}

	public File getInstanceBasePath() {
		if (instanceBasePath == null) {
			throw new IllegalStateException("RocksDBInternalStateBackend has not been initialized," +
				" it's illegal to get the instance base path.");
		}
		return instanceBasePath;
	}

	private void createDB() throws IOException {
		try {
			this.dbInstance = new RocksDBInstance(dbOptions, columnOptions, instanceRocksDBPath);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening rocksDB instance at " + instanceRocksDBPath, e);
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
					String.format("Could not create RocksDB data directory at %s.", directory));
			}
		}
	}

}
