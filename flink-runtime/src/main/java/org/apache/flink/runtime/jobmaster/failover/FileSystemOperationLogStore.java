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

package org.apache.flink.runtime.jobmaster.failover;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

/**
 * Implementation of {@link OperationLogStore} that store all {@link OperationLog}
 * on a {@link FileSystem}.
 */
public class FileSystemOperationLogStore implements OperationLogStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemOperationLogStore.class);

	private final Path workingDir;

	private FileSystem fileSystem;

	private Path filePath;

	private ObjectOutputStream outputStream;

	//------------------------- Constructor for testing -------------------------

	@VisibleForTesting
	FileSystemOperationLogStore (@Nonnull Path workingDir) {
		this.workingDir = workingDir;
	}

	//---------------------------------------------------------------------------

	/**
	 * Instantiates a new {@link FileSystemOperationLogStore}.
	 *
	 * @param jobID         the job id
	 * @param configuration the configuration
	 */
	public FileSystemOperationLogStore(
		@Nonnull JobID jobID,
		@Nonnull Configuration configuration
	) {
		// build working dir
		final String rootPath = configuration.getValue(HighAvailabilityOptions.HA_STORAGE_PATH);
		if (rootPath == null || StringUtils.isBlank(rootPath)) {
			throw new IllegalConfigurationException(
				String.format("Missing high-availability storage path for storing operation logs." +
					" Specify via configuration key '%s'.", HighAvailabilityOptions.HA_STORAGE_PATH));
		}

		this.workingDir = new Path(new Path(rootPath, jobID.toString()), "operation-logs");
	}

	/**
	 * Set the {@link OperationLogStore} to be ready to work. This method should
	 * be called before any other operates.
	 * @throws IOException
	 */
	@Override
	public void start() throws IOException {
		fileSystem = workingDir.getFileSystem();
		if (!fileSystem.exists(workingDir)) {
			fileSystem.mkdirs(workingDir);
		}

		filePath = new Path(workingDir, "operation.log");

		if (!fileSystem.exists(filePath)) {
			outputStream = new ObjectOutputStream(fileSystem.create(filePath, WriteMode.NO_OVERWRITE));
		}
	}

	/**
	 * Stop writing opLogs.
	 * @throws IOException
	 */
	@Override
	public void stop() throws IOException {
		if (outputStream != null) {
			outputStream.flush();
			outputStream.close();
			outputStream = null;
		}
	}

	/**
	 * Clear opLogs written so far by delete the file. {@link OperationLogStore}
	 * reaches a terminate state after be clear(). Call start() before any other
	 * operates.
	 * @throws IOException
	 */
	@Override
	public void clear() throws IOException {
		if (outputStream != null) {
			outputStream.close();
			outputStream = null;
		}

		fileSystem.delete(filePath, false);
	}

	@Override
	public void writeOpLog(@Nonnull OperationLog opLog) throws IOException {
		// outputStream would be null if you start() to write, then stop(),
		// and then re-start() to write. This is because we do not assume all
		// filesystems support append an existing file.
		// In this case, you should clear() the opLog store first, and then
		// re-start() to get a new opLog store.
		Preconditions.checkNotNull(outputStream);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Writing a operation log on file system at {}.", filePath);
		}

		outputStream.writeObject(opLog);
	}

	@Override
	public Iterable<OperationLog> opLogs() {
		return FileSystemOperationLogIterator::new;
	}

	class FileSystemOperationLogIterator implements Iterator<OperationLog> {

		private final ObjectInputStream inputStream;

		FileSystemOperationLogIterator() {
			try {
				inputStream = new ObjectInputStream(fileSystem.open(filePath));
			} catch (IOException e) {
				throw new FlinkRuntimeException("Cannot init filesystem opLog store.");
			}
		}

		@Override
		public boolean hasNext() {
			try {
				return inputStream.available() > 0;
			} catch (IOException e) {
				throw new FlinkRuntimeException("Cannot check hasNext on opLog store.");
			}
		}

		@Override
		public OperationLog next() {
			try {
				return (OperationLog)inputStream.readObject();
			} catch (Exception e) {
				throw new FlinkRuntimeException("Cannot read next opLog from opLog store.");
			}
		}
	}
}
