/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.external.writer.PartitionHashFileWriter;
import org.apache.flink.runtime.io.network.partition.external.writer.PersistentFileWriter;
import org.apache.flink.runtime.io.network.partition.external.writer.PartitionMergeFileWriter;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * ExternalResultPartition is used when shuffling data through external shuffle service,
 * e.g. yarn shuffle service.
 */
public class ExternalResultPartition<T> extends ResultPartition<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ExternalResultPartition.class);

	private final Configuration taskManagerConfiguration;
	private final TaskActions taskActions;
	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final boolean sendScheduleOrUpdateConsumersMessage;
	private final ResultPartitionConsumableNotifier partitionConsumableNotifier;
	private final String partitionRootPath;

	private PersistentFileWriter<T> fileWriter;
	private boolean hasNotifiedExternalConsumers;

	private boolean initialized;

	public ExternalResultPartition(
		Configuration taskManagerConfiguration,
		String owningTaskName,
		TaskActions taskActions,
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		int numberOfSubpartitions,
		int numTargetKeyGroups,
		ResultPartitionConsumableNotifier partitionConsumableNotifier,
		MemoryManager memoryManager,
		IOManager ioManager,
		boolean sendScheduleOrUpdateConsumersMessage) {

		super(owningTaskName, jobId, partitionId, partitionType, numberOfSubpartitions, numTargetKeyGroups);

		this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
		this.taskActions = checkNotNull(taskActions);
		this.partitionConsumableNotifier = checkNotNull(partitionConsumableNotifier);
		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);

		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
		this.partitionRootPath = ExternalBlockShuffleUtils.generatePartitionRootPath(
			getSpillRootPath(taskManagerConfiguration, jobId.toString(), partitionId.toString()),
			partitionId.getProducerId().toString(),
			partitionId.getPartitionId().toString());
	}

	private void initialize() {
		checkNotNull(typeSerializer);
		checkNotNull(parentTask);

		try {
			Path tmpPartitionRootPath = new Path(partitionRootPath);
			FileSystem fs = FileSystem.getLocalFileSystem();
			if (fs.exists(tmpPartitionRootPath)) {
				// if partition root directory exists, we will delete the job root directory
				fs.delete(tmpPartitionRootPath, true);
			}
			int maxRetryCnt = 100;
			do {
				try {
					fs.mkdirs(tmpPartitionRootPath);
				} catch (IOException e) {
					if (maxRetryCnt-- > 0) {
						LOG.error("Fail to create partition root path: " + partitionRootPath
							+ ", left retry times: " + maxRetryCnt);
					} else {
						LOG.error("Reach retry limit, fail to create partition root path: " + partitionRootPath);
						throw e;
					}
				}
			} while (!fs.exists(tmpPartitionRootPath));

			double shuffleMemory = taskManagerConfiguration.getInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB);
			int numPages = (int) (shuffleMemory * 1024 * 1024 / memoryManager.getPageSize());
			List<MemorySegment> memory = memoryManager.allocatePages(parentTask, numPages);

			final int hashMaxSubpartitions = taskManagerConfiguration.getInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS);
			final int mergeFactor = taskManagerConfiguration.getInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR);
			final int mergeMaxDataFiles = taskManagerConfiguration.getInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_MAX_DATA_FILES);

			checkArgument(hashMaxSubpartitions > 0, "The max allowed number of subpartitions should be larger than 0, but actually is: " + hashMaxSubpartitions);
			checkArgument(mergeFactor > 0, "The merge factor should be larger than 0, but actually is: " + mergeFactor);
			checkArgument(mergeMaxDataFiles > 0, "The max data file should be larger than 0, but actually is: " + mergeMaxDataFiles);

			// If the memory amount is less that the number of subpartitions, it should enter partition merge process.
			if (numberOfSubpartitions <= hashMaxSubpartitions && numberOfSubpartitions <= memory.size()) {
				fileWriter = new PartitionHashFileWriter<T>(
					numberOfSubpartitions,
					partitionRootPath,
					memoryManager,
					memory,
					ioManager,
					typeSerializer);
			} else {
				fileWriter = new PartitionMergeFileWriter<T>(
					numberOfSubpartitions,
					partitionRootPath,
					mergeMaxDataFiles,
					mergeFactor,
					memoryManager,
					memory,
					ioManager,
					typeSerializer,
					parentTask);
			}

			initialized = true;

			LOG.info("External Result Partition (partitionId = {}) use {}, root = {}, numberOfSubpartitions = {}, " +
					"maxSubpartitionsForSingleFile = {}, maxDataFiles = {}, mergeFactor = {} memory.size() = {}",
				partitionId, fileWriter.getClass().getName(), partitionRootPath, numberOfSubpartitions,
				hashMaxSubpartitions, mergeMaxDataFiles, mergeFactor, memory.size());
		} catch (Throwable t) {
			deletePartitionDirOnFailure();
			throw new RuntimeException(t);
		}
	}

	@Override
	public void addRecord(T record, int[] targetChannels, boolean flushAlways) throws IOException, InterruptedException {
		if (!initialized) {
			initialize();
		}

		try {
			checkInProduceState();
			fileWriter.add(record, targetChannels);
		} catch (Throwable e) {
			deletePartitionDirOnFailure();
			throw e;
		}
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean flushAlways) throws IOException {
		throw new RuntimeException("Event is not supported in external result partition.");
	}

	@Override
	public void clearBuffers() {
		// No operations.
	}

	@Override
	public void flushAll() {
		// No operations.
	}

	@Override
	public void flush(int subpartitionIndex) {
		// No operations.
	}

	@Override
	protected void releaseInternal() {
		try {
			if (fileWriter != null) {
				fileWriter.clear();
				fileWriter = null;
			}
		} catch (IOException e) {
			LOG.error("Fail to clear external shuffler", e);
			ExceptionUtils.rethrow(e);
		}
	}

	@Override
	public void finish() throws IOException {
		try {
			checkInProduceState();

			FileSystem fs = FileSystem.get(new Path(partitionRootPath).toUri());

			fileWriter.finish();

			// write index files.
			List<List<PartitionIndex>> indicesList = fileWriter.generatePartitionIndices();
			for (int i = 0; i < indicesList.size(); ++i) {
				String indexPath = ExternalBlockShuffleUtils.generateIndexPath(partitionRootPath, i);

				try (FSDataOutputStream indexOut = fs.create(new Path(indexPath), FileSystem.WriteMode.OVERWRITE)) {
					DataOutputView indexView = new DataOutputViewStreamWrapper(indexOut);
					ExternalBlockShuffleUtils.serializeIndices(indicesList.get(i), indexView);
				}
			}

			// write finish files
			String finishedPath = ExternalBlockShuffleUtils.generateFinishedPath(partitionRootPath);
			try (FSDataOutputStream finishedOut = fs.create(new Path(finishedPath), FileSystem.WriteMode.OVERWRITE)) {
				DataOutputView finishedView = new DataOutputViewStreamWrapper(finishedOut);

				finishedView.writeInt(ExternalBlockResultPartitionMeta.SUPPORTED_PROTOCOL_VERSION);

				String externalFileType = fileWriter.getExternalFileType().name();
				finishedView.writeInt(externalFileType.length());
				finishedView.write(externalFileType.getBytes());
				finishedView.writeInt(indicesList.size());
				finishedView.writeInt(numberOfSubpartitions);
			}

			notifyExternalConsumers();
		} catch (Throwable e) {
			deletePartitionDirOnFailure();
			ExceptionUtils.rethrow(e);
		} finally {
			releaseInternal();
		}

		isFinished = true;
	}

	/**
	 * Notifies master that the result partition is ready to consume.
	 */
	private void notifyExternalConsumers() {
		if (sendScheduleOrUpdateConsumersMessage && !hasNotifiedExternalConsumers
			&& partitionType.isBlocking()) {
			partitionConsumableNotifier.notifyPartitionConsumable(jobId, partitionId, taskActions);

			hasNotifiedExternalConsumers = true;
		}
	}

	private void deletePartitionDirOnFailure() {
		// currently we only support local file
		FileSystem fileSystem = FileSystem.getLocalFileSystem();
		boolean deleteSuccess = false;
		try {
			deleteSuccess = fileSystem.delete(new Path(partitionRootPath), true);
			checkState(deleteSuccess, "Failed to delete dirty data.");
		} catch (Throwable e) {
			LOG.error("Exception occurred on deletePartitionDirOnFailure.", e);
			ExceptionUtils.rethrow(e);
		}
	}

	private String getSpillRootPath(
		Configuration configuration, String jobIdStr, String partitionIdStr) {

		String localShuffleDirs = configuration.getString(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS);

		if (localShuffleDirs.isEmpty()) {
			throw new IllegalStateException("The root dir for external result partition is not properly set. " +
				"Please check " + ExternalBlockShuffleServiceOptions.LOCAL_DIRS + " in hadoop configuration.");
		}

		String[] dirs = localShuffleDirs.split(",");
		Arrays.sort(dirs);

		int hashCode = ExternalBlockShuffleUtils.hashPartitionToDisk(jobIdStr, partitionIdStr);
		return dirs[hashCode % dirs.length];
	}
}
