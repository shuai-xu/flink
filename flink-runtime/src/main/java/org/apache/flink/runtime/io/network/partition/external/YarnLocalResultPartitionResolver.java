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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Yarn implementation of LocalResultPartitionResolver.
 */
public class YarnLocalResultPartitionResolver extends LocalResultPartitionResolver {
	private static final Logger LOG = LoggerFactory.getLogger(YarnLocalResultPartitionResolver.class);

	private static final int RESULT_PARTITION_MAP_INITIAL_CAPACITY = 2000;

	private static final int APP_ID_MAP_INITIAL_CAPACITY = 100;

	private final FileSystem fileSystem;

	/** Only search result partitions in these applications' directories. */
	private final ConcurrentHashMap<String, String > appIdToUser = new ConcurrentHashMap<>(
		APP_ID_MAP_INITIAL_CAPACITY);

	/** To accelerate the mapping from ResultPartitionID to its directory.
	 *  Since this concurrent hash map is not guarded by any lock, don't use PUT operation
	 *  in order to make sure that no information losses.
	 */
	private final ConcurrentHashMap<ResultPartitionID, ResultPartitionFileInfo>
		resultPartitionMap = new ConcurrentHashMap<>(RESULT_PARTITION_MAP_INITIAL_CAPACITY);

	/**
	 * The thread is designed to do expensive recursive directory deletion, only recycle result partition files
	 *  in flink session mode since NodeManager can do the full recycle after stopApplication.
	 */
	private final ScheduledExecutorService diskScanner;

	private long lastDiskScanTimestamp = -1L;

	/**
	 * Since YarnShuffleService runs as a module in NodeManager process, while the users of
	 * result partitions' producers vary according to application submitters, in mostly common scenarios
	 * those two kinds of users are not the same. So we cannot just use FileSystem.delete() to
	 * do recycling in YarnShuffleService. Here we use a hadoop tool named "container-executor"
	 * which is used by NodeManager to do privileged operations including recycling containers'
	 * working directories.
	 */
	private final String containerExecutorExecutablePath;

	YarnLocalResultPartitionResolver(ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration) {
		super(shuffleServiceConfiguration);

		this.fileSystem = shuffleServiceConfiguration.getFileSystem();

		this.containerExecutorExecutablePath = parseContainerExecutorExecutablePath();

		diskScanner = Executors.newSingleThreadScheduledExecutor();
		diskScanner.scheduleWithFixedDelay(
			() -> doDiskScan(),
			0,
			shuffleServiceConfiguration.getDiskScanIntervalInMS(),
			TimeUnit.MILLISECONDS);
	}

	@Override
	void initializeApplication(String user, String appId) {
		appIdToUser.putIfAbsent(appId, user);
	}

	@Override
	Set<ResultPartitionID> stopApplication(String appId) {
		// Don't need to deal with partition files because NodeManager will recycle application's directory.
		Set<ResultPartitionID> toRemove = new HashSet<>();
		Iterator<Map.Entry<ResultPartitionID, ResultPartitionFileInfo>> partitionIterator =
			resultPartitionMap.entrySet().iterator();
		while (partitionIterator.hasNext()) {
			Map.Entry<ResultPartitionID, ResultPartitionFileInfo> entry = partitionIterator.next();
			if (entry.getValue().getAppId().equals(appId)) {
				toRemove.add(entry.getKey());
				partitionIterator.remove();
			}
		}
		appIdToUser.remove(appId);
		return toRemove;
	}

	@Override
	Tuple2<String, String> getResultPartitionDir(ResultPartitionID resultPartitionID) throws IOException {
		ResultPartitionFileInfo fileInfo = resultPartitionMap.get(resultPartitionID);
		if (fileInfo != null) {
			if (!fileInfo.isReadyToBeConsumed()) {
				updateUnfinishedResultPartition(resultPartitionID, fileInfo);
			}
			fileInfo.updateOnConsumption();
			return fileInfo.getRootDirAndPartitionDir();
		}

		// Cache miss, scan configured directories to search for the result partition's directory.
		fileInfo = searchResultPartitionDir(resultPartitionID);
		if (fileInfo == null) {
			throw new IOException("Cannot find result partition's directory, result partition id: " +
				resultPartitionID);
		}
		return fileInfo.getRootDirAndPartitionDir();
	}

	@Override
	void recycleResultPartition(ResultPartitionID resultPartitionID) {
		ResultPartitionFileInfo fileInfo = resultPartitionMap.get(resultPartitionID);
		if (fileInfo != null) {
			fileInfo.markToDelete(); // Lazy deletion, do real deletion during disk scan.
		}
	}

	@Override
	void stop() {
		LOG.warn("stop YarnLocalResultPartitionResolver.");
		try {
			diskScanner.shutdownNow();
		} catch (Throwable e) {
			LOG.error("Exception occurs when stopping YarnLocalResultPartitionResolver", e);
		}
	}

	// ------------------------------------- Internal Utilities -----------------------------------

	/**
	 * NodeManager will generate one dir for an application in each local dir,
	 * this method is a helper method to get the application's local dir relative to
	 * actual dirs in different disks.
	 *
	 * <p>See {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer}
	 */
	private static String generateRelativeLocalAppDir(String user, String appId) {
		return "usercache/" + user + "/appcache/" + appId + "/";
	}

	/**
	 * Check and update a previously unfinished result partition, if it has finished, update
	 * its file information.
	 *
	 * @param resultPartitionID Result partition id.
	 * @param fileInfo Previous file information of this result partition.
	 * @return If this result partition is ready to be consumed, return true, otherwise return false.
	 */
	private void updateUnfinishedResultPartition(
		ResultPartitionID resultPartitionID,
		ResultPartitionFileInfo fileInfo) throws IOException {

		String finishedFilePath = ExternalBlockShuffleUtils.generateFinishedPath(
			fileInfo.getRootDirAndPartitionDir().f1);
		try {
			// Use finishedFile to get the partition ready time.
			FileStatus fileStatus = fileSystem.getFileStatus(new Path(finishedFilePath));
			if (fileStatus != null && !fileStatus.isDir()) {
				fileInfo.setReadyToBeConsumed(fileStatus.getModificationTime());
				return;
			}
		} catch (Exception e) {
		}
		// The result partition is still unfinished.
		throw new IOException("Result partition is not ready to be consumed, result partition id: " +
			resultPartitionID);
	}

	/**
	 * Search the directory for a result partition if fail to find its directory in result partition cache.
	 * @param resultPartitionID
	 * @return The information of this result partition's data files.
	 */
	private ResultPartitionFileInfo searchResultPartitionDir(ResultPartitionID resultPartitionID) {
		// Search through all the running applications.
		for (Map.Entry<String, String> appIdAndUser : appIdToUser.entrySet()) {
			// Search through all the configured root directories.
			String appId = appIdAndUser.getKey();
			String user = appIdAndUser.getValue();
			String relativePartitionDir = ExternalBlockShuffleUtils.generatePartitionRootPath(
				generateRelativeLocalAppDir(user, appId),
				resultPartitionID.getProducerId().toString(),
				resultPartitionID.getPartitionId().toString());

			for (String rootDir : shuffleServiceConfiguration.getDirToDiskType().keySet()) {
				String partitionDir = rootDir + relativePartitionDir;
				String finishedFilePath = ExternalBlockShuffleUtils.generateFinishedPath(partitionDir);
				try {
					// Use finishedFile to get the partition ready time.
					FileStatus fileStatus = fileSystem.getFileStatus(new Path(finishedFilePath));
					if (fileStatus != null && !fileStatus.isDir()) {
						ResultPartitionFileInfo fileInfo =
							new ResultPartitionFileInfo(
								appId,
								new Tuple2<>(rootDir, partitionDir),
								true,
								true,
								fileStatus.getModificationTime(),
								System.currentTimeMillis());
						ResultPartitionFileInfo prevFileInfo = resultPartitionMap.putIfAbsent(
							resultPartitionID, fileInfo);
						if (prevFileInfo != null) {
							if (!prevFileInfo.isReadyToBeConsumed()) {
								prevFileInfo.setReadyToBeConsumed(fileStatus.getModificationTime());
							}
							prevFileInfo.updateOnConsumption();
							fileInfo = prevFileInfo;
						}
						return fileInfo;
					}
				} catch (Exception e) {
					// Do nothing.
				}
			}
		}
		return null;
	}

	/**
	 * This method will be triggered periodically to achieve the following four goals:
	 * (1) Generate the latest result partition cache for accelerating getResultPartitionDir.
	 * (2) Recycle result partitions which are ready to be fetched but haven't been consumed for a period of time.
	 * (3) Recycle result partitions which are unfinished but haven't been accessed for a period of time.
	 * (4) Recycle consumed result partitions according to ExternalBlockResultPartitionManager's demand.
	 */
	private void doDiskScan() {
		long currTime = System.currentTimeMillis();

		// 1. Traverse all the result partition directories including unfinished result partitions.
		// Traverse all the applications.
		Set<String> rootDirs = shuffleServiceConfiguration.getDirToDiskType().keySet();
		for (Map.Entry<String, String> userAndAppId : appIdToUser.entrySet()) {
			String user = userAndAppId.getValue();
			String appId = userAndAppId.getKey();
			String relativeAppDir = generateRelativeLocalAppDir(user, appId);
			// Traverse all the configured directories for this application.
			for (String rootDir : rootDirs) {
				String appDir = rootDir + relativeAppDir;
				FileStatus[] appDirStatuses = null;
				try {
					appDirStatuses = fileSystem.listStatus(new Path(appDir));
				} catch (Exception e) {
					continue;
				}
				if (appDirStatuses == null) {
					continue;
				}
				// Traverse all the result partitions of this application in this root directory.
				for (FileStatus partitionDirStatus : appDirStatuses) {
					if (!partitionDirStatus.isDir()) {
						continue;
					}

					ResultPartitionID resultPartitionID = ExternalBlockShuffleUtils
						.convertRelativeDirToResultPartitionID(partitionDirStatus.getPath().getName());
					if (resultPartitionID == null) {
						continue;
					}

					updateResultPartitionFileInfoByFileStatus(
						currTime, appId, resultPartitionID, partitionDirStatus, rootDir, appDir);
				}
			}
		}

		// 2. Remove out-of-date caches and partition files to be deleted through resultPartitionMap.
		Iterator<Map.Entry<ResultPartitionID, ResultPartitionFileInfo>> partitionIterator =
			resultPartitionMap.entrySet().iterator();
		while (partitionIterator.hasNext()) {
			Map.Entry<ResultPartitionID, ResultPartitionFileInfo> entry = partitionIterator.next();
			ResultPartitionFileInfo fileInfo = entry.getValue();
			boolean needToRemoveFileInfo = false;
			if (fileInfo.needToDelete()) {
				needToRemoveFileInfo = true;
				// Cannot distinguish between CONSUMED_PARTITION_TTL_TIMEOUT and PARTIAL_CONSUMED_PARTITION_TTL_TIMEOUT,
				// ExternalBlockResultPartitionManager has already logged detailed information before.
				removeResultPartition(new Path(fileInfo.rootDirAndPartitionDir.f1),
					"FETCHED_PARTITION_TTL_TIMEOUT",
					-1,
					false);
			} else if (!fileInfo.isConsumed()) {
				long lastActiveTime = fileInfo.getPartitionReadyTime();
				if (currTime - lastActiveTime > shuffleServiceConfiguration.getUnconsumedPartitionTTL()) {
					needToRemoveFileInfo = true;
					removeResultPartition(new Path(fileInfo.rootDirAndPartitionDir.f1),
						"UNCONSUMED_PARTITION_TTL_TIMEOUT",
						lastActiveTime,
						true);
				}
			} else if (fileInfo.getFileInfoTimestamp() <= lastDiskScanTimestamp) {
				// The partition's file no longer exists, just delete its file info.
				needToRemoveFileInfo = true;
			}
			if (needToRemoveFileInfo) {
				partitionIterator.remove();
			}
		}

		// 3. Update disk scan timestamp.
		lastDiskScanTimestamp = currTime;
	}

	private void updateResultPartitionFileInfoByFileStatus(
		long currTime,
		String appId,
		ResultPartitionID resultPartitionID,
		FileStatus partitionDirStatus,
		String rootDir,
		String appDir) {

		ResultPartitionFileInfo fileInfo = resultPartitionMap.get(resultPartitionID);
		if (fileInfo != null) {
			fileInfo.updateFileInfoTimestamp(currTime);
			// Don't need to check finished file for a finished result partition.
			if (fileInfo.isReadyToBeConsumed()) {
				return;
			}
		}

		// Check whether this result partition is ready to be consumed.
		FileStatus finishFileStatus = null;
		String partitionDir = appDir + partitionDirStatus.getPath().getName() + "/";
		String finishFilePath = ExternalBlockShuffleUtils.generateFinishedPath(partitionDir);
		try {
			finishFileStatus = fileSystem.getFileStatus(new Path(finishFilePath));
		} catch (Exception e) {
			// probably the partition is still in writing progress
			finishFileStatus = null;
		}

		if (finishFileStatus != null && !finishFileStatus.isDir()) {
			if (fileInfo == null) {
				fileInfo = new ResultPartitionFileInfo(
					appId,
					new Tuple2<>(rootDir, partitionDir),
					true,
					false,
					finishFileStatus.getModificationTime(),
					currTime);
				ResultPartitionFileInfo prevFileInfo = resultPartitionMap.putIfAbsent(
					resultPartitionID, fileInfo);
				if (prevFileInfo != null) {
					fileInfo = prevFileInfo;
				}
			}
			if (!fileInfo.isReadyToBeConsumed()) {
				fileInfo.setReadyToBeConsumed(finishFileStatus.getModificationTime());
			}
		} else {
			if (fileInfo == null) {
				fileInfo = new ResultPartitionFileInfo(
					appId,
					new Tuple2<>(rootDir, partitionDir),
					false,
					false,
					-1L,
					currTime);
				ResultPartitionFileInfo prevFileInfo = resultPartitionMap.putIfAbsent(
					resultPartitionID, fileInfo);
				if (prevFileInfo != null) {
					fileInfo = prevFileInfo;
				}
			}
			if (!fileInfo.isReadyToBeConsumed()) {
				// If this producer doesn't finish writing, we can only use dir's access time to judge.
				long lastActiveTime = partitionDirStatus.getModificationTime();
				if (currTime - lastActiveTime > shuffleServiceConfiguration.getUnfinishedPartitionTTL()) {
					removeResultPartition(partitionDirStatus.getPath(),
						"UNFINISHED_PARTITION_TTL_TIMEOUT",
						lastActiveTime,
						true);
					resultPartitionMap.remove(resultPartitionID);
				}
			}
		}
	}

	private String parseContainerExecutorExecutablePath() {
		String yarnHomeEnvVar = System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
		File hadoopBin = new File(yarnHomeEnvVar, "bin");
		String defaultContainerExecutorPath = new File(hadoopBin, "container-executor").getAbsolutePath();
		String containerExecutorPath = shuffleServiceConfiguration.getConfiguration().getString(
			YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, defaultContainerExecutorPath);

		// Check whether container-executor file exists.
		try {
			if (fileSystem.exists(new Path(containerExecutorPath))) {
				LOG.info("Use container-executor to do recycling, file path: " + containerExecutorPath);
				return containerExecutorExecutablePath;
			}
		} catch (IOException e) {
			// Do nothing.
		}
		throw new IllegalArgumentException("Invalid container-executor configuration: " + containerExecutorPath);
	}

	/**
	 * According to container-executor's help message:
	 *     container-executor ${user} ${yarn-user} ${command} ${command-args}
	 *     where command and command-args:
	 *         delete as user:         3 relative-path
	 * Both user and yarn-user will use the user of the application.
	 *
	 * <p>Get user name by partition directory according to file structure arranged by NodeManager
	 * @see YarnLocalResultPartitionResolver#generateRelativeLocalAppDir , the format of a partition directory is:
	 * ${ConfiguredRootDir}/usercache/${user}/appcache/${appId}/partition_${producerId}_${partitionId} .
	 */
	private void removeResultPartition(Path partitionDir, String recycleReason, long lastActiveTime, boolean printLog) {
		try {
			String user = partitionDir.getParent().getParent().getParent().getName();
			Process process = Runtime.getRuntime().exec(new String[] {
				containerExecutorExecutablePath,
				user,
				user,
				"3",
				partitionDir.toString()
			});
			boolean processExit = process.waitFor(30, TimeUnit.SECONDS);
			if (processExit) {
				int exitCode = process.exitValue();
				if (exitCode != 0) {
					LOG.warn("Fail to delete partition's directory: {}, reason: {}, lastActiveTime: {}, exitCode: {}.",
						partitionDir, recycleReason, lastActiveTime, exitCode);
				} else if (printLog) {
					LOG.info("Delete partition's directory: {}, reason: {}, lastActiveTime: {}.",
						partitionDir, recycleReason, lastActiveTime);
				}
			} else {
				LOG.warn("Delete partition's directory for more than 30 seconds: {}, reason: {}, "
					+ "lastActiveTime: {}.", partitionDir, recycleReason, lastActiveTime);
			}
		} catch (Exception e) {
			LOG.warn("Fail to delete partition's directory: {}, exception:", partitionDir, e);
		}
	}

	/**
	 * Hold the information of a result partition's files for searching and recycling.
	 */
	private static class ResultPartitionFileInfo {

		/** The application id of this result partition's producer. */
		private final String appId;

		/** Configured root dir and result partition's directory path. */
		private final Tuple2<String, String> rootDirAndPartitionDir;

		/** Whether the producer finishes generating this result partition. */
		private volatile boolean readyToBeConsumed;

		/** Whether this result partition has been consumed. */
		private volatile boolean consumed;

		/** When is the result partition ready to be fetched. */
		private volatile long partitionReadyTime;

		/** Timestamp of this information, used to do recycling if the partition's directory has been deleted. */
		private volatile long fileInfoTimestamp;

		ResultPartitionFileInfo(
			String appId,
			Tuple2<String, String> rootDirAndPartitionDir,
			boolean readyToBeConsumed,
			boolean consumed,
			long partitionReadyTime,
			long fileInfoTimestamp) {

			this.appId = appId;
			this.rootDirAndPartitionDir = rootDirAndPartitionDir;
			this.readyToBeConsumed = readyToBeConsumed;
			this.consumed = consumed;
			this.partitionReadyTime = partitionReadyTime;
			this.fileInfoTimestamp = fileInfoTimestamp;
		}

		String getAppId() {
			return appId;
		}

		Tuple2<String, String> getRootDirAndPartitionDir() {
			return rootDirAndPartitionDir;
		}

		boolean isReadyToBeConsumed() {
			return readyToBeConsumed;
		}

		boolean isConsumed() {
			return consumed;
		}

		long getPartitionReadyTime() {
			return partitionReadyTime;
		}

		long getFileInfoTimestamp() {
			return fileInfoTimestamp;
		}

		void setReadyToBeConsumed(long partitionReadyTime) {
			this.partitionReadyTime = partitionReadyTime;
			readyToBeConsumed = true;
		}

		void setConsumed() {
			consumed = true;
		}

		void updateFileInfoTimestamp(long fileInfoTimestamp) {
			if (this.fileInfoTimestamp > 0) {
				this.fileInfoTimestamp = fileInfoTimestamp;
			}
		}

		void markToDelete() {
			updateFileInfoTimestamp(-1L);
		}

		void cancelDeletion() {
			updateFileInfoTimestamp(System.currentTimeMillis());
		}

		boolean needToDelete() {
			if (fileInfoTimestamp > 0) {
				return false;
			} else {
				return true;
			}
		}

		void updateOnConsumption() {
			if (needToDelete()) {
				cancelDeletion();
			}
			if (!isConsumed()) {
				setConsumed();
			}
		}
	}
}
