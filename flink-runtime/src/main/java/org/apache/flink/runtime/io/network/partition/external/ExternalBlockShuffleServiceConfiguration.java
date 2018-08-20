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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.netty.NettyConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Configuration for external block shuffle service such as disk configuration, memory configuration,
 * netty configuration and etc.
 */
public class ExternalBlockShuffleServiceConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockShuffleServiceConfiguration.class);

	private static final String DEFAULT_DISK_TYPE = "default";

	private static final Pattern DISK_TYPE_REGEX = Pattern.compile("^(\\[(\\w*)\\])?(.+)$");

	/** Flink configurations. */
	private final Configuration configuration;

	/** Directory to disk type. */
	private final Map<String, String> dirToDiskType;

	/** Disk type to IO thread number. */
	private final Map<String, Integer> diskTypeToIOThreadNum;

	/** The number of buffers used to transfer partition data. */
	private Integer bufferNumber;

	/** The size of a buffer used to transfer partition data, in bytes. */
	private Integer memorySizePerBufferInBytes;

	private ExternalBlockShuffleServiceConfiguration(
		Configuration configuration,
		Map<String, String> dirToDiskType,
		Map<String, Integer> diskTypeToIOThreadNum,
		Integer bufferNumber,
		Integer memorySizePerBufferInBytes) {

		this.configuration = configuration;
		this.dirToDiskType = dirToDiskType;
		this.diskTypeToIOThreadNum = diskTypeToIOThreadNum;
		this.bufferNumber = bufferNumber;
		this.memorySizePerBufferInBytes = memorySizePerBufferInBytes;
	}

	Configuration getConfiguration() {
		return configuration;
	}

	Map<String, String> getDirToDiskType() {
		return Collections.unmodifiableMap(dirToDiskType);
	}

	Map<String, Integer> getDiskTypeToIOThreadNum() {
		return Collections.unmodifiableMap(diskTypeToIOThreadNum);
	}

	Integer getTotalIOThreadNum() {
		return dirToDiskType.entrySet().stream().mapToInt(entry -> diskTypeToIOThreadNum.get(entry.getValue())).sum();
	}

	Integer getBufferNumber() {
		return bufferNumber;
	}

	Integer getMemorySizePerBufferInBytes() {
		return memorySizePerBufferInBytes;
	}

	NettyConfig createNettyConfig() {
		final Integer port = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY);
		checkArgument(port != null && port > 0 && port < 65536,
			"Invalid port number for ExternalBlockShuffleService: " + port);

		final InetSocketAddress shuffleServiceInetSocketAddress = new InetSocketAddress(port);
		final int memorySizePerBufferInBytes = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES);

		return new NettyConfig(
			shuffleServiceInetSocketAddress.getAddress(),
			shuffleServiceInetSocketAddress.getPort(),
			memorySizePerBufferInBytes, 1, configuration);
	}

	static ExternalBlockShuffleServiceConfiguration fromConfiguration(
		Configuration configuration) throws Exception {

		// 1. Parse and validate disk configurations.
		Map<String, String> dirToDiskType = parseDirToDiskType(configuration);
		Map<String, Integer> diskTypeToIOThreadNum = parseDiskTypeToIOThreadNum(configuration);
		validateDiskTypeConfiguration(dirToDiskType, diskTypeToIOThreadNum);

		final int diskIOThreadNum = dirToDiskType.entrySet().stream()
			.mapToInt(entry -> diskTypeToIOThreadNum.get(entry.getValue())).sum();
		checkArgument(diskIOThreadNum > 0,
			"DiskIOThreadNum should be greater than 0, actual value: " + diskIOThreadNum);

		// 2. Auto-configure netty thread number based on disk IO thread number if it hasn't been configured.
		int nettyThreadNum = configuration.getInteger(ExternalBlockShuffleServiceOptions.SERVER_THREAD_NUM);
		if (nettyThreadNum <= 0) {
			nettyThreadNum = diskIOThreadNum;
			configuration.setInteger(NettyConfig.NUM_THREADS_SERVER.key(), nettyThreadNum);
		}

		// 3. Configure and validate direct memory settings.
		// Direct memory used in shuffle service consists of two parts:
		// 		(1) memory for buffers
		// 		(2) memory for arenas in NettyServer
		final long chunkSizeInBytes = NettyConfig.getChunkSize();
		final long directMemoryLimitInBytes = ((long) configuration.getInteger(
			ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB)) << 20;

		// 3.1 Check whether direct memory is enough for buffers.
		final int memorySizePerBufferInBytes = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.MEMORY_SIZE_PER_BUFFER_IN_BYTES);
		final int minBufferNum = configuration.getInteger(ExternalBlockShuffleServiceOptions.MIN_BUFFER_NUMBER);
		// Make sure that each disk IO thread has at least 2 buffers.
		final int bufferNum = Math.max(diskIOThreadNum * 2, minBufferNum);
		// Reserve one chunk for Netty arenas, since there should be at least one arena.
		long leftDirectMemoryInBytes = directMemoryLimitInBytes - chunkSizeInBytes
			- (long) memorySizePerBufferInBytes * bufferNum;
		checkArgument(leftDirectMemoryInBytes >= 0,
			"Direct memory configured is not enough, total direct memory: "
				+ (directMemoryLimitInBytes >> 20) + " MB, arenaChunkSize: " + chunkSizeInBytes
				+ " Bytes, segmentSize: " + memorySizePerBufferInBytes + " Bytes, minBufferNum: " + minBufferNum
				+ ", bufferNum: " + bufferNum);

		// 3.2 Auto-configure arenasNum base on the remaining memory.
		// We don't use up all the memory in case of OOM. Notice that we have reserved a chunk for Netty arenas before.
		int arenasNum = Math.min((int) (0.8 * leftDirectMemoryInBytes / chunkSizeInBytes) + 1, nettyThreadNum);
		configuration.setInteger(NettyConfig.NUM_ARENAS.key(), arenasNum);
		LOG.info("Auto-configure " + NettyConfig.NUM_ARENAS.key() + " to " + arenasNum);

		return new ExternalBlockShuffleServiceConfiguration(
			configuration,
			dirToDiskType,
			diskTypeToIOThreadNum,
			bufferNum,
			memorySizePerBufferInBytes);
	}

	// ------------------------------ internal methods -------------------------------
	private static Map<String, Integer> parseDiskTypeToIOThreadNum(Configuration configuration) {
		Map<String, Integer> diskTypeToIOThread = new HashMap<>();

		// Set default disk type configuration.
		Integer defaultIOThreadNum = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.DEFAULT_IO_THREAD_NUM_PER_DISK);
		diskTypeToIOThread.put(DEFAULT_DISK_TYPE, defaultIOThreadNum);

		// Parse disk type configuration.
		String strConfig = configuration.getString(
			ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE);
		String[] diskConfigList = strConfig.split(",");
		if (diskConfigList != null && diskConfigList.length > 0) {
			for (String strDiskConfig : diskConfigList) {
				if (strDiskConfig != null && !strDiskConfig.isEmpty()) {
					String[] kv = strDiskConfig.split(":");
					if (kv != null && kv.length == 2) {
						diskTypeToIOThread.put(kv[0].trim(), Integer.valueOf(kv[1].trim()));
					}
				}
			}
		}
		return diskTypeToIOThread;
	}

	private static Map<String, String> parseDirToDiskType(Configuration configuration) {
		Map<String, String> dirToDiskType = new HashMap<>();

		String strConfig = configuration.getString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS);
		String[] dirConfigList = strConfig.split(",");
		if (dirConfigList != null && dirConfigList.length > 0) {
			for (String strDirConfig : dirConfigList) {
				if (strDirConfig != null && !strDirConfig.isEmpty()) {
					Matcher matcher = DISK_TYPE_REGEX.matcher(strDirConfig);
					if (matcher.matches()) {
						String diskType = matcher.group(2);
						String dir = matcher.group(3);
						if (dir != null && !dir.isEmpty()) {
							dirToDiskType.put(dir.trim(),
								(diskType != null && !diskType.isEmpty()) ? diskType.trim() : DEFAULT_DISK_TYPE);
						}
					}
				}
			}
		}
		return dirToDiskType;
	}

	/** Make sure that each directory has its corresponding IO thread configuration. */
	private static void validateDiskTypeConfiguration(
		Map<String, String> dirToDiskType, Map<String, Integer> diskTypeToIOThreadNum) throws Exception {

		Set<String> diskTypes = diskTypeToIOThreadNum.keySet();
		boolean success = dirToDiskType.entrySet().stream().noneMatch(dirEntry -> {
			boolean ifContains = diskTypes.contains(dirEntry.getValue());
			if (!ifContains) {
				LOG.error("Invalid configuration: Require IO thread num for dir [{0}] with disk type [{1}].",
					dirEntry.getKey(), dirEntry.getValue());
			}
			return !ifContains;
		});

		checkArgument(success, "Invalid disk configuration for ExternalBlockShuffleService, "
			+ ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE.key() + " : " + diskTypeToIOThreadNum
			+ ", " + ExternalBlockShuffleServiceOptions.LOCAL_DIRS + " : " + dirToDiskType);
	}
}
