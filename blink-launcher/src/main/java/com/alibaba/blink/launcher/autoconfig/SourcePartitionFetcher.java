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

package com.alibaba.blink.launcher.autoconfig;

import org.apache.flink.table.api.InputPartitionSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 并行获取所有source的partitions.
 *
 * <p>使用方式：先逐个添加source，再获取所有的source partition.
 *
 * <p>Note: SourcePartitionFetcher needs to be reset every time in testing to avoid cached results
 */
public class SourcePartitionFetcher {
	private static final Logger LOG = LoggerFactory.getLogger(SourcePartitionFetcher.class);
	private static final int DEFAULT_GET_SOURCE_PARTITION_TIMETOUT_MS = 30000;

	private static final SourcePartitionFetcher INSTANCE = new SourcePartitionFetcher();

	private SourcePartitionFetcher() {
	}

	public static SourcePartitionFetcher getInstance() {
		return INSTANCE;
	}

	private Map<Integer, Integer> sourcePartitions = null;
	private List<String> errorMessage = Collections.synchronizedList(new ArrayList<>());
	private Map<Integer, CompletableFuture<Integer>> futureMap = new ConcurrentHashMap<>();

	public synchronized void addSourceFunction(int id, String name, InputPartitionSource sourceFunction) {
		CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
		new Thread(
			() -> {
				int parallelism = 0;
				List<String> partitions = null;
				LOG.info("Getting source partition of {}:{}", id, name);
				try {
					partitions = sourceFunction.getPartitionList();
				} catch (Throwable e) {
					String message = String.format("Failed to get partitions of %s:%s. Max parallelism of" +
						" this source may not be set correctly. Caused by: %s", id, name, e.getMessage());
					LOG.warn(message);
					errorMessage.add(message);
				}
				if (partitions != null && partitions.size() > 0) {
					parallelism = partitions.size();
				}
				completableFuture.complete(parallelism);
			}
		).start();
		futureMap.put(id, completableFuture);
	}

	/**
	 * Returns cached results if possible, to avoid extra network time.
	 */
	public synchronized Map<Integer, Integer> getSourcePartitions() {
		if (futureMap == null) {
			throw new IllegalArgumentException("please call addSourceFunction first!");
		}

		if (sourcePartitions != null) {
			LOG.debug("Use previous source partition data");
			return sourcePartitions;
		} else {
			sourcePartitions = new ConcurrentHashMap<>();
			futureMap.forEach((Integer id, CompletableFuture<Integer> future) -> {
				try {
					Integer result = future.get(DEFAULT_GET_SOURCE_PARTITION_TIMETOUT_MS, TimeUnit.MILLISECONDS);
					sourcePartitions.put(id, result);
					LOG.info("Source {} has {} partitions", id, result);
				} catch (Throwable e) {
					LOG.warn("Failed to get result of source partition: {}. Exception: {}", id, e.getCause());
				} finally {
					future.cancel(true);
				}
			});
		}

		LOG.info("Successfully get source partition");
		return sourcePartitions;
	}

	List<String> getErrorMessage() {
		return errorMessage;
	}

	public void reset() {
		if (this.sourcePartitions != null) {
			this.sourcePartitions.clear();
			this.sourcePartitions = null;
		}
		this.errorMessage.clear();
		this.futureMap.clear();
	}
}
