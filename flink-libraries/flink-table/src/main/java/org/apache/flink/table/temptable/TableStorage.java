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

package org.apache.flink.table.temptable;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.LifeCycleAware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A local file system based storage.
 */
public class TableStorage implements LifeCycleAware {

	private static final Logger logger = LoggerFactory.getLogger(TableStorage.class);

	public static final int DEFAULT_MAX_SEGMENT_LENGTH = 128 * 1024 * 1024;

	private final int maxSegmentLength;

	private final String fileRootPath;

	private Map<String, NavigableMap<Long, File>> partitionSegmentTracker;

	public TableStorage(String fileRootPath) {
		this(fileRootPath, DEFAULT_MAX_SEGMENT_LENGTH);
	}

	public TableStorage(String fileRootPath, int maxSegmentLength) {
		this.fileRootPath = fileRootPath;
		this.maxSegmentLength = maxSegmentLength;
	}

	@Override
	public void open(Configuration parameters) {
		deleteAll(fileRootPath);
		partitionSegmentTracker = new ConcurrentHashMap<>();
	}

	@Override
	public void close() {
		partitionSegmentTracker.clear();
		deleteAll(fileRootPath);
	}

	private void deleteAll(String root) {
		logger.info("delete file under: " + root);
		File file = new File(root);
		if (file.exists()) {
			File[] subFiles = file.listFiles();
			if (subFiles != null) {
				for (File subFile : subFiles) {
					if (subFile.isDirectory()) {
						deleteAll(subFile.getAbsolutePath());
					} else {
						subFile.delete();
					}
				}
			}
			logger.info("delete file:" + root);
			file.delete();
		}
	}

	public List<Integer> getTablePartitions(String tableName) {
		String path = fileRootPath + File.separator + tableName;
		File file = new File(path);
		if (!file.exists()) {
			return Collections.emptyList();
		}

		if (!file.isDirectory()) {
			logger.error(path + " is not a valid table storage path");
			throw new RuntimeException(path + " is not a valid table storage path");
		}
		File[] subFiles = file.listFiles();

		List<Integer> partitions = new ArrayList<>();
		if (subFiles != null) {
			for (File subFile : subFiles) {
				partitions.add(Integer.valueOf(subFile.getName()));
			}
		}

		return partitions;
	}

	/**
	 * create the file if not exists.
	 */
	public void write(String tableName, int partitionId, byte[] content) {
		String baseDirPath = getPartitionDirPath(tableName, partitionId);
		File baseDir = new File(baseDirPath);

		if (!baseDir.exists()) {
			baseDir.mkdirs();
		}

		int offset = 0;

		File lastFile;
		long lastFileOffset;
		NavigableMap<Long, File> offsetMap = partitionSegmentTracker.computeIfAbsent(baseDirPath, d -> new ConcurrentSkipListMap<>());
		Map.Entry<Long, File> lastEntry = offsetMap.lastEntry();
		if (lastEntry == null) {
			lastFile = new File(baseDirPath + File.separator + 0);
			lastFileOffset = 0L;
			offsetMap.put(0L, lastFile);
		} else {
			lastFile = lastEntry.getValue();
			lastFileOffset = lastEntry.getKey();
		}
		while (offset < content.length) {
			int writeBytes = (int) Math.min(maxSegmentLength - lastFile.length(), content.length - offset);
			writeFile(lastFile, content, offset, writeBytes);
			offset += writeBytes;
			if (offset < content.length) {
				lastFileOffset += maxSegmentLength;
				lastFile = new File(baseDirPath + File.separator + lastFileOffset);
				offsetMap.put(lastFileOffset, lastFile);
			}
		}
	}

	private void writeFile(File file, byte[] content, int offset, int len) {
		OutputStream output = null;
		BufferedOutputStream bufferedOutput = null;
		try {
			output = new FileOutputStream(file, true);
			bufferedOutput = new BufferedOutputStream(output);
			bufferedOutput.write(content, offset, len);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException("write file error", e);
		} finally {
			if (bufferedOutput != null) {
				try {
					bufferedOutput.close();
				} catch (Exception e) {}
			}

			if (output != null) {
				try {
					output.close();
				} catch (Exception e) {}
			}
		}
	}

	/**
	 * todo rework on mmap.
	 */
	public int read(String tableName, int partitionId, int offset, int readCount, byte[] buffer) {
		String baseDirPath = getPartitionDirPath(tableName, partitionId);
		if (!partitionSegmentTracker.containsKey(baseDirPath)) {
			logger.error("file: " + baseDirPath + " is not ready for read.");
			throw new RuntimeException("file: " + baseDirPath + " is not ready for read.");
		}

		int totalRead = 0;
		NavigableMap<Long, File> offsetMap = partitionSegmentTracker.get(baseDirPath);
		Map.Entry<Long, File> segmentEntry = offsetMap.floorEntry(Long.valueOf(offset));
		while (segmentEntry != null && totalRead < readCount) {
			Long segmentFileOffset = segmentEntry.getKey();
			File segmentFile = segmentEntry.getValue();
			long readLimit = segmentFileOffset + maxSegmentLength - offset;
			long readBytes = readLimit >= readCount - totalRead ? readCount - totalRead : readLimit;
			int nRead = readFile(segmentFile, offset - segmentFileOffset, readBytes, buffer, totalRead);
			totalRead += nRead;
			offset += nRead;
			segmentEntry = offsetMap.higherEntry(segmentFileOffset);
		}

		return totalRead;
	}

	private int readFile(File file, long fileOffset, long readCount, byte[] buffer, int bufferOffset) {
		FileInputStream input = null;
		BufferedInputStream bufferedInput = null;

		int nRead;

		try {
			input = new FileInputStream(file);
			bufferedInput = new BufferedInputStream(input);
			bufferedInput.skip(fileOffset);
			nRead = bufferedInput.read(buffer, bufferOffset, (int) readCount);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException("read file error", e);
		} finally {
			if (bufferedInput != null) {
				try {
					bufferedInput.close();
				} catch (IOException e) {
				}
			}
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
				}
			}
		}
		return nRead;
	}

	/**
	 * initialize the file to write, replace with an empty file if exists.
	 */
	public void initializePartition(String tableName, int partitionId) {
		String partitionPath = getPartitionDirPath(tableName, partitionId);
		partitionSegmentTracker.remove(partitionPath);
		deleteAll(partitionPath);
		File partition = new File(partitionPath);
		partition.mkdirs();
	}

	@VisibleForTesting
	Map<String, NavigableMap<Long, File>> getPartitionSegmentTracker() {
		return partitionSegmentTracker;
	}

	@VisibleForTesting
	String getPartitionDirPath(String tableName, int partitionId) {
		return fileRootPath + File.separator + tableName + File.separator + partitionId;
	}
}
