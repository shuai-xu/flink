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
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.service.LifeCycleAware;
import org.apache.flink.table.runtime.functions.aggfunctions.cardinality.MurmurHash;
import org.apache.flink.table.temptable.util.BytesUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A local file system based storage.
 */
public class TableStorage implements LifeCycleAware {

	private static final Logger logger = LoggerFactory.getLogger(TableStorage.class);

	private long maxSegmentSize;

	private String storagePath;

	private Map<String, NavigableMap<Long, File>> partitionSegmentTracker;

	@Override
	public void open(Configuration config) {
		String tableServiceId = config.getString(TableServiceOptions.TABLE_SERVICE_ID, UUID.randomUUID().toString());
		String rootPath = config.getString(TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH, System.getProperty("user.dir"));
		storagePath = rootPath + File.separator + "tableservice_" + tableServiceId;
		// todo max segment size is set to Long.MAX_VALUE for the moment, so there will be only one file for one table partition.
		maxSegmentSize = Long.MAX_VALUE;
		deleteAll(storagePath);
		createDirs(storagePath);
		partitionSegmentTracker = new ConcurrentHashMap<>();
		logger.info("TableStorage opened with storage path: " + storagePath);
	}

	@Override
	public void close() {
		partitionSegmentTracker.clear();
		deleteAll(storagePath);
	}

	private void deleteAll(String root) {
		synchronized (TableStorage.class) {
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
	}

	private void createDirs(String root) {
		synchronized (TableStorage.class) {
			File dir = new File(root);
			if (!dir.exists()) {
				dir.mkdirs();
			}
		}
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
			lastFile = new File(baseDirPath + File.separator + 0 + ".data");
			lastFileOffset = 0L;
			offsetMap.put(0L, lastFile);
		} else {
			lastFile = lastEntry.getValue();
			lastFileOffset = lastEntry.getKey();
		}
		while (offset < content.length) {
			int writeBytes = (int) Math.min(maxSegmentSize - lastFile.length(), content.length - offset);
			writeFile(lastFile, content, offset, writeBytes);
			offset += writeBytes;
			if (offset < content.length) {
				lastFileOffset += maxSegmentSize;
				lastFile = new File(baseDirPath + File.separator + lastFileOffset);
				offsetMap.put(lastFileOffset, lastFile);
			}
		}
	}

	public void delete(String tableName, int partitionId) {
		logger.debug("delete table, tableName: " + tableName + ", partitionId: " + partitionId);
		String baseDirPath = getPartitionDirPath(tableName, partitionId);
		deleteAll(baseDirPath);
	}

	public void finishPartition(String tableName, int partitionId) {
		String baseDirPath = getPartitionDirPath(tableName, partitionId);

		// add finish file
		String finishFilePath = baseDirPath + File.separator + "finished";
		byte[] finishData = generateFinishData();

		File finishFile = new File(finishFilePath);
		if (finishFile.exists()) {
			finishFile.delete();
		}
		try {
			finishFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeFile(finishFile, finishData, 0, finishData.length);

		// add index file
		// todo mock, there may be more files
		String indexFilePath = baseDirPath + File.separator + "0.index";
		byte[] indexData = generateIndexData(baseDirPath);

		File indexFile = new File(indexFilePath);
		if (indexFile.exists()) {
			indexFile.delete();
		}
		try {
			indexFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeFile(indexFile, indexData, 0, indexData.length);
	}

	private byte[] generateIndexData(String baseDirPath) {
		String dataFilePath = baseDirPath + File.separator + "0.data";
		long fileLength = new File(dataFilePath).length();
		byte[] indexData = new byte[Integer.BYTES * 2 + Long.BYTES * 2];
		byte[] size = BytesUtil.intToBytes(1);
		byte[] partitionId = BytesUtil.intToBytes(0);
		byte[] startOffset = BytesUtil.longToBytes(0L);
		byte[] length = BytesUtil.longToBytes(fileLength);
		int offset = 0;
		System.arraycopy(size, 0, indexData, offset, Integer.BYTES);
		offset += Integer.BYTES;
		System.arraycopy(partitionId, 0, indexData, offset, Integer.BYTES);
		offset += Integer.BYTES;
		System.arraycopy(startOffset, 0, indexData, offset, Long.BYTES);
		offset += Long.BYTES;
		System.arraycopy(length, 0, indexData, offset, Long.BYTES);
		offset += Long.BYTES;
		return indexData;
	}

	private byte[] generateFinishData() {
		byte[] fileTypeBytes = null;
		try {
			fileTypeBytes = PersistentFileType.HASH_PARTITION_FILE.toString().getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		int totalLength = Integer.BYTES + Integer.BYTES + fileTypeBytes.length + Integer.BYTES + Integer.BYTES;
		int offset = 0;
		byte[] data = new byte[totalLength];
		byte[] versionBytes = BytesUtil.intToBytes(1);
		byte[] fileTypeLengthBytes = BytesUtil.intToBytes(fileTypeBytes.length);
		byte[] spillCountBytes = BytesUtil.intToBytes(1);
		byte[] subpartitionNumBytes = BytesUtil.intToBytes(1);
		System.arraycopy(versionBytes, 0, data, offset, Integer.BYTES);
		offset += Integer.BYTES;
		System.arraycopy(fileTypeLengthBytes, 0, data, offset, Integer.BYTES);
		offset += Integer.BYTES;
		System.arraycopy(fileTypeBytes, 0, data, offset, fileTypeBytes.length);
		offset += fileTypeBytes.length;
		System.arraycopy(spillCountBytes, 0, data, offset, Integer.BYTES);
		offset += Integer.BYTES;
		System.arraycopy(subpartitionNumBytes, 0, data, offset, Integer.BYTES);
		offset += Integer.BYTES;
		return data;
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

	@VisibleForTesting
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
			long readLimit = segmentFileOffset + maxSegmentSize - offset;
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
		long tableHashId = MurmurHash.hash(tableName) & Integer.MAX_VALUE;
		return storagePath + File.separator + tableHashId + File.separator + partitionId;
	}
}
