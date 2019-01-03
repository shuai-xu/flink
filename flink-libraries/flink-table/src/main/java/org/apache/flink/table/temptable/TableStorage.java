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

/**
 * A local file system based storage.
 */
public class TableStorage implements LifeCycleAware {

	private static final Logger logger = LoggerFactory.getLogger(TableStorage.class);

	private final String fileRootPath;

	public TableStorage(String fileRootPath) {
		this.fileRootPath = fileRootPath;
	}

	@Override
	public void open(Configuration parameters) {
		deleteAll(fileRootPath);
	}

	@Override
	public void close() {
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

		String dirPath = fileRootPath + File.separator
			+ tableName;
		File dir = new File(dirPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}

		String filePath = dirPath + File.separator + partitionId;
		File file = new File(filePath);
		OutputStream output = null;
		BufferedOutputStream bufferedOutput = null;

		try {
			output = new FileOutputStream(file, true);
			bufferedOutput = new BufferedOutputStream(output);
			bufferedOutput.write(content);
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

		String dirPath = fileRootPath + File.separator
			+ tableName;
		String filePath = dirPath + File.separator + partitionId;
		File file = new File(filePath);
		if (!file.exists()) {
			logger.error("file: " + filePath + " is not ready for read.");
			throw new RuntimeException("file: " + filePath + " is not ready for read.");
		}

		FileInputStream input = null;
		BufferedInputStream bufferedInput = null;

		int nRead;

		try {
			input = new FileInputStream(file);
			bufferedInput = new BufferedInputStream(input);
			bufferedInput.skip(offset);
			nRead = bufferedInput.read(buffer, 0, readCount);
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
	public void initializePartition(String tableName, int partitionId) throws IOException {
		String dirPath = fileRootPath + File.separator
			+ tableName;
		String filePath = dirPath + File.separator + partitionId;
		File file = new File(filePath);
		if (file.exists()) {
			file.delete();
		}
		File dir = new File(dirPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		file.createNewFile();
	}
}
