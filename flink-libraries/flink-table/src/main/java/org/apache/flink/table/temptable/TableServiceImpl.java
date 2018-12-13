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

import java.util.List;

/**
 * Handle requests of {@link TableService}.
 */
public class TableServiceImpl implements LifeCycleAware, TableService {

	private TableStorage tableStorage;

	private String rootPath;

	public static final String TABLESERVICE_STORAGE_PATH = "flink.service.tableservice.storage.rootpath";

	public static final String TABLESERVICE_DEFAULT_STORAGE_PATH_VALUE = "/tmp/table_service";

	private static final Logger logger = LoggerFactory.getLogger(TableServiceImpl.class);

	@Override
	public void open(Configuration parameters) {
		logger.info("FlinkTableService begin open.");
		rootPath = parameters.getString(TABLESERVICE_STORAGE_PATH, TABLESERVICE_DEFAULT_STORAGE_PATH_VALUE);
		tableStorage = new TableStorage(rootPath);
		tableStorage.open(parameters);
		logger.info("FlinkTableService end open.");
	}

	@Override
	public void close() {
		logger.info("FlinkTableService begin close.");
		if (tableStorage != null) {
			tableStorage.close();
		}
		logger.info("FlinkTableService end close.");
	}

	@Override
	public List<Integer> getPartitions(String tableName) {
		logger.debug("FlinkTableService receive getPartitionCount request");
		return tableStorage.getTablePartitions(tableName);
	}

	@Override
	public int write(String tableName, int partitionId, byte[] content) {
		try {
			logger.debug("FlinkTableService receive write request");
			if (content != null) {
				tableStorage.write(tableName, partitionId, content);
			}
			return content == null ? 0 : content.length;
		} catch (Exception e) {
			logger.debug("FlinkTableService receive write request, but error occurs: " + e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public byte[] read(String tableName, int partitionId, int offset, int readCount) {
		logger.debug("FlinkTableService receive read request");
		byte [] buffer = new byte[readCount];

		int nRead = tableStorage.read(tableName, partitionId, offset, readCount, buffer);

		if (nRead <= 0) {
			return new byte[0];
		} else {
			byte[] result = new byte[nRead];
			System.arraycopy(buffer, 0, result, 0, nRead);
			return result;
		}
	}
}
