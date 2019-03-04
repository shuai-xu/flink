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
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.service.LifeCycleAware;
import org.apache.flink.service.ServiceContext;
import org.apache.flink.table.temptable.util.TableServiceUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Handle requests of {@link TableService}.
 */
public class TableServiceImpl implements LifeCycleAware, TableService {

	private TableStorage tableStorage;

	private static final Logger logger = LoggerFactory.getLogger(TableServiceImpl.class);

	private ServiceContext serviceContext;

	private TableServiceMetrics tableServiceMetrics;

	private TableMetaManager tableMetaManager;

	private boolean isLeader;

	public TableServiceImpl(ServiceContext serviceContext) {
		this.serviceContext = serviceContext;
	}

	@Override
	public void open(Configuration config) {
		logger.info("FlinkTableService begin open.");
		tableStorage = new TableStorage();
		tableStorage.open(config);
		tableServiceMetrics = new TableServiceMetrics(serviceContext.getMetricGroup());
		isLeader = serviceContext.getIndexOfCurrentInstance() == 0;
		if (isLeader) {
			tableMetaManager = new TableMetaManager();
		}
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
		List<ResultPartitionID> list = tableMetaManager.getResultPartitions(tableName);
		List<Integer> result = new ArrayList<>();
		for (ResultPartitionID partitionID : list) {
			result.add((int) partitionID.getPartitionId().getUpperPart());
		}
		return result;
	}

	@Override
	public int write(String tableName, int partitionId, byte[] content) {
		try {
			logger.debug("FlinkTableService receive write request");
			if (content != null) {
				tableStorage.write(tableName, partitionId, content);
			}
			int writeLength =  content == null ? 0 : content.length;
			tableServiceMetrics.getWriteTotalBytesMetrics().inc(writeLength);
			return writeLength;
		} catch (Exception e) {
			logger.debug("FlinkTableService receive write request, but error occurs: " + e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(String tableName, int partitionId) throws Exception {
		try {
			logger.debug("FlinkTableService receive delete request");
			tableStorage.delete(tableName, partitionId);
		} catch (Exception e) {
			logger.debug("FlinkTableService receive delete request, but error occurs: " + e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void initializePartition(String tableName, int partitionId) throws Exception {
		logger.debug("FlinkTableService receive initial partition request");
		tableStorage.initializePartition(tableName, partitionId);
	}

	@Override
	public void registerPartition(String tableName, int partitionId) throws Exception {
		logger.debug("FlinkTableService receive register partition request");
		Preconditions.checkState(
			isLeader,
			"Only leader can handle register partition request, this should not happen");
		ResultPartitionID resultPartitionID = TableServiceUtil.tablePartitionToResultPartition(tableName, partitionId);
		tableMetaManager.addResultPartition(tableName, resultPartitionID);
	}

	@Override
	public void unregisterPartition(String tableName) throws Exception {
		logger.debug("FlinkTableService receive remove partitions request");
		tableMetaManager.removeTablePartitions(tableName);
	}

	@Override
	public void finishPartition(String tableName, int partitionId) throws Exception {
		logger.debug("FlinkTableService receive finish partition request");
		tableStorage.finishPartition(tableName, partitionId);
	}
}
