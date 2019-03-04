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

import java.util.List;

/**
 * The interface of TableService API.
 */
public interface TableService {

	/**
	 * Return a list of all the partition ids of the given table.
	 *
	 * @param tableName the name of the table.
	 * @return partition id in a list.
	 * @throws Exception
	 */
	List<Integer> getPartitions(String tableName) throws Exception;

	/**
	 * Append some bytes to one partition of a table.
	 * @param tableName the name of the table.
	 * @param partitionId partition id of the table.
	 * @param content bytes to append
	 * @return The number of bytes that have been successfully written.
	 * @throws Exception
	 */
	int write(String tableName, int partitionId, byte[] content) throws Exception;

	/**
	 * Delete one partition of a table.
	 * @param tableName the name of the table.
	 * @param partitionId partition id of the table.
	 * @throws Exception
	 */
	void delete(String tableName, int partitionId) throws Exception;

	/**
	 * initialize a table with specified partition, this request will delete the data if the given table name and partition id.
	 * @param tableName the name of the table.
	 * @param partitionId partition id of the table.
	 * @throws Exception
	 */
	void initializePartition(String tableName, int partitionId) throws Exception;

	/**
	 * register a table with specified partition.
	 * @param tableName the name of the table.
	 * @param partitionId partition id of the table.
	 * @throws Exception
	 */
	void registerPartition(String tableName, int partitionId) throws Exception;

	/**
	 * unregister all partitions of a table.
	 * @param tableName the name of the table.
	 * @throws Exception
	 */
	void unregisterPartition(String tableName) throws Exception;

	/**
	 * finish writing partition of table, this will generate finish file and index file.
	 * @param tableName the name of the table.
	 * @param partitionId partition id of the table.
	 * @throws Exception
	 */
	void finishPartition(String tableName, int partitionId) throws Exception;
}
