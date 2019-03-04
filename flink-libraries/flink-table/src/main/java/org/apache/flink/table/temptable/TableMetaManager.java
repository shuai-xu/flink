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

import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This Manager will handle all table partition meta information.
 */
public class TableMetaManager {

	@GuardedBy("this")
	private Map<String, List<ResultPartitionID>> tableToResultPartitions = new HashMap<>();

	// todo This field will be used in the future.
	@GuardedBy("this")
	private Map<ResultPartitionID, ResultPartitionLocation> resultPartitionMetaMap = new HashMap<>();

	public synchronized void addResultPartition(String tableName, ResultPartitionID resultPartitionID) {
		List<ResultPartitionID> resultPartitionIDList =
			tableToResultPartitions.computeIfAbsent(tableName, key -> new ArrayList<>());
		resultPartitionIDList.add(resultPartitionID);
	}

	public synchronized void removeTablePartitions(String tableName) {
		tableToResultPartitions.remove(tableName);
	}

	public synchronized List<ResultPartitionID> getResultPartitions(String tableName) {
		return tableToResultPartitions.getOrDefault(tableName, Collections.emptyList());
	}

	public synchronized void addResultPartitionMeta(
		ResultPartitionID resultPartitionID, ResultPartitionLocation resultPartitionLocation) {
		resultPartitionMetaMap.put(resultPartitionID, resultPartitionLocation);
	}

	public synchronized ResultPartitionLocation getResultPartitionMeta(ResultPartitionID resultPartitionID) {
		return resultPartitionMetaMap.get(resultPartitionID);
	}

}
