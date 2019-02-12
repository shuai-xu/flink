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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.table.sources.Partition;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.Serializable;
import java.util.Map;

/**
 * A class that describes a partition of a Hive table.
 * Please note that the class is serializable because all its member variables are serializable.
 */
public class HiveTablePartition implements Partition, Serializable {
	/** Partition storage descriptor. */
	private StorageDescriptor storageDescriptor;

	/** The map of partition key names and their values. */
	private Map<String, Object> partitionValues;

	public HiveTablePartition(StorageDescriptor storageDescriptor, Map<String, Object> partitionValues) {
		this.storageDescriptor = storageDescriptor;
		this.partitionValues = partitionValues;
	}

	public StorageDescriptor getStorageDescriptor() {
		return storageDescriptor;
	}

	public Map<String, Object> getPartitionValues() {
		return partitionValues;
	}

	@Override
	public Object getFieldValue(String fieldName) {
		if (null != partitionValues) {
			return partitionValues.get(fieldName);
		} else {
			return null;
		}
	}

	@Override
	public Object getOriginValue() {
		//todo: seems useless?
		return null;
	}

}
