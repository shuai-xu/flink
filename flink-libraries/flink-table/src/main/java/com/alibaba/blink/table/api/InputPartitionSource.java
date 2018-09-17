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

package com.alibaba.blink.table.api;

import org.apache.flink.api.common.operators.ResourceSpec;

import java.util.List;

/**
 * This interface should be implemented by all kind of input source that has multiple partitions.
 *
 * <p></p>This is used by auto-config to set a suitable parallelism for source operator.
 */
public interface InputPartitionSource {

	/**
	 * Get the partition name list of the source.
	 *
	 * @return partition list
	 * @throws Exception Get partition list failed
	 */
	List<String> getPartitionList() throws Exception;

	/**
	 * Estimate resource (e.g. heap memory, native memory) of the single source partition.
	 *
	 * @return ResourceSpec Used resource of the partition
	 */
	default ResourceSpec estimateSinglePartitionResource() {
		return ResourceSpec.DEFAULT;
	}
}
