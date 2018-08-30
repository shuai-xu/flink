/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskmanager.TaskActions;

import java.io.IOException;

/**
 * ExternalResultPartition is used when shuffling data through external shuffle service,
 * e.g. yarn shuffle service.
 */
public class ExternalResultPartition<T extends IOReadableWritable> extends ResultPartition<T> {

	public ExternalResultPartition(
		Configuration taskManagerConfiguration,
		String owningTaskName,
		TaskActions taskActions,
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		int numberOfSubpartitions,
		int numTargetKeyGroups,
		ResultPartitionConsumableNotifier partitionConsumableNotifier,
		MemoryManager memoryManager,
		IOManager ioManager,
		boolean sendScheduleOrUpdateConsumersMessage) {

		super(owningTaskName, jobId, partitionId, partitionType, numberOfSubpartitions, numTargetKeyGroups);
	}

	@Override
	public void addRecord(T record, int[] targetChannels, boolean flushAlways) throws IOException, InterruptedException {

	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean flushAlways) throws IOException {
		throw new RuntimeException("Not support writeEvent.");
	}

	@Override
	public void clearBuffers() {

	}

	@Override
	public void flushAll() {

	}

	@Override
	public void flush(int subpartitionIndex) {

	}

	@Override
	protected void releaseInternal() {

	}

	@Override
	public void finish() throws IOException {

	}
}
