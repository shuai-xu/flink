/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.TaskStateManager;

/**
 * Stream environment that allows to wait for checkpoint acknowledgement.
 */
public class AcknowledgeStreamMockEnvironment extends StreamMockEnvironment {
	private volatile long checkpointId;
	private volatile TaskStateSnapshot checkpointStateHandles;

	private final OneShotLatch startLatch = new OneShotLatch();

	private final OneShotLatch completeLatch = new OneShotLatch();

	public AcknowledgeStreamMockEnvironment(
		Configuration jobConfig,
		Configuration taskConfig,
		ExecutionConfig executionConfig,
		long memorySize,
		MockInputSplitProvider inputSplitProvider,
		int bufferSize,
		TaskStateManager taskStateManager) {
		super(
			jobConfig,
			taskConfig,
			executionConfig,
			memorySize,
			inputSplitProvider,
			bufferSize,
			taskStateManager);
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	@Override
	public void acknowledgeCheckpoint(
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot checkpointStateHandles) {

		try {
			this.startLatch.await();
		} catch (Exception e) {
			throw new RuntimeException("Could not wait for the start of checkpoint.", e);
		}

		this.checkpointId = checkpointId;
		this.checkpointStateHandles = checkpointStateHandles;
		this.completeLatch.trigger();
	}

	public OneShotLatch getStartLatch() {
		return startLatch;
	}

	public OneShotLatch getCompleteLatch() {
		return completeLatch;
	}

	public TaskStateSnapshot getCheckpointStateHandles() {
		return checkpointStateHandles;
	}
}
