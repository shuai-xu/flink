/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.gemini;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.gemini.fullheap.FullHeapStateStore;
import org.apache.flink.util.Preconditions;

/**
 * Creates {@link StateStore}, {@link SnapshotOperator} and {@link RestoreOperator}
 * according to the configuration of backend.
 */
public class GeminiAgent {

	/**
	 * The backend this agent belongs to.
	 */
	private final GeminiInternalStateBackend stateBackend;

	/**
	 * The type of snapshot to take.
	 */
	private final GeminiConfiguration.SnapshotType snapshotType;

	/**
	 * The type of memory to use.
	 */
	private final GeminiConfiguration.MemoryType memoryType;

	public GeminiAgent(
		GeminiInternalStateBackend stateBackend,
		GeminiConfiguration configuration
	) {
		Preconditions.checkNotNull(stateBackend);
		Preconditions.checkNotNull(configuration);

		this.stateBackend = stateBackend;
		this.snapshotType = configuration.getSnapshotType();
		this.memoryType = configuration.getMemoryType();
	}

	/**
	 * Creates a {@link StateStore} according to the configuration.
	 *
	 * @param descriptor The descriptor of the state this store belongs to.
	 * @return A {@link StateStore}.
	 */
	public StateStore createStateStore(
		InternalStateDescriptor descriptor
	) {
		if (snapshotType == GeminiConfiguration.SnapshotType.FULL &&
			memoryType == GeminiConfiguration.MemoryType.HEAP) {
			return new FullHeapStateStore(stateBackend, descriptor);
		} else {
			// Currently we don't support incremental snapshot or off-heap, but will do later.
			throw new RuntimeException("Unsupported StateStore type");
		}
	}

	/**
	 * Creates a {@link SnapshotOperator} according to the configuration.
	 *
	 * @param checkpointId  The ID of the checkpoint.
	 * @param timestamp     The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @param cancelStreamRegistry Registry for all opened streams, so they can be closed
	 *                             if the task using this backend is closed.
	 * @return A {@link SnapshotOperator}.
	 */
	public SnapshotOperator createSnapshotOperator(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions,
		CloseableRegistry cancelStreamRegistry
	) {
		// Currently only full snapshot is supported.
		if (snapshotType != GeminiConfiguration.SnapshotType.FULL) {
			throw new RuntimeException("Only full snapshot supported");
		}

		return new FullSnapshotOperator(
			stateBackend,
			checkpointId,
			streamFactory,
			cancelStreamRegistry);
	}

	/**
	 * Creates a {@link RestoreOperator} according to the configuration.
	 *
	 * @param cancelStreamRegistry Registry for all opened streams, so they can be closed
	 *                             if the task using this backend is closed.
	 * @return A {@link RestoreOperator}.
	 */
	public RestoreOperator createRestoreOperator(
		CloseableRegistry cancelStreamRegistry
	) {
		// Currently only full restore is supported.
		if (snapshotType != GeminiConfiguration.SnapshotType.FULL) {
			throw new RuntimeException("Only full restore supported");
		}

		return new FullRestoreOperator(stateBackend, cancelStreamRegistry);
	}
}
