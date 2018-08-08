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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * Implementation of {@link AbstractInternalStateBackend} which stores the key-value
 * pairs of internal states in memory and takes snapshots asynchronously.
 */
public class GeminiInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(GeminiInternalStateBackend.class);

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * Configuration for this state backend.
	 */
	private transient GeminiConfiguration geminiConfiguration;

	/**
	 * Agent for this state backend.
	 */
	private transient GeminiAgent geminiAgent;

	/**
	 * Maps state names to their {@link StateStore}s.
	 */
	private transient Map<String, StateStore> stateStoreMap;

	public GeminiInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		Configuration configuration) {
		super(numberOfGroups, groups, userClassLoader);

		Preconditions.checkNotNull(configuration);
		Preconditions.checkNotNull(localRecoveryConfig);

		this.localRecoveryConfig = localRecoveryConfig;
		this.stateStoreMap = new HashMap<>(16);
		this.geminiConfiguration = new GeminiConfiguration(configuration);
		this.geminiAgent = new GeminiAgent(this, geminiConfiguration);

		LOG.info("GeminiStateBackend is created with configuration: " + configuration);
	}

	public LocalRecoveryConfig getLocalRecoveryConfig() {
		return localRecoveryConfig;
	}

	@Override
	public void closeImpl() {
		stateStoreMap.clear();
	}

	@Override
	protected InternalState createInternalState(InternalStateDescriptor descriptor) {
		Preconditions.checkNotNull(descriptor);

		String stateName = descriptor.getName();

		Preconditions.checkState(!stateStoreMap.containsKey(stateName),
			"State store for " + stateName + " has already been existed.");

		StateStore stateStore = geminiAgent.createStateStore(descriptor);
		stateStoreMap.put(stateName, stateStore);

		return new GeminiInternalState(this, descriptor, stateStore, isCopyValue());
	}

	public Map<String, StateStore> getStateStoreMap() {
		return stateStoreMap;
	}

	@Override
	public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions
	) throws Exception {

		if (states.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		final SnapshotOperator snapshotOperator = geminiAgent.createSnapshotOperator(
			checkpointId,
			timestamp,
			streamFactory,
			checkpointOptions,
			cancelStreamRegistry);

		long syncStartTime = System.currentTimeMillis();

		snapshotOperator.takeSnapshot();

		LOG.info("GeminiStateBackend snapshot synchronous part took " +
			(System.currentTimeMillis() - syncStartTime) + " ms.");

		return new FutureTask<SnapshotResult<StatePartitionSnapshot>>(
			new Callable<SnapshotResult<StatePartitionSnapshot>>() {
				@Override
				public SnapshotResult<StatePartitionSnapshot> call() throws Exception {
					return snapshotOperator.materializeSnapshot();
				}
			}) {
			@Override
			protected void done() {
				snapshotOperator.releaseResources(isCancelled());
				LOG.info("resources of checkpoint " + checkpointId + " is released.");
			}
		};
	}

	@Override
	public void restore(Collection<StatePartitionSnapshot> restoredSnapshots) throws Exception {
		if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
			return;
		}

		RestoreOperator restoreOperator = geminiAgent.createRestoreOperator(cancelStreamRegistry);
		restoreOperator.restore(restoredSnapshots);
	}

	// -----------------------------------------------------------------------------------------------------------------

	private boolean isCopyValue() {
		return geminiConfiguration.isCopyValue() &&
			geminiConfiguration.getMemoryType() != GeminiConfiguration.MemoryType.OFFHEAP;
	}
}
