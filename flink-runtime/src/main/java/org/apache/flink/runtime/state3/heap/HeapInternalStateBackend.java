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

package org.apache.flink.runtime.state3.heap;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state3.AbstractInternalStateBackend;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.RunnableFuture;

/**
 * Implementation of {@link AbstractInternalStateBackend} which stores the key-value
 * pairs of states on the Java Heap.
 */
public class HeapInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(HeapInternalStateBackend.class);

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * Whether this backend supports async snapshot.
	 */
	private final boolean asynchronousSnapshots;

	public HeapInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry
	) {
		this(numberOfGroups, groups, userClassLoader, localRecoveryConfig, kvStateRegistry, true);
	}

	public HeapInternalStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry,
		boolean asynchronousSnapshots
	) {
		super(numberOfGroups, groups, userClassLoader, kvStateRegistry);

		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);
		this.asynchronousSnapshots = asynchronousSnapshots;

		LOG.info("HeapInternalStateBackend is created with {} mode.", (asynchronousSnapshots ? "async" : "sync"));
	}

	@Override
	public void closeImpl() {

	}

	@Override
	@SuppressWarnings("unchecked")
	protected StateStorage createStateStorageForKeyedState(KeyedStateDescriptor descriptor) {
		String stateName = descriptor.getName();
		StateStorage stateStorage = stateStorages.get(stateName);

		if (stateStorage == null) {
			stateStorage = new HeapStateStorage<>(
				this,
				descriptor.getKeySerializer(),
				VoidNamespaceSerializer.INSTANCE,
				descriptor.getValueSerializer(),
				VoidNamespace.INSTANCE,
				false,
				asynchronousSnapshots
			);
			stateStorages.put(stateName, stateStorage);
		}

		return stateStorage;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected StateStorage createStateStorageForSubKeyedState(SubKeyedStateDescriptor descriptor) {
		String stateName = descriptor.getName();
		StateStorage stateStorage = stateStorages.get(stateName);

		if (stateStorage == null) {
			stateStorage = new HeapStateStorage<>(
				this,
				descriptor.getKeySerializer(),
				descriptor.getNamespaceSerializer(),
				descriptor.getValueSerializer(),
				null,
				true,
				asynchronousSnapshots
			);
			stateStorages.put(stateName, stateStorage);
		}

		return stateStorage;
	}

	@Override
	public RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions
	) throws Exception {

		// TODO implement snapshot after sub-keyed state.
		return DoneFuture.of(SnapshotResult.empty());
	}

	@Override
	public void restore(
		Collection<StatePartitionSnapshot> restoredSnapshots
	) throws Exception {
		// TODO implement restore after sub-keyed state.
	}

}
