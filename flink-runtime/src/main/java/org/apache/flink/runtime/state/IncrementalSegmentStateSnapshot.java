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

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FileSegmentStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An implementation of {@link KeyedStateHandle} which will be used in incremental snapshot/restore
 * with all its stream state handles are {@link FileSegmentStateHandle}..
 */
public class IncrementalSegmentStateSnapshot extends IncrementalKeyedStateSnapshot {

	private static final Logger LOG = LoggerFactory.getLogger(IncrementalSegmentStateSnapshot.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Once the shared states are registered, it is the {@link SharedStateRegistry}'s
	 * responsibility to cleanup those shared states.
	 * But in the cases where the state handle is discarded before performing the registration,
	 * the handle should delete all the shared states created by it.
	 *
	 * <p>his variable is not null iff the handles was registered.
	 */
	private transient SharedStateRegistry sharedStateRegistry;

	public IncrementalSegmentStateSnapshot(
			KeyGroupRange keyGroupRange,
			long checkpointId,
			Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedState,
			Map<StateHandleID, StreamStateHandle> privateState,
			StreamStateHandle metaStateHandle) {
		super(keyGroupRange, checkpointId, sharedState, privateState, metaStateHandle);

		this.sharedStateRegistry = null;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		// This is a quick check to avoid that we register twice with the same registry. However, the code allows to
		// register again with a different registry. The implication is that ownership is transferred to this new
		// registry. This should only happen in case of a restart, when the CheckpointCoordinator creates a new
		// SharedStateRegistry for the current attempt and the old registry becomes meaningless. We also assume that
		// an old registry object from a previous run is due to be GCed and will never be used for registration again.
		Preconditions.checkState(
				sharedStateRegistry != stateRegistry,
				"The state handle has already registered its shared states to the given registry.");

		sharedStateRegistry = Preconditions.checkNotNull(stateRegistry);

		LOG.trace("Registering IncrementalSegmentStateSnapshot for checkpoint {} from backend.", getCheckpointId());
		StreamStateHandle metaStateHandle = getMetaStateHandle();

		registerSegmentHandle(stateRegistry, metaStateHandle);

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandle : getSharedState().entrySet()) {
			StreamStateHandle stateHandle = sharedStateHandle.getValue().f1;
			if (stateHandle instanceof FileSegmentStateHandle) {
				registerSegmentHandle(stateRegistry, sharedStateHandle.getValue().f1);
			} else if (stateHandle instanceof PlaceholderSegmentStateHandle) {

				PlaceholderSegmentStateHandle placeholderSegmentStateHandle = (PlaceholderSegmentStateHandle) stateHandle;
				String uniqueId = placeholderSegmentStateHandle.getFilePath().toString();
				SharedStateRegistryKey registryKey =
						createSharedStateRegistryKeyFromUniqueId(uniqueId);

				stateRegistry.registerReference(registryKey, placeholderSegmentStateHandle);
				// let IncrementalSegmentStateSnapshot itself could have actual file segment handle.
				sharedStateHandle.setValue(Tuple2.of(uniqueId, placeholderSegmentStateHandle.toFileSegmentStateHandle()));
			}
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> privateStateHandle : getPrivateState().entrySet()) {
			registerSegmentHandle(stateRegistry, privateStateHandle.getValue());
		}
	}

	private void registerSegmentHandle(SharedStateRegistry stateRegistry, StreamStateHandle streamStateHandle) {
		Preconditions.checkState(
				streamStateHandle instanceof FileSegmentStateHandle,
				"The state handle to register should be a FileSegmentStateHandle.");

		SharedStateRegistryKey registryKey = ((FileSegmentStateHandle) streamStateHandle).getRegistryKey();
		stateRegistry.registerReference(registryKey, streamStateHandle);
	}

	@Override
	public void discardState() {
		SharedStateRegistry registry = this.sharedStateRegistry;
		final boolean isRegistered = (registry != null);

		LOG.trace("Discarding IncrementalKeyedStateSnapshot (registered = {}) for checkpoint {} from backend.",
				isRegistered,
				getCheckpointId());

		if (isRegistered) {
			registry.unregisterReference(createSharedStateRegistryKeyFromSegment(getMetaStateHandle()));

			for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : getSharedState().entrySet()) {
				registry.unregisterReference(
						createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0));
			}

			for (Map.Entry<StateHandleID, StreamStateHandle> entry : getPrivateState().entrySet()) {
				registry.unregisterReference(createSharedStateRegistryKeyFromSegment(entry.getValue()));
			}
		}
	}

	@Override
	public String toString() {
		return "IncrementalSegmentStateSnapshot{" +
				"keyGroupRange=" + getKeyGroupRange() +
				", checkpointId=" + getCheckpointId() +
				", sharedState=" + getSharedState() +
				", privateState=" + getPrivateState() +
				", metaStateHandle=" + getMetaStateHandle() +
				", registered=" + (sharedStateRegistry != null) +
				'}';
	}

	// ----------------------------------------------------------------------------------------

	SharedStateRegistryKey createSharedStateRegistryKeyFromUniqueId(String uniqueId) {
		return new SharedStateRegistryKey(uniqueId);
	}

	private SharedStateRegistryKey createSharedStateRegistryKeyFromSegment(StreamStateHandle streamStateHandle) {
		Preconditions.checkState(streamStateHandle instanceof FileSegmentStateHandle,
				"Incremental segment state snapshot only supports FileSegmentStateHandle.");
		return new SharedStateRegistryKey(((FileSegmentStateHandle) streamStateHandle).getFilePath().toString());
	}
}
