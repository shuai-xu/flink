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
import org.apache.flink.runtime.checkpoint.savepoint.CheckpointTestUtils;

import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

public class IncrementalKeyedStateSnapshotTest {
	/**
	 * This test checks, that for an unregistered {@link IncrementalKeyedStateSnapshot} all state
	 * (including shared) is discarded.
	 */
	@Test
	public void testUnregisteredDiscarding() throws Exception {
		IncrementalKeyedStateSnapshot stateHandle = create(new Random(42), KeyGroupRange.of(0, 0));

		stateHandle.discardState();

		for (StreamStateHandle handle : stateHandle.getPrivateState().values()) {
			verify(handle).discardState();
		}

		for (Tuple2<String, StreamStateHandle> tuple : stateHandle.getSharedState().values()) {
			verify(tuple.f1).discardState();
		}

		verify(stateHandle.getMetaStateHandle()).discardState();
	}

	/**
	 * This test checks, that for a registered {@link IncrementalKeyedStateHandle} discards respect
	 * all shared state and only discard it one all references are released.
	 */
	@Test
	public void testSharedStateDeRegistration() throws Exception {

		SharedStateRegistry registry = spy(new SharedStateRegistry());

		// Create two state handles with overlapping shared state
		IncrementalKeyedStateSnapshot stateHandle1 = create(new Random(42), KeyGroupRange.of(0, 0));
		IncrementalKeyedStateSnapshot stateHandle2 = create(new Random(42), KeyGroupRange.of(0, 0));

		// Both handles should not be registered and not discarded by now.
		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue().f1, times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue().f1, times(0)).discardState();
		}

		// Now we register both ...
		stateHandle1.registerSharedStates(registry);
		stateHandle2.registerSharedStates(registry);

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> stateHandleEntry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(stateHandleEntry.getValue().f0, stateHandleEntry.getKey());

			verify(registry).registerReference(
				registryKey,
				stateHandleEntry.getValue().f1);
		}

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> stateHandleEntry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(stateHandleEntry.getValue().f0, stateHandleEntry.getKey());

			verify(registry).registerReference(
				registryKey,
				stateHandleEntry.getValue().f1);
		}

		// We discard the first
		stateHandle1.discardState();

		// Should be unregistered, non-shared discarded, shared not discarded
		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey());

			verify(registry, times(1)).unregisterReference(registryKey);
			verify(entry.getValue().f1, times(0)).discardState();
		}

		for (Tuple2<String, StreamStateHandle> handle :
			stateHandle2.getSharedState().values()) {

			verify(handle.f1, times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> handleEntry :
			stateHandle1.getPrivateState().entrySet()) {

			verify(handleEntry.getValue(), times(1)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> handleEntry :
			stateHandle2.getPrivateState().entrySet()) {

			verify(handleEntry.getValue(), times(0)).discardState();
		}

		verify(stateHandle1.getMetaStateHandle(), times(1)).discardState();
		verify(stateHandle2.getMetaStateHandle(), times(0)).discardState();

		// We discard the second
		stateHandle2.discardState();


		// Now everything should be unregistered and discarded
		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey());

			verify(registry, times(2)).unregisterReference(registryKey);
			verify(entry.getValue().f1).discardState();
		}

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey());

			verify(registry, times(2)).unregisterReference(registryKey);
			verify(entry.getValue().f1).discardState();
		}

		verify(stateHandle1.getMetaStateHandle(), times(1)).discardState();
		verify(stateHandle2.getMetaStateHandle(), times(1)).discardState();
	}

	/**
	 * This tests that re-registration of shared state with another registry works as expected. This simulates a
	 * recovery from a checkpoint, when the checkpoint coordinator creates a new shared state registry and re-registers
	 * all live checkpoint states.
	 */
	@Test
	public void testSharedStateReRegistration() throws Exception {

		SharedStateRegistry stateRegistryA = spy(new SharedStateRegistry());

		IncrementalKeyedStateSnapshot stateHandleX = create(new Random(1), KeyGroupRange.of(0, 0));
		IncrementalKeyedStateSnapshot stateHandleY = create(new Random(2), KeyGroupRange.of(0, 0));
		IncrementalKeyedStateSnapshot stateHandleZ = create(new Random(3), KeyGroupRange.of(0, 0));

		// Now we register first time ...
		stateHandleX.registerSharedStates(stateRegistryA);
		stateHandleY.registerSharedStates(stateRegistryA);
		stateHandleZ.registerSharedStates(stateRegistryA);

		try {
			// Second attempt should fail
			stateHandleX.registerSharedStates(stateRegistryA);
			fail("Should not be able to register twice with the same registry.");
		} catch (IllegalStateException ignore) {
		}

		// Everything should be discarded for this handle
		stateHandleZ.discardState();
		verify(stateHandleZ.getMetaStateHandle(), times(1)).discardState();
		for (Tuple2<String, StreamStateHandle> stateHandle : stateHandleZ.getSharedState().values()) {
			verify(stateHandle.f1, times(1)).discardState();
		}

		// Close the first registry
		stateRegistryA.close();

		// Attempt to register to closed registry should trigger exception
		try {
			create(new Random(4), KeyGroupRange.of(0, 0)).registerSharedStates(stateRegistryA);
			fail("Should not be able to register new state to closed registry.");
		} catch (IllegalStateException ignore) {
		}

		// All state should still get discarded
		stateHandleY.discardState();
		verify(stateHandleY.getMetaStateHandle(), times(1)).discardState();
		for (Tuple2<String, StreamStateHandle> stateHandle: stateHandleY.getSharedState().values()) {
			verify(stateHandle.f1, times(1)).discardState();
		}

		// This should still be unaffected
		verify(stateHandleX.getMetaStateHandle(), never()).discardState();
		for (Tuple2<String, StreamStateHandle> stateHandle : stateHandleX.getSharedState().values()) {
			verify(stateHandle.f1, never()).discardState();
		}

		// We re-register the handle with a new registry
		SharedStateRegistry sharedStateRegistryB = spy(new SharedStateRegistry());
		stateHandleX.registerSharedStates(sharedStateRegistryB);
		stateHandleX.discardState();

		// Should be completely discarded because it is tracked through the new registry
		verify(stateHandleX.getMetaStateHandle(), times(1)).discardState();
		for (Tuple2<String, StreamStateHandle> stateHandle : stateHandleX.getSharedState().values()) {
			verify(stateHandle.f1, times(1)).discardState();
		}

		sharedStateRegistryB.close();
	}

	@Test
	public void testGetIntersection() {
		// verify expected getIntersection
		IncrementalKeyedStateSnapshot stateSnapshot03 = create(ThreadLocalRandom.current(), KeyGroupRange.of(0, 3));

		KeyedStateHandle intersection = stateSnapshot03.getIntersection(KeyGroupRange.of(3, 5));
		assertNotNull(intersection);
		assertEquals(stateSnapshot03, intersection);

		// verify return null when key-group rang not intersected
		intersection = stateSnapshot03.getIntersection(KeyGroupRange.of(4, 6));
		assertNull(intersection);
	}

	private static IncrementalKeyedStateSnapshot create(Random rnd, KeyGroupRange keyGroupRange) {
		return new IncrementalKeyedStateSnapshot(
			keyGroupRange,
			1L,
			placeTupleSpies(CheckpointTestUtils.createRandomStateSnapshotMap(rnd)),
			placeSpies(CheckpointTestUtils.createRandomStateHandleMap(rnd)),
			spy(CheckpointTestUtils.createDummyStreamStateHandle(rnd)));
	}

	private static Map<StateHandleID, StreamStateHandle> placeSpies(
		Map<StateHandleID, StreamStateHandle> map) {

		for (Map.Entry<StateHandleID, StreamStateHandle> entry : map.entrySet()) {
			entry.setValue(spy(entry.getValue()));
		}
		return map;
	}

	private static Map<StateHandleID, Tuple2<String, StreamStateHandle>> placeTupleSpies(
		Map<StateHandleID, Tuple2<String, StreamStateHandle>> map) {

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : map.entrySet()) {
			Tuple2<String, StreamStateHandle> tuple = entry.getValue();
			entry.setValue(Tuple2.of(tuple.f0, spy(tuple.f1)));
		}
		return map;
	}
}
