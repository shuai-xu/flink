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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.checkpoint.savepoint.CheckpointTestUtils.createDummySegmentStreamStateHandle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for {@link IncrementalSegmentStateSnapshot}.
 */
public class IncrementalSegmentStateSnapshotTest {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * This test checks, that for an unregistered {@link IncrementalSegmentStateSnapshot} the file still exists.
	 */
	@Test
	public void testUnregisteredDiscarding() throws Exception {
		File file = createTempFile();

		IncrementalSegmentStateSnapshot stateHandle = create(new Random(42), KeyGroupRange.of(0, 0), file, 0, file.length(), false);

		assertTrue(file.exists());
		stateHandle.discardState();
		assertTrue(file.exists());
	}

	@Test
	public void testGetIntersection() throws IOException {
		File file = createTempFile();

		// verify expected getIntersection
		IncrementalSegmentStateSnapshot stateSnapshot03 = create(ThreadLocalRandom.current(), KeyGroupRange.of(0, 3), file, 0, file.length(), false);

		KeyedStateHandle intersection = stateSnapshot03.getIntersection(KeyGroupRange.of(3, 5));
		assertNotNull(intersection);
		assertEquals(stateSnapshot03, intersection);

		// verify return null when key-group rang not intersected
		intersection = stateSnapshot03.getIntersection(KeyGroupRange.of(4, 6));
		assertNull(intersection);
	}

	/**
	 * This tests that re-registration of state with another registry works as expected. This simulates a
	 * recovery from a checkpoint, when the checkpoint coordinator creates a new
	 * state registry and re-registers
	 * all live checkpoint states.
	 */
	@Test
	public void testSharedStateReRegistration() throws Exception {
		File file1 = createTempFile();
		File file2 = createTempFile();

		SharedStateRegistry stateRegistryA = spy(new SharedStateRegistry());

		long end1 = file1.length() / 2;
		long satrt2 = end1;
		long end2 = file1.length();
		long end3 = file2.length();
		IncrementalSegmentStateSnapshot stateSnapshotX = create(new Random(1), KeyGroupRange.of(0, 0), file1, 0, end1, false);
		StreamStateHandle metaStateHandle1 = stateSnapshotX.getMetaStateHandle();
		assertTrue(metaStateHandle1 instanceof FileSegmentStateHandle);
		FileSegmentStateHandle registeredStateHandle1 = (FileSegmentStateHandle) metaStateHandle1;
		SharedStateRegistryKey actualRegisteredKey1 = new SharedStateRegistryKey(registeredStateHandle1.getFilePath().toString());
		int stateSnapshotXSize = stateSnapshotX.getSharedState().size() + stateSnapshotX.getPrivateState().size() + 1;

		IncrementalSegmentStateSnapshot stateSnapshotY = create(new Random(2), KeyGroupRange.of(0, 0), file1, satrt2, end2, true);
		int stateSnapshotYSize = stateSnapshotY.getSharedState().size() + stateSnapshotY.getPrivateState().size() + 1;

		IncrementalSegmentStateSnapshot stateSnapshotZ = create(new Random(3), KeyGroupRange.of(0, 0), file2, 0, end3, false);
		StreamStateHandle metaStateHandle2 = stateSnapshotZ.getMetaStateHandle();
		assertTrue(metaStateHandle2 instanceof FileSegmentStateHandle);
		FileSegmentStateHandle registeredStateHandle2 = (FileSegmentStateHandle) metaStateHandle2;
		SharedStateRegistryKey actualRegisteredKey2 = new SharedStateRegistryKey(registeredStateHandle2.getFilePath().toString());
		int stateSnapshotZSize = stateSnapshotZ.getSharedState().size() + stateSnapshotZ.getPrivateState().size() + 1;

		// Now we register first time ...
		stateSnapshotX.registerSharedStates(stateRegistryA);
		verify(stateRegistryA, times(stateSnapshotXSize))
				.registerReference(eq(actualRegisteredKey1), any(FileSegmentStateHandle.class));

		stateSnapshotY.registerSharedStates(stateRegistryA);
		verify(stateRegistryA, times(stateSnapshotXSize + stateSnapshotYSize))
				.registerReference(eq(actualRegisteredKey1), any(FileSegmentStateHandle.class));

		stateSnapshotZ.registerSharedStates(stateRegistryA);
		verify(stateRegistryA, times(stateSnapshotZSize))
				.registerReference(eq(actualRegisteredKey2), any(FileSegmentStateHandle.class));

		try {
			// Second attempt should fail
			stateSnapshotX.registerSharedStates(stateRegistryA);
			fail("Should not be able to register twice with the same registry.");
		} catch (IllegalStateException ignore) {
		}

		stateSnapshotX.discardState();
		verify(stateRegistryA, times(stateSnapshotXSize)).unregisterReference(eq(actualRegisteredKey1));
		assertTrue(file1.exists());

		// Close the first registry
		stateRegistryA.close();

		// Attempt to register to closed registry should trigger exception
		try {
			create(new Random(4), KeyGroupRange.of(0, 0), file1, 0, file1.length() / 4, true).registerSharedStates(stateRegistryA);
			fail("Should not be able to register new state to closed registry.");
		} catch (IllegalStateException ignore) {
		}

		// All state should still get discarded
		stateSnapshotY.discardState();
		verify(stateRegistryA, times(stateSnapshotXSize + stateSnapshotYSize)).unregisterReference(eq(actualRegisteredKey1));
		assertFalse(file1.exists());

		// This should still be unaffected
		assertTrue(file2.exists());
		verify(stateRegistryA, never()).unregisterReference(eq(actualRegisteredKey2));

		// We re-register the handle with a new registry
		SharedStateRegistry stateRegistryB = spy(new SharedStateRegistry());
		stateSnapshotZ.registerSharedStates(stateRegistryB);
		verify(stateRegistryB, times(stateSnapshotZSize)).registerReference(eq(actualRegisteredKey2), any(FileSegmentStateHandle.class));

		stateSnapshotZ.discardState();
		verify(stateRegistryB, times(stateSnapshotZSize)).unregisterReference(eq(actualRegisteredKey2));
		assertFalse(file2.exists());

		stateRegistryB.close();
	}

	/**
	 * This test checks, that for a registered {@link IncrementalSegmentStateSnapshot} discards respect
	 * all state and only discard it one all references are released.
	 */
	@Test
	public void testSegmentSharedStateDeRegistration() throws Exception {

		File file = temporaryFolder.newFile();
		byte[] data = new byte[ThreadLocalRandom.current().nextInt(4 * 1024) + 2048];
		ThreadLocalRandom.current().nextBytes(data);

		try (OutputStream out = new FileOutputStream(file)) {
			out.write(data);
		}

		long endPosition = data.length;
		long midPosition = endPosition / 6;

		SharedStateRegistry registry = spy(new SharedStateRegistry());

		// Create two state handles with overlapping state
		IncrementalSegmentStateSnapshot stateSnapshot1 = create(new Random(42), KeyGroupRange.of(0, 0), file, 0, midPosition, false);
		IncrementalSegmentStateSnapshot stateSnapshot2 = create(new Random(42), KeyGroupRange.of(0, 0), file, midPosition, endPosition, false);
		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : stateSnapshot2.getSharedState().entrySet()) {
			String id = entry.getValue().f0;
			assertTrue(entry.getValue().f1 instanceof FileSegmentStateHandle);
			PlaceholderSegmentStateHandle placeholderSegmentStateHandle = spy(
				new PlaceholderSegmentStateHandle((FileSegmentStateHandle) entry.getValue().f1));
			entry.setValue(Tuple2.of(id, placeholderSegmentStateHandle));
		}

		// Both handles should not be registered and not discarded by now.
		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
				stateSnapshot1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
					stateSnapshot1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0);

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue().f1, times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry :
				stateSnapshot2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
					stateSnapshot1.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0);

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue().f1, times(0)).discardState();
		}

		// Now we register the first one
		stateSnapshot1.registerSharedStates(registry);

		StreamStateHandle metaStateHandle = stateSnapshot1.getMetaStateHandle();
		assertTrue(metaStateHandle instanceof FileSegmentStateHandle);
		FileSegmentStateHandle registeredStateHandle = (FileSegmentStateHandle) metaStateHandle;
		SharedStateRegistryKey actualRegisteredKey = new SharedStateRegistryKey(registeredStateHandle.getFilePath().toString());

		int stateSnapshot1Size = stateSnapshot1.getSharedState().size() + stateSnapshot1.getPrivateState().size() + 1;
		int registeredTimes = stateSnapshot1Size;
		verify(registry, times(registeredTimes)).registerReference(eq(actualRegisteredKey), any(FileSegmentStateHandle.class));

		// Now we register the second one
		stateSnapshot2.registerSharedStates(registry);

		int stateSnapshot2Size = stateSnapshot2.getSharedState().size() + stateSnapshot2.getPrivateState().size() + 1;
		registeredTimes += stateSnapshot2Size;
		verify(registry, times(registeredTimes)).registerReference(eq(actualRegisteredKey), any(FileSegmentStateHandle.class));

		// We discard the first
		stateSnapshot1.discardState();

		assertTrue(file.exists());
		verify(registry, times(stateSnapshot1Size)).unregisterReference(actualRegisteredKey);

		// We discard the second
		stateSnapshot2.discardState();

		assertFalse(file.exists());
		verify(registry, times(stateSnapshot1Size + stateSnapshot2Size)).unregisterReference(actualRegisteredKey);
	}

	public static IncrementalSegmentStateSnapshot create(
			Random rnd,
			KeyGroupRange keyGroupRange,
			File underlyingFile,
			long startPosition,
			long endPosition,
			boolean registeredEver) {

		long start1 = startPosition;
		long offset = endPosition - startPosition;
		long end1 = Math.max(start1, offset / 3 + startPosition);
		long start2 = end1;
		long end2 = offset / 2 + startPosition;
		long start3 = end2;
		long end3 = endPosition;

		return new IncrementalSegmentStateSnapshot(
				keyGroupRange,
				1L,
				placeTupleSpies(createRandomStateSnapshotMap(rnd, underlyingFile, start1, end1, registeredEver)),
				placeSpies(createRandomStateHandleMap(rnd, underlyingFile, start2, end2)),
				spy(createDummySegmentStreamStateHandle(underlyingFile, start3, end3)));
	}

	public static Map<StateHandleID, StreamStateHandle> createRandomStateHandleMap(
			Random rnd,
			File file,
			long startPosition,
			long endPosition) {
		final int size = rnd.nextInt(4);
		Map<StateHandleID, StreamStateHandle> result = new HashMap<>(size);
		if (size > 0) {
			long start = startPosition;
			long step = (endPosition - startPosition) / size ;
			long end = size == 1 ? endPosition : step + startPosition;

			for (int i = 0; i < size; ++i) {
				StateHandleID randomId = new StateHandleID(createRandomUUID(rnd).toString());
				StreamStateHandle stateHandle = createDummySegmentStreamStateHandle(file, start, end);
				start = end;
				if (i == size - 2) {
					end = endPosition;
				} else {
					end = Math.min(step * (i + 1) + startPosition, endPosition);
				}
				result.put(randomId, stateHandle);
			}
		}

		return result;
	}

	public static Map<StateHandleID, Tuple2<String, StreamStateHandle>> createRandomStateSnapshotMap(
			Random rnd,
			File file,
			long startPosition,
			long endPosition,
			boolean registeredEver) {
		final int size = rnd.nextInt(4);
		Map<StateHandleID, Tuple2<String, StreamStateHandle>> result = new HashMap<>(size);
		if (size > 0) {
			long start = startPosition;
			long step = (endPosition - startPosition) / size ;
			long end = size == 1 ? endPosition : step + startPosition;

			for (int i = 0; i < size; ++i) {
				StateHandleID randomId = new StateHandleID(createRandomUUID(rnd).toString());
				StreamStateHandle stateHandle = createDummySegmentStreamStateHandle(file, start, end);
				start = end;
				if (i == size - 2) {
					end = endPosition;
				} else {
					end = Math.min(step * (i + 1) + startPosition, endPosition);
				}
				if (registeredEver) {
					result.put(randomId, Tuple2.of(file.toURI().toString(), new PlaceholderSegmentStateHandle((FileSegmentStateHandle) stateHandle)));
				} else {
					result.put(randomId, Tuple2.of(file.toURI().toString(), stateHandle));
				}
			}
		}

		return result;
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

	private static UUID createRandomUUID(Random rnd) {
		return new UUID(rnd.nextLong(), rnd.nextLong());
	}

	private File createTempFile() throws IOException {
		File file = temporaryFolder.newFile();
		byte[] data = new byte[ThreadLocalRandom.current().nextInt(4 * 1024) + 2048];
		ThreadLocalRandom.current().nextBytes(data);
		try (OutputStream out = new FileOutputStream(file)) {
			out.write(data);
		}
		return file;
	}
}
