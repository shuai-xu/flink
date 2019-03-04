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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvailableSlotsTest extends TestLogger {

	static final ResourceProfile DEFAULT_TESTING_PROFILE = new ResourceProfile(1.0, 512);

	static final ResourceProfile DEFAULT_TESTING_BIG_PROFILE = new ResourceProfile(2.0, 1024);

	@Test
	public void testAddAndRemove() throws Exception {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots();

		final ResourceID resource1 = new ResourceID("resource1");
		final ResourceID resource2 = new ResourceID("resource2");

		final AllocatedSlot slot1 = createAllocatedSlot(resource1);
		final AllocatedSlot slot2 = createAllocatedSlot(resource1);
		final AllocatedSlot slot3 = createAllocatedSlot(resource2);

		availableSlots.add(slot1, 1L);
		availableSlots.add(slot2, 2L);
		availableSlots.add(slot3, 3L);

		assertEquals(3, availableSlots.size());
		assertTrue(availableSlots.contains(slot1.getAllocationId()));
		assertTrue(availableSlots.contains(slot2.getAllocationId()));
		assertTrue(availableSlots.contains(slot3.getAllocationId()));
		assertTrue(availableSlots.containsTaskManager(resource1));
		assertTrue(availableSlots.containsTaskManager(resource2));

		availableSlots.removeAllForTaskManager(resource1);

		assertEquals(1, availableSlots.size());
		assertFalse(availableSlots.contains(slot1.getAllocationId()));
		assertFalse(availableSlots.contains(slot2.getAllocationId()));
		assertTrue(availableSlots.contains(slot3.getAllocationId()));
		assertFalse(availableSlots.containsTaskManager(resource1));
		assertTrue(availableSlots.containsTaskManager(resource2));

		availableSlots.removeAllForTaskManager(resource2);

		assertEquals(0, availableSlots.size());
		assertFalse(availableSlots.contains(slot1.getAllocationId()));
		assertFalse(availableSlots.contains(slot2.getAllocationId()));
		assertFalse(availableSlots.contains(slot3.getAllocationId()));
		assertFalse(availableSlots.containsTaskManager(resource1));
		assertFalse(availableSlots.containsTaskManager(resource2));
	}

	@Test
	public void testPollFreeSlot() {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots();

		final ResourceID resource1 = new ResourceID("resource1");
		final AllocatedSlot slot1 = createAllocatedSlot(resource1);

		availableSlots.add(slot1, 1L);

		assertEquals(1, availableSlots.size());
		assertTrue(availableSlots.contains(slot1.getAllocationId()));
		assertTrue(availableSlots.containsTaskManager(resource1));

		assertNull(availableSlots.poll(LocationPreferenceSchedulingStrategy.getInstance(), SlotProfile.noLocality(DEFAULT_TESTING_BIG_PROFILE)));

		SlotAndLocality slotAndLocality = availableSlots.poll(LocationPreferenceSchedulingStrategy.getInstance(), SlotProfile.noLocality(DEFAULT_TESTING_PROFILE));
		assertEquals(slot1, slotAndLocality.getSlot());
		assertEquals(0, availableSlots.size());
		assertFalse(availableSlots.contains(slot1.getAllocationId()));
		assertFalse(availableSlots.containsTaskManager(resource1));
	}

	@Test
	public void testPollSlotConvergedInTaskManagers() {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots();
		SlotPool.AllocatedSlots allocatedSlots = new SlotPool.AllocatedSlots();

		final ResourceID resource1 = new ResourceID("resource1");
		final ResourceID resource2 = new ResourceID("resource2");
		final ResourceID resource3 = new ResourceID("resource3");
		final ResourceID resource4 = new ResourceID("resource4");

		final AllocatedSlot slot11 = createAllocatedSlot(resource1);
		final AllocatedSlot slot12 = createAllocatedSlot(resource1);
		final AllocatedSlot slot13 = createAllocatedSlot(resource1);

		final AllocatedSlot slot21 = createAllocatedSlot(resource2);
		final AllocatedSlot slot22 = createAllocatedSlot(resource2);
		final AllocatedSlot slot23 = createAllocatedSlot(resource2);

		final AllocatedSlot slot31 = createAllocatedSlot(resource3);
		final AllocatedSlot slot32 = createAllocatedSlot(resource3);
		final AllocatedSlot slot33 = createAllocatedSlot(resource3);

		final AllocatedSlot slot41 = createAllocatedSlot(resource4);
		final AllocatedSlot slot42 = createAllocatedSlot(resource4);
		final AllocatedSlot slot43 = createAllocatedSlot(resource4);

		availableSlots.add(slot11, 1L);
		availableSlots.add(slot12, 1L);
		availableSlots.add(slot13, 1L);

		availableSlots.add(slot21, 1L);
		allocatedSlots.add(new SlotRequestId(), slot22);
		allocatedSlots.add(new SlotRequestId(), slot23);

		availableSlots.add(slot31, 1L);
		availableSlots.add(slot32, 1L);

		availableSlots.add(slot41, 1L);
		allocatedSlots.add(new SlotRequestId(), slot42);

		AllocatedSlot polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(slot21, polledSlot);
		allocatedSlots.add(new SlotRequestId(), polledSlot);

		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(slot41, polledSlot);
		allocatedSlots.add(new SlotRequestId(), polledSlot);

		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(resource1, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);
		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(resource1, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);
		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(resource1, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);

		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(resource3, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);
		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots).getSlot();
		assertEquals(resource3, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);

		assertNull(availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
			allocatedSlots));
	}

	@Test
	public void testSlotStrictlyMatchWhenPollSlotConvergedInTaskManagers() {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots();
		SlotPool.AllocatedSlots allocatedSlots = new SlotPool.AllocatedSlots();

		final ResourceID resource1 = new ResourceID("resource1");

		final AllocatedSlot slot11 = createAllocatedSlot(resource1);
		final AllocatedSlot slot12 = createAllocatedSlot(resource1);
		final AllocatedSlot slot13 = createAllocatedSlot(resource1);

		availableSlots.add(slot11, 1L);
		availableSlots.add(slot12, 1L);
		availableSlots.add(slot13, 1L);

		assertNull(availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(new ResourceProfile(1.0, 500)),
			allocatedSlots));
		assertNull(availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(new ResourceProfile(1.0, 520)),
			allocatedSlots));

		AllocatedSlot polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(new ResourceProfile(1.0, 512)),
			allocatedSlots).getSlot();
		assertEquals(resource1, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);
		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(new ResourceProfile(1.0, 512)),
			allocatedSlots).getSlot();
		assertEquals(resource1, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);
		polledSlot = availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(new ResourceProfile(1.0, 512)),
			allocatedSlots).getSlot();
		assertEquals(resource1, polledSlot.getTaskManagerId());
		allocatedSlots.add(new SlotRequestId(), polledSlot);

		assertNull(availableSlots.pollSlotConvergedInTaskManagers(
			LocationPreferenceSchedulingStrategy.getInstance(),
			SlotProfile.noLocality(new ResourceProfile(1.0, 512)),
			allocatedSlots));
	}

	static AllocatedSlot createAllocatedSlot(final ResourceID resourceId) {
		TaskManagerLocation mockTaskManagerLocation = mock(TaskManagerLocation.class);
		when(mockTaskManagerLocation.getResourceID()).thenReturn(resourceId);

		TaskManagerGateway mockTaskManagerGateway = mock(TaskManagerGateway.class);

		return new AllocatedSlot(
			new AllocationID(),
			mockTaskManagerLocation,
			0,
			DEFAULT_TESTING_PROFILE,
			mockTaskManagerGateway);
	}
}
