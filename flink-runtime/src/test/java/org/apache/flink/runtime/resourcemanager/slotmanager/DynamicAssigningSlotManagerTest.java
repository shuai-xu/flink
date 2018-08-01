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

package org.apache.flink.runtime.resourcemanager.slotmanager;

	import org.apache.flink.api.common.JobID;
	import org.apache.flink.api.common.time.Time;
	import org.apache.flink.api.java.tuple.Tuple2;
	import org.apache.flink.runtime.clusterframework.types.AllocationID;
	import org.apache.flink.runtime.clusterframework.types.ResourceID;
	import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
	import org.apache.flink.runtime.clusterframework.types.SlotID;
	import org.apache.flink.runtime.concurrent.Executors;
	import org.apache.flink.runtime.messages.Acknowledge;
	import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
	import org.apache.flink.runtime.resourcemanager.SlotRequest;
	import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
	import org.apache.flink.runtime.taskexecutor.SlotReport;
	import org.apache.flink.runtime.taskexecutor.SlotStatus;
	import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
	import org.apache.flink.runtime.testingUtils.TestingUtils;

	import org.junit.Test;

	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.List;
	import java.util.Map;
	import java.util.concurrent.CompletableFuture;

	import static org.junit.Assert.assertEquals;
	import static org.junit.Assert.assertNotNull;
	import static org.mockito.Matchers.any;
	import static org.mockito.Matchers.anyLong;
	import static org.mockito.Matchers.anyString;
	import static org.mockito.Matchers.eq;
	import static org.mockito.Mockito.mock;
	import static org.mockito.Mockito.times;
	import static org.mockito.Mockito.verify;
	import static org.mockito.Mockito.when;

/**
 * Tests for the dynamic assigning slot manager for Blink.
 */
public class DynamicAssigningSlotManagerTest {

	private static final double DEFAULT_TESTING_CPU_CORES = 1.0;

	private static final int DEFAULT_TESTING_MEMORY = 512;

	private static final ResourceProfile DEFAULT_TESTING_PROFILE =
		new ResourceProfile(DEFAULT_TESTING_CPU_CORES, DEFAULT_TESTING_MEMORY);

	private static final ResourceProfile DEFAULT_TESTING_BIG_PROFILE =
		new ResourceProfile(2 * DEFAULT_TESTING_CPU_CORES, 2 * DEFAULT_TESTING_MEMORY);

	/**
	 * Test that the DynamicAssigningSlotManager is able to recover the allocation status after RM failover.
	 */
	@Test
	public void testTaskManagerRegistrationAfterFailover() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = mock(ResourceActions.class);

		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);

		final JobID jobId = new JobID();
		final AllocationID allocationID = new AllocationID();

		final SlotStatus slotStatus1 = new SlotStatus(slotId1, ResourceProfile.UNKNOWN, null, null, null, 3L);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, ResourceProfile.UNKNOWN, jobId, allocationID, DEFAULT_TESTING_PROFILE, 6L);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		try (DynamicAssigningSlotManager slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertEquals("The number registered slots does not equal the expected number.", 2, slotManager.getNumberRegisteredSlots());
			assertEquals(1, slotManager.getNumberFreeSlots());

			Map<ResourceID, Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile>> allocatedSlotsResource = slotManager.getAllocatedSlotsResource();
			assertEquals(1, allocatedSlotsResource.size());

			Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile> allocatedOnThisTaskManager = allocatedSlotsResource.get(resourceId);
			assertNotNull(allocatedOnThisTaskManager);
			assertEquals(1, allocatedOnThisTaskManager.f0.size());

			ResourceProfile allocationResourceProfile = allocatedOnThisTaskManager.f0.get(slotId2);
			assertEquals(DEFAULT_TESTING_PROFILE, allocationResourceProfile);
		}
	}


	/**
	 * Tests that when there are free resource in the task executor, request slot succeed,
	 * when free resource exhausted, request slot will not be fulfilled.
	 */
	@Test
	public void testRequestSlotUtilFreeResourceNotEnough() throws Exception {
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final SlotID slotId1 = new SlotID(resourceID, 0);
		final SlotID slotId2 = new SlotID(resourceID, 1);
		final List<SlotStatus> slotStatus = new ArrayList<>(2);
		slotStatus.add(new SlotStatus(slotId1, ResourceProfile.UNKNOWN, jobId, null, null, 0L));
		slotStatus.add(new SlotStatus(slotId2, ResourceProfile.UNKNOWN, jobId, null, null, 0L));
		final SlotReport slotReport = new SlotReport(slotStatus);

		final SlotRequest slotRequest1 = new SlotRequest(
			jobId,
			new AllocationID(),
			DEFAULT_TESTING_PROFILE,
			"localhost");

		final SlotRequest slotRequest2 = new SlotRequest(
			jobId,
			new AllocationID(),
			DEFAULT_TESTING_PROFILE,
			"localhost");

		final ResourceActions resourceManagerActions = mock(ResourceActions.class);
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			eq(jobId),
			any(AllocationID.class),
			any(ResourceProfile.class),
			anyString(),
			eq(resourceManagerId),
			anyLong(),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		try (DynamicAssigningSlotManager slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			assertEquals(2, slotManager.getNumberFreeSlots());

			slotManager.registerSlotRequest(slotRequest1);
			assertEquals(1, slotManager.getNumberFreeSlots());

			slotManager.registerSlotRequest(slotRequest2);
			assertEquals(1, slotManager.getNumberFreeSlots());
			verify(resourceManagerActions, times(1)).allocateResource(eq(DEFAULT_TESTING_PROFILE));
		}
	}

	/**
	 * Tests that a new slot appeared in SlotReport, and can fulfill a pending request if resource enough in the task executor.
	 */
	@Test
	public void testNewlyAppearedFreeSlotFulfillPendingRequestUntilResourceNotEnough() throws Exception {
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId1 = new SlotID(resourceID, 0);
		final SlotID slotId2 = new SlotID(resourceID, 1);
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		final List<SlotStatus> slotStatus = new ArrayList<>(2);
		slotStatus.add(new SlotStatus(slotId1, ResourceProfile.UNKNOWN, jobId, null, null, 0L));
		slotStatus.add(new SlotStatus(slotId2, ResourceProfile.UNKNOWN, jobId, null, null, 0L));

		final SlotReport slotReport = new SlotReport(slotStatus);

		AllocationID allocationID1 = new AllocationID();
		AllocationID allocationID2 = new AllocationID();
		final SlotRequest slotRequest1 = new SlotRequest(
			jobId,
			allocationID1,
			DEFAULT_TESTING_PROFILE,
			"localhost");
		final SlotRequest slotRequest2 = new SlotRequest(
			jobId,
			allocationID2,
			DEFAULT_TESTING_PROFILE,
			"localhost");

		final ResourceActions resourceManagerActions = mock(ResourceActions.class);
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			eq(jobId),
			any(AllocationID.class),
			any(ResourceProfile.class),
			anyString(),
			eq(resourceManagerId),
			anyLong(),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		try (DynamicAssigningSlotManager slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {

			slotManager.registerSlotRequest(slotRequest1);
			slotManager.registerSlotRequest(slotRequest2);

			verify(resourceManagerActions, times(2)).allocateResource(DEFAULT_TESTING_PROFILE);

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			assertEquals(1, slotManager.getNumberFreeSlots());
		}
	}

	/**
	 * Tests that a big slot released, two little pending requests can be fulfilled now.
	 * See BLINK-14891808
	 */
	@Test
	public void testReleasedResourceFulfillPendingRequests() throws Exception {
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId1 = new SlotID(resourceID, 0);
		final SlotID slotId2 = new SlotID(resourceID, 1);
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		final List<SlotStatus> slotStatus = new ArrayList<>(2);
		slotStatus.add(new SlotStatus(slotId1, ResourceProfile.UNKNOWN, jobId, null, null, 0L));
		slotStatus.add(new SlotStatus(slotId2, ResourceProfile.UNKNOWN, jobId, null, null, 0L));

		final SlotReport slotReport = new SlotReport(slotStatus);

		AllocationID allocationID1 = new AllocationID();
		AllocationID allocationID2 = new AllocationID();

		final SlotRequest slotRequest = new SlotRequest(
			jobId,
			new AllocationID(),
			DEFAULT_TESTING_BIG_PROFILE,
			"localhost");
		final SlotRequest slotRequest1 = new SlotRequest(
			jobId,
			allocationID1,
			DEFAULT_TESTING_PROFILE,
			"localhost");
		final SlotRequest slotRequest2 = new SlotRequest(
			jobId,
			allocationID2,
			DEFAULT_TESTING_PROFILE,
			"localhost");

		final ResourceActions resourceManagerActions = mock(ResourceActions.class);
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			eq(jobId),
			any(AllocationID.class),
			any(ResourceProfile.class),
			anyString(),
			eq(resourceManagerId),
			anyLong(),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(resourceID, taskExecutorGateway);

		try (DynamicAssigningSlotManager slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.setTotalResourceOfTaskExecutor(DEFAULT_TESTING_BIG_PROFILE);

			slotManager.registerSlotRequest(slotRequest);

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			slotManager.registerSlotRequest(slotRequest1);
			slotManager.registerSlotRequest(slotRequest2);

			assertEquals(1, slotManager.getNumberFreeSlots());

			slotManager.freeSlot(slotId1, slotRequest.getAllocationId());

			SlotReport newSlotReport = new SlotReport(new SlotStatus(slotId2, ResourceProfile.UNKNOWN, jobId, null, null, 0L));
			slotManager.reportSlotStatus(taskExecutorConnection.getInstanceID(), newSlotReport);

			assertEquals(0, slotManager.getNumberFreeSlots());
		}
	}

	private DynamicAssigningSlotManager createSlotManager(ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
		DynamicAssigningSlotManager slotManager = new DynamicAssigningSlotManager(
			TestingUtils.defaultScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		slotManager.setTotalResourceOfTaskExecutor(new ResourceProfile(2, 1000));
		slotManager.start(resourceManagerId, Executors.directExecutor(), resourceManagerActions);

		return slotManager;
	}
}
