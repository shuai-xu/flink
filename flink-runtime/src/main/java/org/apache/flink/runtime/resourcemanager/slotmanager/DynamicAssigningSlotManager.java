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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This slot manager is used by yarn session mode. It will ignore the resource for a slot,
 * but will make the total resources for all tasks in the slots of a task manager
 * not exceed the total resource of the task manager.
 */
public class DynamicAssigningSlotManager extends SlotManager {
	private static final Logger LOG = LoggerFactory.getLogger(DynamicAssigningSlotManager.class);

	/**
	 * All allocated slots's resource profile.
	 */
	private final Map<ResourceID, Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile>> allocatedSlotsResource;

	/**
	 * The resource can be used by task in a task manager.
	 */
	private ResourceProfile totalResourceOfTaskExecutor;

	public DynamicAssigningSlotManager(
			ScheduledExecutor scheduledExecutor,
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout,
			Time taskManagerCheckerInitialDelay) {
		super(scheduledExecutor, taskManagerRequestTimeout, slotRequestTimeout, taskManagerTimeout, taskManagerCheckerInitialDelay);
		this.allocatedSlotsResource = new HashMap<>();
		setSlotListener(new SlotListenerImpl());
	}

	@Override
	protected TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
		Random random = new Random();
		List<TaskManagerSlot> resourceSlots = new ArrayList<>(freeSlots.values());
		int count = 0;
		while (count++ < freeSlots.size() / 2) {
			int index = random.nextInt(freeSlots.size());
			TaskManagerSlot slot = resourceSlots.get(index);
			if (hasEnoughResource(slot.getSlotId().getResourceID(), requestResourceProfile)) {
				recordAllocatedSlotAndResource(slot.getSlotId(), requestResourceProfile);
				freeSlots.remove(slot.getSlotId());
				return slot;
			}
		}

		Iterator<Map.Entry<SlotID, TaskManagerSlot>> iterator = freeSlots.entrySet().iterator();
		while (iterator.hasNext()) {
			TaskManagerSlot slot = iterator.next().getValue();
			if (hasEnoughResource(slot.getSlotId().getResourceID(), requestResourceProfile)) {
				recordAllocatedSlotAndResource(slot.getSlotId(), requestResourceProfile);
				freeSlots.remove(slot.getSlotId());
				return slot;
			}
		}
		return null;
	}

	@Override
	protected PendingSlotRequest findMatchingRequest(TaskManagerSlot taskManagerSlot) {
		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			if (!pendingSlotRequest.isAssigned() &&
					hasEnoughResource(taskManagerSlot.getSlotId().getResourceID(), pendingSlotRequest.getResourceProfile())) {
				recordAllocatedSlotAndResource(taskManagerSlot.getSlotId(), pendingSlotRequest.getResourceProfile());
				return pendingSlotRequest;
			}
		}
		return null;
	}

	/**
	 * Set the total resource of a task executor.
	 * @param resourceOfTaskExecutor The available resource for task in a task executor.
	 */
	public void setTotalResourceOfTaskExecutor(ResourceProfile resourceOfTaskExecutor) {
		this.totalResourceOfTaskExecutor = resourceOfTaskExecutor;
	}

	private void recordAllocatedSlotAndResource(SlotID slotID, ResourceProfile resourceProfile) {
		Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile> slotToResource = allocatedSlotsResource.get(slotID.getResourceID());
		if (slotToResource != null) {
			slotToResource.f0.put(slotID, resourceProfile);
			slotToResource.f1 = slotToResource.f1.minus(resourceProfile);
		} else {
			Map<SlotID, ResourceProfile> remain = new HashMap<>();
			remain.put(slotID, resourceProfile);
			slotToResource = new Tuple2<>(remain, totalResourceOfTaskExecutor.minus(resourceProfile));
			allocatedSlotsResource.put(slotID.getResourceID(), slotToResource);
		}
	}

	private boolean hasEnoughResource(ResourceID taskManagerId, ResourceProfile required) {
		Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile> allocatedResources = allocatedSlotsResource.get(taskManagerId);
		ResourceProfile remain = (allocatedResources == null) ? totalResourceOfTaskExecutor : allocatedResources.f1;

		boolean isMatched = remain.isMatching(required);

		if (isMatched && LOG.isDebugEnabled()) {
			LOG.debug("Find matched resource in task manager id {} with remaining resource {} for required resource {}." +
							"The allocated slot resources are {} and all the slots are {}.",
					taskManagerId, remain, required, allocatedResources, slots);
		}

		return isMatched;
	}

	private void removeSlotFromAllocatedResources(SlotID slotId) {
		if (allocatedSlotsResource.containsKey(slotId.getResourceID())) {
			Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile> slotToResource = allocatedSlotsResource.get(slotId.getResourceID());
			ResourceProfile rf = slotToResource.f0.remove(slotId);
			if (rf != null) {
				Map<String, Resource> extendedResources = new HashMap<>();
				for (Map.Entry<String, Resource> extend : slotToResource.f1.getExtendedResources().entrySet()) {
					Resource rfValue = rf.getExtendedResources().get(extend.getKey());
					if (rfValue != null) {
						extendedResources.put(extend.getKey(), extend.getValue().merge(rfValue));
					} else {
						extendedResources.put(extend.getKey(), extend.getValue());
					}
				}
				slotToResource.f1 = new ResourceProfile(slotToResource.f1.getCpuCores() + rf.getCpuCores(),
					slotToResource.f1.getDirectMemoryInMB() + rf.getDirectMemoryInMB(),
					slotToResource.f1.getHeapMemoryInMB() + rf.getHeapMemoryInMB(),
					slotToResource.f1.getNativeMemoryInMB() + rf.getNativeMemoryInMB(),
					slotToResource.f1.getNetworkMemoryInMB() + rf.getNetworkMemoryInMB(),
					extendedResources);
			}
			if (slotToResource.f0.isEmpty()) {
				allocatedSlotsResource.remove(slotId.getResourceID());
			}
		}
	}

	@VisibleForTesting
	public Map<ResourceID, Tuple2<Map<SlotID, ResourceProfile>, ResourceProfile>> getAllocatedSlotsResource() {
		return allocatedSlotsResource;
	}

	/**
	 * Implementation of SlotListener for this slot pool.
	 */
	private class SlotListenerImpl implements SlotListener {

		@Override
		public void notifySlotRegistered(SlotID slotId, ResourceProfile allocationResourceProfile) {
			recordAllocatedSlotAndResource(slotId, allocationResourceProfile);
		}

		@Override
		public void notifySlotFree(SlotID slotId) {
			removeSlotFromAllocatedResources(slotId);
		}

		@Override
		public void notifySlotRemoved(SlotID slotId) {
			removeSlotFromAllocatedResources(slotId);
		}
	}
}
