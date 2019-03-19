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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.IllegalExecutionStateException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A slot allocator which will try its best to allocate slots.
 * And when failed to allocate all slots, it will assign the returned slot to executions and notice the caller.
 */
public class BestEffortExecutionSlotAllocator implements ExecutionSlotAllocator{

	static final Logger LOG = LoggerFactory.getLogger(BestEffortExecutionSlotAllocator.class);

	private final SlotProvider slotProvider;

	private final boolean allowQueuedScheduling;

	private final Time allocationTimeout;

	public BestEffortExecutionSlotAllocator(
			SlotProvider slotProvider,
			boolean allowQueuedScheduling,
			Time allocationTimeout){
		this.slotProvider = slotProvider;
		this.allowQueuedScheduling = allowQueuedScheduling;
		this.allocationTimeout = allocationTimeout;
	}

	@Override
	public CompletableFuture<Collection<LogicalSlot>> allocateSlotsFor(Collection<Execution> executions) {

		// Important: reserve all the space we need up front.
		// that way we do not have any operation that can fail between allocating the slots
		// and adding them to the list. If we had a failure in between there, that would
		// cause the slots to get lost
		final boolean queued = allowQueuedScheduling;

		List<SlotRequestId> slotRequestIds = new ArrayList<>(executions.size());
		List<ScheduledUnit> scheduledUnits = new ArrayList<>(executions.size());
		List<SlotProfile> slotProfiles = new ArrayList<>(executions.size());
		List<Execution> scheduledExecutions = new ArrayList<>(executions.size());

		for (Execution exec : executions) {
			try {
				Tuple2<ScheduledUnit, SlotProfile> scheduleUnitAndSlotProfile = exec.enterScheduledAndPrepareSchedulingResources();
				slotRequestIds.add(new SlotRequestId());
				scheduledUnits.add(scheduleUnitAndSlotProfile.f0);
				slotProfiles.add(scheduleUnitAndSlotProfile.f1);
				scheduledExecutions.add(exec);
			} catch (IllegalExecutionStateException e) {
				LOG.info("The execution {} may be already scheduled by other thread.", exec.getVertex().getTaskNameWithSubtaskIndex(), e);
			}
		}

		if (slotRequestIds.isEmpty()) {
			return CompletableFuture.completedFuture(null);
		}

		List<CompletableFuture<LogicalSlot>> allocationFutures =
				slotProvider.allocateSlots(slotRequestIds, scheduledUnits, queued, slotProfiles, allocationTimeout);
		for (int i = 0; i < allocationFutures.size(); i++) {
			final int index = i;
			allocationFutures.get(i).whenComplete(
					(ignore, throwable) -> {
						if (throwable != null) {
							slotProvider.cancelSlotRequest(
									slotRequestIds.get(index),
									scheduledUnits.get(index).getSlotSharingGroupId(),
									scheduledUnits.get(index).getCoLocationConstraint(),
									throwable);
						}
					}
			);
		}
		// this future is complete once all slot futures are complete.
		// the future fails once one slot future fails.
		final FutureUtils.ConjunctFuture<Collection<LogicalSlot>> allAllocationFutures = FutureUtils.combineAllInOrder(allocationFutures);

		CompletableFuture<Collection<LogicalSlot>> returnFuture = allAllocationFutures
				.handle(
						(slots, throwable) -> {
							if (throwable != null) {
								LOG.info("Batch request {} slots, but only {} are fulfilled.",
										allAllocationFutures.getNumFuturesTotal(), allAllocationFutures.getNumFuturesCompleted());

								// Complete all futures first, or else the execution fail may cause global failover,
								// and other executions may fail first without clear the pending request.
								List<LogicalSlot> returnSlots = new ArrayList<>(allocationFutures.size());
								for (int i = 0; i < allocationFutures.size(); i++) {
									if (allocationFutures.get(i).completeExceptionally(throwable)) {
										returnSlots.add(i, null);
									} else {
										try {
											returnSlots.add(i, allocationFutures.get(i).get());
										} catch (Exception e) {
											returnSlots.add(i, null);
										}
									}
								}
								return returnSlots;
							} else {
								return slots;
							}
						}
				);
		returnFuture.exceptionally(
				throwable -> {
					if (throwable instanceof CancellationException) {
						for (int i = 0; i < allocationFutures.size(); i++) {
							if (!allocationFutures.get(i).completeExceptionally(throwable)) {
								try {
									LogicalSlot slot = allocationFutures.get(i).get();
									if (slot != null) {
										slot.releaseSlot(throwable);
									}
								} catch (Exception e) {
								}
							}
						}
						return null;
					} else {
						throw new CompletionException(throwable);
					}
				});
		return returnFuture;
	}
}
