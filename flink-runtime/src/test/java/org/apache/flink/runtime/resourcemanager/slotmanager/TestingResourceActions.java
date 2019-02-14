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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.placementconstraint.SlotTag;

import javax.annotation.Nonnull;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Testing implementation of the {@link ResourceActions}.
 */
public class TestingResourceActions implements ResourceActions {

	@Nonnull
	private final BiConsumer<InstanceID, Exception> releaseResourceConsumer;

	@Nonnull
	private final Consumer<ResourceProfile> allocateResourceConsumer;

	@Nonnull
	private final Consumer<Tuple3<JobID, AllocationID, Exception>> notifyAllocationFailureConsumer;

	public TestingResourceActions(
			@Nonnull BiConsumer<InstanceID, Exception> releaseResourceConsumer,
			@Nonnull Consumer<ResourceProfile> allocateResourceConsumer,
			@Nonnull Consumer<Tuple3<JobID, AllocationID, Exception>> notifyAllocationFailureConsumer) {
		this.releaseResourceConsumer = releaseResourceConsumer;
		this.allocateResourceConsumer = allocateResourceConsumer;
		this.notifyAllocationFailureConsumer = notifyAllocationFailureConsumer;
	}


	@Override
	public void releaseResource(InstanceID instanceId, Exception cause) {
		releaseResourceConsumer.accept(instanceId, cause);
	}

	@Override
	public void allocateResource(ResourceProfile resourceProfile) {
		allocateResourceConsumer.accept(resourceProfile);
	}

	@Override
	public void allocateResource(ResourceProfile resourceProfile, Set<SlotTag> tags) {
	}

	@Override
	public void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause) {
		notifyAllocationFailureConsumer.accept(Tuple3.of(jobId, allocationId, cause));
	}

	@Override
	public void cancelResourceAllocation(ResourceProfile resourceProfile) {
	}

	@Override
	public void cancelResourceAllocation(ResourceProfile resourceProfile, Set<SlotTag> tags) {
	}
}
