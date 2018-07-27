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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Describe the resource of a task manager, including the task resource and framework resource.
 * The task resource is represented by the ResourceProfile, and the framework resource including:
 * private memory for the task manager process and netty framework.
 */
public class TaskManagerResource {

	/** The netty memory for TaskManager process. */
	private final int taskManagerNettyMemorySizeMB;

	/** The reserved native memory for TaskManager process. */
	private final int taskManagerNativeMemorySizeMB;

	/** The reserved heap memory for TaskManager process.  */
	private final int taskManagerHeapMemorySizeMB;

	/** The memory type used for NetworkBufferPool and MemoryManager, heap or off-heap. */
	private final boolean offHeap;

	/** The ratio for young generation of persistent memory. */
	private final double persistentYoungHeapRatio;

	/** The ratio for young generation of dynamic memory. */
	private final double dynamicYoungHeapRatio;

	/** The ratio for CMS gc occupy. */
	private final double cmsGCRatio;

	/** The resource profile of running tasks in TaskManager. */
	private ResourceProfile taskResourceProfile;

	/** The number of slots in TaskManager. */
	private final int slotNum;

	/** The memory segment size for network and managed buffers. */
	private final int pageSize;

	protected TaskManagerResource(
		int taskManagerNettyMemorySizeMB,
		int taskManagerNativeMemorySizeMB,
		int taskManagerHeapMemorySizeMB,
		boolean offHeap,
		int pageSize,
		double dynamicYoungHeapRatio,
		double persistentYoungHeapRatio,
		double cmsGCRatio,
		ResourceProfile taskResourceProfile,
		int slotNum) {

		this.taskManagerNettyMemorySizeMB = taskManagerNettyMemorySizeMB;
		this.taskManagerNativeMemorySizeMB = taskManagerNativeMemorySizeMB;
		this.taskManagerHeapMemorySizeMB = taskManagerHeapMemorySizeMB;
		this.offHeap = offHeap;
		this.pageSize = pageSize;
		this.taskResourceProfile = taskResourceProfile;
		this.slotNum = slotNum;
		this.persistentYoungHeapRatio = persistentYoungHeapRatio;
		this.dynamicYoungHeapRatio = dynamicYoungHeapRatio;
		this.cmsGCRatio = cmsGCRatio;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public int getTaskManagerNettyMemorySizeMB() {
		return taskManagerNettyMemorySizeMB;
	}

	public int getTotalContainerMemory() {
		return getTotalHeapMemory() + getTotalDirectMemory() + getTotalNativeMemory();
	}

	public ResourceProfile getTotalResourceProfile() {
		return new ResourceProfile(getContainerCpuCores(), getTotalContainerMemory());
	}

	public int getManagedMemorySize() {
		return taskResourceProfile.getManagedMemoryInMB() * slotNum;
	}

	/**
	 * Get the memory for network buffer pool, it is calculated by job master according to the input and output channel number.
	 * @return the network memory for all tasks in the task executor.
	 */
	public int getNetworkMemorySize() {
		return taskResourceProfile.getNetworkMemoryInMB() * slotNum;
	}

	public int getTotalNativeMemory() {
		return taskResourceProfile.getNativeMemoryInMB() * slotNum + taskManagerNativeMemorySizeMB;
	}

	/**
	 * We separate heap memory into to two parts: dynamic memory and persistent memory.
	 * persistent memory is memory used by buffer pool, which will never be freed once allocated,
	 * and will always in old generation, the memory left is dynamic memory, which will be used for
	 * creating objects which will be created and released frequently, and exists in both old and
	 * new generation.
	 * -----------------------------------------------
	 *             Heap Memory
	 * -----------------------------------------------
	 *      Young          |          OLD
	 * -----------------------------------------------
	 *      extra   |   dynamic    |    persistent
	 * -----------------------------------------------
	 * dynamic memory in young is controlled by dynamicYoungHeapRatio. we add some extra memory to young
	 * to make sure that young generation is larger than persistent * persistentYoungHeapRatio, which make
	 * TaskManager more GC friendly when persistent memory is not pre-allocated.
	 */
	public int getTotalHeapMemory() {
		return getYoungHeapMemory() + getOldHeapMemory();
	}

	public int getTotalDirectMemory() {
		int directMemory = taskManagerNettyMemorySizeMB + taskResourceProfile.getDirectMemoryInMB() * slotNum + getNetworkMemorySize();
		if (offHeap) {
			directMemory += getManagedMemorySize();
		}
		return directMemory;
	}

	int getOldHeapMemory() {
		// dynamic memory in old generation
		int dynamicMemory = getDynamicHeapMemory();
		int result = (int) (dynamicMemory - dynamicMemory * dynamicYoungHeapRatio);

		// persistent memory in old generation
		result += getPersistentHeapMemory();
		return result;
	}

	public int getYoungHeapMemory() {
		int dynamicMemory = getDynamicHeapMemory();
		int persistentMemory = getPersistentHeapMemory();

		// we will ensure the young generation size large than given fraction of persistentMemory in case of we may
		// allocate persistent memory dynamically, which will cause too much young gc.
		return (int) Math.max(dynamicMemory * dynamicYoungHeapRatio, persistentMemory * persistentYoungHeapRatio);
	}

	/**
	 * Persistent Memory means memory in heap which will never be released once allocated, we need to prepare old
	 * generation memory for them only.
	 *
	 * @return size of the persistent memory
	 */
	int getPersistentHeapMemory() {
		int persistentMemory = 0;
		if (!offHeap) {
			persistentMemory += getManagedMemorySize();
		}
		return persistentMemory;
	}

	/**
	 * Dynamic Memory means memory in heap which will be manipulate frequently, and we need to prepare both young
	 * and old generation memory for them.
	 *
	 * @return size of the dynamic memory
	 */
	int getDynamicHeapMemory() {
		return taskResourceProfile.getHeapMemoryInMB() * slotNum + taskManagerHeapMemorySizeMB;
	}

	public int getCMSGCOccupancyFraction() {
		int dynamicMemory = getDynamicHeapMemory();
		int persistentMemory = getPersistentHeapMemory();

		// we want to trigger gc when given fraction of dynamic memory in old generation is used.
		double userDefinedTriggerMemory = dynamicMemory * (1 - dynamicYoungHeapRatio) * cmsGCRatio;
		return (int) ((persistentMemory + userDefinedTriggerMemory) / (getOldHeapMemory()) * 100);
	}

	int getSlotNum() {
		return slotNum;
	}

	ResourceProfile getTaskResourceProfile() {
		return taskResourceProfile;
	}

	public double getContainerCpuCores() {
		return taskResourceProfile.getCpuCores() * slotNum;
	}

	public int getPageSize() {
		return pageSize;
	}

	// --------------------------------------------------------------------------------------------
	//  Parsing configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method to parse the configuration for task manager resource.
	 *
	 * @param configuration The configuration.
	 * @param resourceProfile The resource profile of task in the task manager.
	 * @return TaskManagerResourceProfile
	 */
	public static TaskManagerResource fromConfiguration(
			Configuration configuration,
			ResourceProfile resourceProfile,
			int slotNum) {

		final int taskManagerNettyMemorySizeMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY);
		final int taskManagerNativeMemorySizeMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NATIVE_MEMORY);
		final int taskManagerHeapMemorySizeMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY);

		double dynamicYoungRatio = configuration.getDouble(
				TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO);
		double persistentYoungRatio = configuration.getDouble(
				TaskManagerOptions.TASK_MANAGER_MEMORY_PERSISTENT_YOUNG_RATIO);
		double cmsGCRatio = configuration.getDouble(
				TaskManagerOptions.TASK_MANAGER_MEMORY_CMS_GC_RATIO);

		// check whether we use heap or off-heap memory
		final boolean useOffHeap = configuration.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP);

		final int pageSize = configuration.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);

		return new TaskManagerResource(
				taskManagerNettyMemorySizeMB,
				taskManagerNativeMemorySizeMB,
				taskManagerHeapMemorySizeMB,
				useOffHeap,
				pageSize,
				dynamicYoungRatio,
				persistentYoungRatio,
				cmsGCRatio,
				resourceProfile,
				slotNum);
	}

	/**
	 * Utility method to convert from TaskManagerResource to ResourceProfile.
	 *
	 * @param taskManagerResource The input resource configuration.
	 * @return ResourceProfile equivalent to taskManagerResource.
	 */
	public static ResourceProfile convertToResourceProfile(TaskManagerResource taskManagerResource) {
		Map<String, Resource> extendedResources = new HashMap<>();
		extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME,
			new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, taskManagerResource.getManagedMemorySize()));
		return new ResourceProfile(taskManagerResource.getTaskResourceProfile().getCpuCores(),
			taskManagerResource.getTaskResourceProfile().getHeapMemoryInMB(),
			taskManagerResource.getTaskResourceProfile().getDirectMemoryInMB(),
			taskManagerResource.getTaskResourceProfile().getNativeMemoryInMB(),
			taskManagerResource.getNetworkMemorySize(),
			extendedResources);

	}

	@Override
	public int hashCode() {
		return Objects.hash(
				taskManagerNettyMemorySizeMB, offHeap, taskManagerNativeMemorySizeMB, taskManagerHeapMemorySizeMB,
				dynamicYoungHeapRatio, persistentYoungHeapRatio, cmsGCRatio, taskResourceProfile, slotNum);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == TaskManagerResource.class) {
			TaskManagerResource that = (TaskManagerResource) obj;
			return this.taskResourceProfile.equals(that.taskResourceProfile) &&
					this.taskManagerNettyMemorySizeMB == that.taskManagerNettyMemorySizeMB &&
					this.taskManagerNativeMemorySizeMB == that.taskManagerNativeMemorySizeMB &&
					this.taskManagerHeapMemorySizeMB == that.taskManagerHeapMemorySizeMB &&
					this.offHeap == that.offHeap &&
					this.slotNum == that.slotNum &&
					this.cmsGCRatio == that.cmsGCRatio &&
					this.dynamicYoungHeapRatio == that.dynamicYoungHeapRatio &&
					this.persistentYoungHeapRatio == that.persistentYoungHeapRatio;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "TaskManagerResourceProfile {" +
				"TaskResourceProfile=" + taskResourceProfile +
				", taskManagerNettyMemoryInMB=" + taskManagerNettyMemorySizeMB +
				", taskManagerNativeMemorySizeMB=" + taskManagerNativeMemorySizeMB +
				", taskManagerHeapMemorySizeMB=" + taskManagerHeapMemorySizeMB +
				", dynamicYoungRatio=" + dynamicYoungHeapRatio +
				", persistentYoungRatio=" + persistentYoungHeapRatio +
				", cmsGCRatio=" + cmsGCRatio +
				", offHeap=" + offHeap +
				", slotNum=" + slotNum +
				'}';
	}

}
