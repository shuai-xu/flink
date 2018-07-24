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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

/**
 * Test the computation of resources in TaskManagerResource.
 */
public class TaskManagerResourceTest {

	private static final int TM_NETTY_MEMORY_MB = 13;
	private static final int TM_NATIVE_MEMORY_MB = 15;
	private static final int TM_HEAP_MEMORY_MB = 17;
	private static final int NUM_SLOTS = 2;

	private static final double CORE = 1.0;
	private static final int HEAP_MEMORY_MB = 10;
	private static final int DIRECT_MEMORY_MB = 100;
	private static final int NATIVE_MEMORY_MB = 1000;

	@Test
	public void testBasicMemories() {
		final int managedMemoryInMB = 0;
		final int networkMemoryInMB = 0;

		Map<String, Resource> extendedResources = new HashMap<>();
		extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
		final ResourceProfile taskResourceProfile = new ResourceProfile(CORE, HEAP_MEMORY_MB,
			DIRECT_MEMORY_MB, NATIVE_MEMORY_MB, networkMemoryInMB, extendedResources);

		final Configuration config = initializeConfiguration();
		final TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);

		assertEquals(TM_HEAP_MEMORY_MB + (HEAP_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalHeapMemory());
		assertEquals(DIRECT_MEMORY_MB * NUM_SLOTS + TM_NETTY_MEMORY_MB, tmResource.getTotalDirectMemory());
		assertEquals(TM_NATIVE_MEMORY_MB + TM_HEAP_MEMORY_MB + TM_NETTY_MEMORY_MB
			+ (HEAP_MEMORY_MB + DIRECT_MEMORY_MB + NATIVE_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalContainerMemory());
		assertEquals(taskResourceProfile.getManagedMemoryInMB() * NUM_SLOTS, tmResource.getManagedMemorySize());
		assertEquals(taskResourceProfile.getNetworkMemoryInMB() * NUM_SLOTS, tmResource.getNetworkMemorySize());
	}

	@Test
	public void testAllMemories() {
		final int managedMemoryInMB = 10000;
		final int networkMemoryInMB = 100000;

		Map<String, Resource> extendedResources = new HashMap<>();
		extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
		ResourceProfile taskResourceProfile = new ResourceProfile(CORE, HEAP_MEMORY_MB,
			DIRECT_MEMORY_MB, NATIVE_MEMORY_MB, networkMemoryInMB, extendedResources);

		final Configuration config = initializeConfiguration();
		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);
		final TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);

		assertEquals(TM_HEAP_MEMORY_MB + (HEAP_MEMORY_MB + managedMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalHeapMemory());
		assertEquals((DIRECT_MEMORY_MB + networkMemoryInMB) * NUM_SLOTS + TM_NETTY_MEMORY_MB, tmResource.getTotalDirectMemory());
		assertEquals(TM_NATIVE_MEMORY_MB + TM_HEAP_MEMORY_MB + TM_NETTY_MEMORY_MB
			+ (HEAP_MEMORY_MB + DIRECT_MEMORY_MB + NATIVE_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalContainerMemory());
		assertEquals(taskResourceProfile.getManagedMemoryInMB() * NUM_SLOTS, tmResource.getManagedMemorySize());
		assertEquals(taskResourceProfile.getNetworkMemoryInMB() * NUM_SLOTS, tmResource.getNetworkMemorySize());

		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, true);
		final TaskManagerResource tmResourceOffHeap = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);

		assertEquals(TM_HEAP_MEMORY_MB + HEAP_MEMORY_MB * NUM_SLOTS,
			tmResourceOffHeap.getTotalHeapMemory());
		assertEquals((DIRECT_MEMORY_MB + networkMemoryInMB + managedMemoryInMB) * NUM_SLOTS + TM_NETTY_MEMORY_MB,
			tmResourceOffHeap.getTotalDirectMemory());
		assertEquals(TM_NATIVE_MEMORY_MB + TM_HEAP_MEMORY_MB + TM_NETTY_MEMORY_MB
			+ (HEAP_MEMORY_MB + DIRECT_MEMORY_MB + NATIVE_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResourceOffHeap.getTotalContainerMemory());
		assertEquals(taskResourceProfile.getManagedMemoryInMB() * NUM_SLOTS, tmResourceOffHeap.getManagedMemorySize());
		assertEquals(taskResourceProfile.getNetworkMemoryInMB() * NUM_SLOTS, tmResourceOffHeap.getNetworkMemorySize());
	}

	@Test
	public void testCMSOccupyFraction() {
		int userHeapSize = 1024;
		final int userNetworkMemorySize = 128;
		final int userManagedMemorySize = 1024;

		Map<String, Resource> extendedResources = new HashMap<>();
		extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, userManagedMemorySize));
		ResourceProfile resourceProfile = new ResourceProfile(0.07, userHeapSize, 0, 0, userNetworkMemorySize, extendedResources);

		Configuration configuration = new Configuration();
		configuration.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);

		TaskManagerResource tmResourceProfile = TaskManagerResource.fromConfiguration(
				configuration,
				resourceProfile,
				NUM_SLOTS
		);

		final int tmHeapSize = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY);
		final double gcOccupyRatio = configuration.getDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_CMS_GC_RATIO);
		final double dynamicYoungGenRatio = configuration.getDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO);
		double persistentYoungGenRatio = configuration.getDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_PERSISTENT_YOUNG_RATIO);

		int dynamicHeapSize = userHeapSize * NUM_SLOTS +  tmHeapSize;
		int persistentHeapSize = userManagedMemorySize * NUM_SLOTS;
		// MAX(dynamicMem * NUM_SLOTS * dynamicYoungGenRatio, persistentMem * persistentYoungGenRatio)
		// Dynamic young gen would be larger than persistent young gen.
		int expectedYoungGenHeapSize = (int) (dynamicHeapSize * dynamicYoungGenRatio);

		assertEquals(expectedYoungGenHeapSize, tmResourceProfile.getYoungHeapMemory());
		// (dynamic mem * 0.75 * gcOccupyRatio + persistent mem)/(dynamic mem * 0.75 + persistent mem)
		assertEquals(
				(int) ((dynamicHeapSize * (1 - dynamicYoungGenRatio) * gcOccupyRatio + persistentHeapSize)
					/ (dynamicHeapSize * (1 - dynamicYoungGenRatio) + persistentHeapSize) * 100),
				tmResourceProfile.getCMSGCOccupancyFraction());

		// Reduce userHeapSize.
		userHeapSize = 512;
		resourceProfile = new ResourceProfile(0.07, userHeapSize, 0, 0, userNetworkMemorySize, extendedResources);
		persistentYoungGenRatio = 0.5;
		configuration.setDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_PERSISTENT_YOUNG_RATIO, persistentYoungGenRatio);
		tmResourceProfile = TaskManagerResource.fromConfiguration(
			configuration,
			resourceProfile,
			NUM_SLOTS
		);

		// persistent mem * 0.5
		persistentHeapSize = userManagedMemorySize * NUM_SLOTS;
		dynamicHeapSize = userHeapSize * NUM_SLOTS +  tmHeapSize;
		// Persistent young gen would be larger than dynamic young gen
		expectedYoungGenHeapSize = (int) (persistentHeapSize * persistentYoungGenRatio);
		assertEquals(expectedYoungGenHeapSize, tmResourceProfile.getYoungHeapMemory());
		// (dynamic mem * 0.75 * gcOccupyRatio + persistent mem)/(dynamic mem * 0.75 + persistent mem)

		assertEquals(dynamicHeapSize, tmResourceProfile.getDynamicHeapMemory());
		assertEquals(persistentHeapSize, tmResourceProfile.getPersistentHeapMemory());

		assertEquals((int) (((persistentHeapSize + gcOccupyRatio * dynamicHeapSize * (1 - dynamicYoungGenRatio)) /
				(dynamicHeapSize * (1 - dynamicYoungGenRatio) + persistentHeapSize)) * 100),
			tmResourceProfile.getCMSGCOccupancyFraction());
	}

	private Configuration initializeConfiguration() {
		final Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NATIVE_MEMORY, TM_NATIVE_MEMORY_MB);
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY, TM_HEAP_MEMORY_MB);
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, TM_NETTY_MEMORY_MB);
		config.setDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO, 0);
		config.setDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_PERSISTENT_YOUNG_RATIO, 0);
		config.setDouble(TaskManagerOptions.TASK_MANAGER_MEMORY_CMS_GC_RATIO, 1);
		return config;
	}
}
