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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.DynamicAssigningSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN or Mesos.
 *
 * <p>This ResourceManager doesn't acquire new resources.
 */
public class StandaloneResourceManager extends ResourceManager<ResourceID> {

	private final Configuration flinkConfig;

	private final TaskManagerResource taskManagerResource;

	public StandaloneResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler) {
		this(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			new Configuration(),
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler);
	}

	public StandaloneResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration configuration,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler) {
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler);
		flinkConfig = configuration;

		// build the task manager's total resource according to user's resource
		taskManagerResource =
				TaskManagerResource.fromConfiguration(flinkConfig, initContainerResourceConfig(), 1);
		log.info("taskManagerResource: " + taskManagerResource);

		if (slotManager instanceof DynamicAssigningSlotManager) {
			((DynamicAssigningSlotManager) slotManager).setTotalResourceOfTaskExecutor(
					TaskManagerResource.convertToResourceProfile(taskManagerResource));
			log.info("The resource for user in a task executor is {}.", taskManagerResource);
		} else {
			log.warn("DynamicAssigningSlotManager have not been set in StandaloneResourceManager, " +
					"setResources() of operator may not work!");
		}
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		// nothing to initialize
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
	}

	@Override
	public boolean stopWorker(ResourceID resourceID) {
		// standalone resource manager cannot stop workers
		return false;
	}

	@Override
	public void cancelNewWorker(ResourceProfile resourceProfile) {
	}

	@Override
	protected int getNumberAllocatedWorkers() {
		return 0;
	}

	@Override
	protected ResourceID workerStarted(ResourceID resourceID) {
		return resourceID;
	}

	// Utility methods

	private ResourceProfile initContainerResourceConfig() {
		double core = flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_CORE);
		int heapMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
		int nativeMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_NATIVE_MEMORY);
		int directMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_DIRECT_MEMORY);

		int networkBuffersNum = flinkConfig.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);
		long pageSize = flinkConfig.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);
		int networkMemory = (int) Math.ceil((pageSize * networkBuffersNum) / (1024.0 * 1024.0));

		// Add managed memory to extended resources.
		long managedMemory = flinkConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
		Map<String, Resource> resourceMap = new HashMap<>();
		resourceMap.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemory));
		return new ResourceProfile(
				core,
				heapMemory,
				directMemory,
				nativeMemory,
				networkMemory,
				resourceMap);
	}

}
