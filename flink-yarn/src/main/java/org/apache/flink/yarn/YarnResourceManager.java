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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceConstraints;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.placementconstraint.SlotTag;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ResourceManager<YarnWorkerNode> implements AMRMClientAsync.CallbackHandler {

	/** The process environment variables. */
	private final Map<String, String> env;

	/** YARN container map. Package private for unit test purposes. */
	private final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;

	/**
	 * The default registration timeout for task executor in seconds.
	 */
	private static final int DEFAULT_TASK_MANAGER_REGISTRATION_DURATION = 300;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/** The min cpu core of a task executor, used to decide how many slots can be placed on a task executor. */
	private final double minCorePerContainer;

	/** The min memory of task executor to allocate (in MB), used to decide how many slots can be placed on a task executor. */
	private final int minMemoryPerContainer;

	/** The max cpu core of a task executor, used to decide how many slots can be placed on a task executor. */
	private final double maxCorePerContainer;

	/** The max memory of a task executor, used to decide how many slots can be placed on a task executor. */
	private final int maxMemoryPerContainer;

	/** The min extended resource of a task executor, used to decide how many slots can be placed on a task executor. */
	private final Map<String, Double> minExtendedResourcePerContainer;

	/** The max extended resource of a task executor, used to decide how many slots can be placed on a task executor. */
	private final Map<String, Double> maxExtendedResourcePerContainer;

	/** Yarn vcore ratio, how many virtual cores will use a physical core.  */
	private final double yarnVcoreRatio;

	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	private static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final YarnConfiguration yarnConfig;

	private final Time containerRegisterTimeout;

	@Nullable
	private final String webInterfaceUrl;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Request adapter to communicate with Resource Manager (YARN's master) with special version. */
	private RequestAdapter requestAdapter;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClient nodeManagerClient;

	/** The number of containers requested for each priority, but not yet granted.
	 *  Currently we use priority to identity a typical type of resource.
	 **/
	private final ConcurrentHashMap<Integer, AtomicInteger> numPendingContainerRequests;

	private final Map<Tuple2<TaskManagerResource, Set<SlotTag>>, Integer> resourceAndTagsToPriorityMap = new HashMap<>();

	private final Map<Integer, Tuple2<TaskManagerResource, Set<SlotTag>>> priorityToResourceAndTagsMap = new HashMap<>();

	/**
	 * The number of slots not used by any request.
	 */
	private final Map<Integer, Integer> priorityToSpareSlots;

	/**
	 * Number of startNewWorker calls blocked due to exceeding max resource limit.
	 */
	private final Map<Integer, Integer> priorityToBlockedWorkers;

	private final Object spareSlotsAndBlockedWorkersLock = new Object();

	/**
	 * executor for start yarn container.
	 */
	@VisibleForTesting
	protected Executor executor;

	private volatile int latestPriority = 0;

	public YarnResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			Map<String, String> env,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String webInterfaceUrl) {
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
		this.flinkConfig  = flinkConfig;
		this.yarnConfig = new YarnConfiguration();
		this.env = env;
		this.workerNodeMap = new ConcurrentHashMap<>();
		final int yarnHeartbeatIntervalMS = flinkConfig.getInteger(
				YarnConfigOptions.HEARTBEAT_DELAY_SECONDS) * 1000;

		final long yarnExpiryIntervalMS = yarnConfig.getLong(
				YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
				YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);

		if (yarnHeartbeatIntervalMS >= yarnExpiryIntervalMS) {
			log.warn("The heartbeat interval of the Flink Application master ({}) is greater " +
					"than YARN's expiry interval ({}). The application is likely to be killed by YARN.",
					yarnHeartbeatIntervalMS, yarnExpiryIntervalMS);
		}
		yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMS;

		numPendingContainerRequests = new ConcurrentHashMap<>();
		priorityToSpareSlots = new HashMap<>();
		priorityToBlockedWorkers = new HashMap<>();

		containerRegisterTimeout = Time.seconds(flinkConfig.getLong(YarnConfigOptions.CONTAINER_REGISTER_TIMEOUT));

		this.executor = new ThreadPoolExecutor(0, flinkConfig.getInteger(YarnConfigOptions.CONTAINER_LAUNCHER_NUMBER),
			60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
			.setNameFormat("ContainerLauncher #%d")
			.build());

		this.yarnVcoreRatio = flinkConfig.getInteger(YarnConfigOptions.YARN_VCORE_RATIO);

		this.minCorePerContainer = Math.max(flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MIN_CORE),
			yarnConfig.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES) / yarnVcoreRatio);

		this.minMemoryPerContainer = Math.max(flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MIN_MEMORY),
			yarnConfig.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));

		this.maxCorePerContainer = Math.min(flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_CORE),
			yarnConfig.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES) / yarnVcoreRatio);

		this.maxMemoryPerContainer = Math.min(flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_MEMORY),
			yarnConfig.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));

		this.minExtendedResourcePerContainer = loadExtendedResourceConstrains(flinkConfig, true);
		this.maxExtendedResourcePerContainer = loadExtendedResourceConstrains(flinkConfig, false);

		this.webInterfaceUrl = webInterfaceUrl;
	}

	protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
			YarnConfiguration yarnConfiguration,
			int yarnHeartbeatIntervalMillis,
			@Nullable String webInterfaceUrl) throws Exception {
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(
			yarnHeartbeatIntervalMillis,
			this);

		resourceManagerClient.init(yarnConfiguration);
		resourceManagerClient.start();

		//TODO: change akka address to tcp host and port, the getAddress() interface should return a standard tcp address
		Tuple2<String, Integer> hostPort = parseHostPort(getAddress());

		final int restPort;

		if (webInterfaceUrl != null) {
			final int lastColon = webInterfaceUrl.lastIndexOf(':');

			if (lastColon == -1) {
				restPort = -1;
			} else {
				restPort = Integer.valueOf(webInterfaceUrl.substring(lastColon + 1));
			}
		} else {
			restPort = -1;
		}

		final RegisterApplicationMasterResponse registerApplicationMasterResponse =
			resourceManagerClient.registerApplicationMaster(hostPort.f0, restPort, webInterfaceUrl);
		getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		return resourceManagerClient;
	}

	private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			new RegisterApplicationMasterResponseReflector(log).getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		for (final Container container : containersFromPreviousAttempts) {
			workerNodeMap.put(new ResourceID(container.getId().toString()), new YarnWorkerNode(container));
			scheduleRunAsync(() -> checkContainersRegistered(new ResourceID(container.getId().toString())), containerRegisterTimeout);
			if (container.getPriority().getPriority() >= latestPriority) {
				// The priority for previous master will not be used.
				latestPriority = container.getPriority().getPriority() + 1;
			}
		}
		// TODO: In a rare condition, application master may not get full container list after it fail over.
		// Thus we may not get the right previous latestPriority once previously unreported containers reconnect RM.
	}

	protected NMClient createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		NMClient nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(false);
		return nodeManagerClient;
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		try {
			resourceManagerClient = createAndStartResourceManagerClient(
				yarnConfig,
				yarnHeartbeatIntervalMillis,
				webInterfaceUrl);
			requestAdapter = Utils.getRequestAdapter(flinkConfig, resourceManagerClient);
			log.info("Using request adapter: " + requestAdapter.getClass().getSimpleName());
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}

		nodeManagerClient = createAndStartNodeManagerClient(yarnConfig);

		try {
			String curDir = env.get(ApplicationConstants.Environment.PWD.key());
			Utils.uploadTaskManagerConf(flinkConfig, yarnConfig, env, curDir);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not upload TaskManager config file.", e);
		}
	}

	@Override
	public CompletableFuture<Void> postStop() {
		// shut down all components
		Throwable firstException = null;

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Throwable t) {
				firstException = t;
			}
		}

		if (nodeManagerClient != null) {
			try {
				nodeManagerClient.stop();
			} catch (Throwable t) {
				firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
			}
		}

		final CompletableFuture<Void> terminationFuture = super.postStop();

		if (firstException != null) {
			return FutureUtils.completedExceptionally(new FlinkException("Error while shutting down YARN resource manager", firstException));
		} else {
			return terminationFuture;
		}
	}

	@Override
	protected void internalDeregisterApplication(
			ApplicationStatus finalStatus,
			@Nullable String diagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, diagnostics, "");
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}

		Utils.deleteApplicationFiles(env);
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		startNewWorker(resourceProfile, Collections.emptySet());
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile, Set<SlotTag> tags) {
		// Priority for worker containers - priorities are intra-application
		int slotNumber = calculateSlotNumber(resourceProfile);
		TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(flinkConfig, resourceProfile, slotNumber);
		int priority = generatePriority(tmResource, tags);
		Resource containerResource = generateContainerResource(tmResource);

		boolean requestNewContainer = false;
		synchronized (spareSlotsAndBlockedWorkersLock) {
			int spareSlots = priorityToSpareSlots.getOrDefault(priority, 0);
			if (spareSlots > 0) {
				priorityToSpareSlots.put(priority, spareSlots - 1);
			} else {
				String errMsg = String.format(
					"Container trying to request with priority %s exceeds total resource limit, give up requesting.",
					priority);
				if (checkAllocateNewResourceExceedTotalResourceLimit(
					containerResource.getVirtualCores() / yarnVcoreRatio, containerResource.getMemory(), errMsg, true)) {
					int num = 1;
					if (priorityToBlockedWorkers.containsKey(priority)) {
						num = priorityToBlockedWorkers.get(priority) + 1;
					}
					priorityToBlockedWorkers.put(priority, num);
					slotManager.enableIdleTaskManagersFastTimeout();
					return;
				}

				if (slotNumber > 1) {
					priorityToSpareSlots.put(priority, slotNumber - 1);
				}
				requestNewContainer = true;
			}
		}

		if (requestNewContainer) {
			requestYarnContainer(containerResource, Priority.newInstance(priority),
				tmResource.getTaskResourceProfile().getResourceConstraints());
		}
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode) {
		if (workerNode != null) {
			final Container container = workerNode.getContainer();
			log.info("Stopping container {}.", container.getId());
			// Don't use nodeManagerClient.stopContainer in order to make stopping worker faster.
			resourceManagerClient.releaseAssignedContainer(container.getId());
			if (workerNodeMap.remove(workerNode.getResourceID()) != null) {
				tryStartBlockedWorkers();
				return true;
			}
		} else {
			log.info("Can not find container with resource ID {}.", workerNode.getResourceID());
		}
		return false;
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	@VisibleForTesting
	void checkContainersRegistered(ResourceID containerId) {
		YarnWorkerNode node = workerNodeMap.get(containerId);
		if (node != null && !taskExecutorRegistered(containerId)) {
			log.info("Container {} did not register in {}, will stop it and request a new one if needed.", containerId, containerRegisterTimeout);
			if (stopWorker(node)) {
				Priority priority = node.getContainer().getPriority();
				requestYarnContainer(getOrigContainerResource(priority.getPriority()), priority,
					getResourceConstraints(priority.getPriority()));
			}
		}
	}

	@Override
	public void cancelNewWorker(ResourceProfile resourceProfile) {
		cancelNewWorker(resourceProfile, Collections.emptySet());
	}

	@Override
	public void cancelNewWorker(ResourceProfile resourceProfile, Set<SlotTag> tags) {
		int slotNumber = calculateSlotNumber(resourceProfile);
		TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(flinkConfig, resourceProfile, slotNumber);
		int priority = generatePriority(tmResource, tags);
		Resource containerResource = generateContainerResource(tmResource);

		synchronized (spareSlotsAndBlockedWorkersLock) {
			if (priorityToBlockedWorkers.containsKey(priority) && priorityToBlockedWorkers.get(priority) > 0) {
				int blockedNum = priorityToBlockedWorkers.get(priority) - 1;
				if (blockedNum > 0) {
					priorityToBlockedWorkers.put(priority, blockedNum);
				} else {
					priorityToBlockedWorkers.remove(priority);
				}
				return;
			}
		}

		AtomicInteger pendingNumber = numPendingContainerRequests.get(priority);
		if (pendingNumber == null) {
			log.error("There is no previous allocation with id {} for {}.", priority, resourceProfile);
		} else if (pendingNumber.get() > 0) {
			// update the pending request number
			if (slotNumber == 1) {
				// if one container has one slot, just decrease the pending number
				pendingNumber.decrementAndGet();
				requestAdapter.removeRequest(containerResource, Priority.newInstance(priority),
					pendingNumber.get(), getResourceConstraints(priority));
			} else {
				Integer spareSlots = priorityToSpareSlots.get(priority);
				// if spare slots not fulfill a container, add one to the spare number, else decrease the pendign number
				if (spareSlots == null) {
					priorityToSpareSlots.put(priority, 1);
				} else if (spareSlots < slotNumber - 1) {
					priorityToSpareSlots.put(priority, spareSlots + 1);
				} else {
					priorityToSpareSlots.remove(priority);
					pendingNumber.decrementAndGet();
					requestAdapter.removeRequest(containerResource, Priority.newInstance(priority),
						pendingNumber.get(), getResourceConstraints(priority));
				}
			}
		}
	}

	@Override
	protected int getNumberAllocatedWorkers() {
		return workerNodeMap.size();
	}

	// ------------------------------------------------------------------------
	//  AMRMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------

	@Override
	public float getProgress() {
		// Temporarily need not record the total size of asked and allocated containers
		return 1;
	}

	@Override
	public void onContainersCompleted(final List<ContainerStatus> list) {
		runAsync(() -> {
				for (final ContainerStatus containerStatus : list) {
					log.info("Container {} finished with exit code {}", containerStatus.getContainerId(),
						containerStatus.getExitStatus());

					final ResourceID resourceId = new ResourceID(containerStatus.getContainerId().toString());
					final YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);

					Exception exception = new Exception(containerStatus.getDiagnostics());
					taskManagerExceptions.put(System.currentTimeMillis(), new Tuple2<>(resourceId, exception));
					boolean registered = closeTaskManagerConnection(resourceId, exception);
					// We only request new container for it when the container has not register to the RM as otherwise
					// the job master will ask for it when failover.
					if (!registered && yarnWorkerNode != null) {
						if (priorityToResourceAndTagsMap.containsKey(yarnWorkerNode.getContainer().getPriority().getPriority())) {
							// Container completed unexpectedly ~> start a new one
							final Container container = yarnWorkerNode.getContainer();
							internalRequestYarnContainer(
								getOrigContainerResource(yarnWorkerNode.getContainer().getPriority().getPriority()),
								yarnWorkerNode.getContainer().getPriority());
						} else {
							log.info("Not found resource for priority {}, this is usually due to job master failover.",
								yarnWorkerNode.getContainer().getPriority().getPriority());
						}
					} else if (yarnWorkerNode != null) {
						tryStartBlockedWorkers();
					}
				}
			}
		);
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		runAsync(() -> {
			for (Container container : containers) {
				int priority = container.getPriority().getPriority();
				AtomicInteger pendingNumber = numPendingContainerRequests.get(priority);

				log.info(
					"Received new container: {} - priority {}. Remaining pending container requests: {}",
					container.getId(),
					priority,
					pendingNumber != null ? pendingNumber.get() : 0);

				if (pendingNumber != null && pendingNumber.get() > 0) {
					pendingNumber.decrementAndGet();
					requestAdapter.removeRequest(getOrigContainerResource(priority),
						Priority.newInstance(priority), pendingNumber.get(), getResourceConstraints(priority));

					String errMsg = String.format("Container allocated with id %s exceed total resource limit, releasing container.", container.getId());
					if (checkAllocateNewResourceExceedTotalResourceLimit(
						container.getResource().getVirtualCores() / yarnVcoreRatio,
						container.getResource().getMemory(),
						errMsg,
						false)) {
						resourceManagerClient.releaseAssignedContainer(container.getId());
						synchronized (spareSlotsAndBlockedWorkersLock) {
							int slotNum = priorityToResourceAndTagsMap.get(priority).f0.getSlotNum();
							int spareSlot = priorityToSpareSlots.get(priority);
							if (spareSlot >= slotNum) {
								priorityToSpareSlots.put(priority, spareSlot - slotNum);
							} else {
								priorityToSpareSlots.put(priority, 0);
								priorityToBlockedWorkers.put(priority,
									priorityToBlockedWorkers.getOrDefault(priority, 0) + slotNum - spareSlot);
							}
						}
						continue;
					}

					if (pendingNumber.get() == 0) {
						priorityToSpareSlots.put(priority, 0);
					}

					final String containerIdStr = container.getId().toString();
					final ResourceID resourceId = new ResourceID(containerIdStr);

					workerNodeMap.put(resourceId, new YarnWorkerNode(container));

					scheduleRunAsync(() -> checkContainersRegistered(resourceId), containerRegisterTimeout);

					executor.execute(new Runnable() {
						@Override
						public void run() {
							if (workerNodeMap.get(resourceId) == null) {
								log.info("Skip launching container {}, container doesn't exist in container worker map",
									containerIdStr);
								return;
							}
							try {
								// Context information used to start a TaskExecutor Java process
								ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(container);
								nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
							} catch (Throwable t) {
								// failed to launch the container, will release the failed one and ask for a new one
								log.error("Could not start TaskManager in container {},", container, t);
								resourceManagerClient.releaseAssignedContainer(container.getId());
								if (workerNodeMap.remove(resourceId) != null) {
									requestYarnContainer(
										getOrigContainerResource(container.getPriority().getPriority()),
										container.getPriority(),
										getResourceConstraints(container.getPriority().getPriority()));
								} else {
									log.info("The container {} has already been stopped.", container);
								}
							}
						}
					});
				} else {
					// return the excessive containers
					log.info("Returning excess container {}.", container.getId());
					resourceManagerClient.releaseAssignedContainer(container.getId());
				}
			}

			// if we are waiting for no further containers, we can go to the
			// regular heartbeat interval
			int pendingRequest = 0;
			for (AtomicInteger num : numPendingContainerRequests.values()) {
				pendingRequest += num.get();
			}
			if (pendingRequest == 0) {
				resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
			}
		});
	}

	@Override
	public void onShutdownRequest() {
		shutDown();
	}

	@Override
	public void onNodesUpdated(List<NodeReport> list) {
		// We are not interested in node updates
	}

	@Override
	public void onError(Throwable error) {
		onFatalError(error);
	}

	// ------------------------------------------------------------------------
	//  Utility methods
	// ------------------------------------------------------------------------

	/**
	 * Converts a Flink application status enum to a YARN application status enum.
	 * @param status The Flink application status.
	 * @return The corresponding YARN application status.
	 */
	private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
		if (status == null) {
			return FinalApplicationStatus.UNDEFINED;
		}
		else {
			switch (status) {
				case SUCCEEDED:
					return FinalApplicationStatus.SUCCEEDED;
				case FAILED:
					return FinalApplicationStatus.FAILED;
				case CANCELED:
					return FinalApplicationStatus.KILLED;
				default:
					return FinalApplicationStatus.UNDEFINED;
			}
		}
	}

	// parse the host and port from akka address,
	// the akka address is like akka.tcp://flink@100.81.153.180:49712/user/$a
	private static Tuple2<String, Integer> parseHostPort(String address) {
		String[] hostPort = address.split("@")[1].split(":");
		String host = hostPort[0];
		String port = hostPort[1].split("/")[0];
		return new Tuple2<>(host, Integer.valueOf(port));
	}

	private void requestYarnContainer(Resource resource, Priority priority, ResourceConstraints constraints) {
		AtomicInteger pendingNumber = new AtomicInteger(0);
		AtomicInteger prevPendingNumber = numPendingContainerRequests.putIfAbsent(priority.getPriority(), pendingNumber);
		if (prevPendingNumber != null) {
			pendingNumber = prevPendingNumber;
		}
		pendingNumber.getAndIncrement();

		requestAdapter.addRequest(resource, priority, pendingNumber.get(), constraints);

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(FAST_YARN_HEARTBEAT_INTERVAL_MS);

		log.info("Requesting new TaskExecutor container with resources {}. Priority {}. Number pending requests {}." +
				" Resource constraints {}.", resource, priority, pendingNumber.get(), constraints);
	}

	private boolean checkAllocateNewResourceExceedTotalResourceLimit(double cpu, int memory, String errMsg, boolean includePending) {
		if (maxTotalCpuCore == Double.MAX_VALUE && maxTotalMemoryMb == Integer.MAX_VALUE) {
			return false;
		}

		double currentTotalCpu = 0.0;
		int currentTotalMemory = 0;

		currentTotalCpu += flinkConfig.getInteger(YarnConfigOptions.JOB_APP_MASTER_CORE);
		currentTotalMemory += flinkConfig.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY);

		for (YarnWorkerNode workerNode : workerNodeMap.values()) {
			Resource resource = workerNode.getContainer().getResource();
			currentTotalCpu += resource.getVirtualCores() / yarnVcoreRatio;
			currentTotalMemory += resource.getMemory();
		}

		if (includePending) {
			for (Map.Entry<Integer, AtomicInteger> entry : numPendingContainerRequests.entrySet()) {
				Tuple2<TaskManagerResource, Set<SlotTag>> tuple = priorityToResourceAndTagsMap.get(entry.getKey());
				TaskManagerResource taskManagerResource = tuple == null ? null : tuple.f0;
				int num = entry.getValue().get();
				currentTotalCpu += taskManagerResource.getContainerCpuCores() * num;
				currentTotalMemory += taskManagerResource.getTotalContainerMemory() * num;
			}
		}

		if (currentTotalCpu + cpu > maxTotalCpuCore || currentTotalMemory + memory > maxTotalMemoryMb) {
			if (errMsg != null) {
				errMsg += String.format(
					" (new resource = <CPU:%s, MEM:%s>, current total resource = <CPU:%s, MEM:%s>, limit = <CPU:%s, MEM:%s>)",
					cpu, memory, currentTotalCpu, currentTotalMemory, maxTotalCpuCore, maxTotalMemoryMb);
				log.warn(errMsg);
				tryAllocateExceedLimitExceptions.put(System.currentTimeMillis(), new ResourceManagerException(errMsg));
			}
			return true;
		}
		return false;
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Container container)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		TaskManagerResource tmResource = getTaskManagerResource(container.getPriority().getPriority());
		final ContaineredTaskManagerParameters taskManagerParameters = ContaineredTaskManagerParameters.create(
			flinkConfig,
			container.getResource().getMemory(),
			Math.max(tmResource.getTotalHeapMemory(),
				container.getResource().getMemory() -
					tmResource.getTotalDirectMemory() -
					tmResource.getTotalNativeMemory()),
			tmResource.getTotalDirectMemory(),
			tmResource.getSlotNum(),
			tmResource.getYoungHeapMemory(),
			container.getResource().getVirtualCores() / yarnVcoreRatio);

		log.info("TaskExecutor {} will be started with container size {} MB, JVM heap size {} MB, " +
				"new generation size {} MB, JVM direct memory limit {} MB on {}",
			container.getId(),
			taskManagerParameters.taskManagerTotalMemoryMB(),
			taskManagerParameters.taskManagerHeapSizeMB(),
			taskManagerParameters.getYoungMemoryMB(),
			taskManagerParameters.taskManagerDirectMemoryLimitMB(),
			container.getNodeHttpAddress());

		final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
			flinkConfig, "", 0, tmResource.getSlotNum(), null);

		//TODO: Add resource profile of slots to task executor config.
		//For blink, all slots in a task executor have same resource profile.
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		ObjectOutputStream rpOutput = new ObjectOutputStream(output);
		rpOutput.writeObject(tmResource.getTaskResourceProfile());
		rpOutput.close();

		taskManagerConfig.setString(TaskManagerOptions.TASK_MANAGER_RESOURCE_PROFILE_KEY,
			new String(Base64.encodeBase64(output.toByteArray())));

		final long managedMemory = tmResource.getManagedMemorySize() > 1 ? tmResource.getManagedMemorySize() :
				flinkConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
		taskManagerConfig.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), managedMemory);

		final int floatingManagedMemory = tmResource.getFloatingManagedMemorySize();
		taskManagerConfig.setInteger(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE.key(), floatingManagedMemory);

		taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY.key(),
			tmResource.getTaskManagerNettyMemorySizeMB());

		taskManagerConfig.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION,
				flinkConfig.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION));
		long networkBufBytes = ((long) tmResource.getNetworkMemorySize()) << 20;
		taskManagerConfig.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, networkBufBytes);
		taskManagerConfig.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, networkBufBytes);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			taskManagerConfig,
			currDir,
			YarnTaskExecutorRunner.class,
			log);

		// set a special environment variable to uniquely identify this container
		taskExecutorLaunchContext.getEnvironment().put(ENV_FLINK_CONTAINER_ID, container.getId().toString());
		taskExecutorLaunchContext.getEnvironment().put(ENV_FLINK_NODE_ID, container.getNodeId().getHost());

		//TODO: these filed are required by blink
		taskExecutorLaunchContext.getEnvironment().put(YarnConfigKeys.ENV_APP_ID, env.get(YarnConfigKeys.ENV_APP_ID));

		return taskExecutorLaunchContext;
	}

	/**
	 * Generate priority by given resource profile.
	 * Priority is only used for distinguishing request of different resource.
	 * @param tmResource The resource profile of a request
	 * @return The priority of this resource profile.
	 */
	private int generatePriority(TaskManagerResource tmResource, Set<SlotTag> tags) {
		Tuple2<TaskManagerResource, Set<SlotTag>> tuple = new Tuple2<>(tmResource, tags);
		Integer priority = resourceAndTagsToPriorityMap.get(tuple);
		if (priority != null) {
			return priority;
		} else {
			priority = latestPriority++;
			resourceAndTagsToPriorityMap.put(tuple, priority);
			priorityToResourceAndTagsMap.put(priority, tuple);
			return priority;
		}
	}

	/**
	 * Request new container if pending containers cannot satisfies pending slot requests.
	 */
	private void internalRequestYarnContainer(Resource resource, Priority priority) {
		AtomicInteger pendingNumber = numPendingContainerRequests.get(priority.getPriority());
		Tuple2<TaskManagerResource, Set<SlotTag>> tuple = priorityToResourceAndTagsMap.get(priority.getPriority());
		TaskManagerResource tmResource = tuple == null ? null : tuple.f0;
		if (pendingNumber == null || tmResource == null) {
			log.error("There is no previous allocation with id {} for {}.", priority, resource);
		} else {
			// TODO: Just a weak check because we don't know how many pending slot requests belongs to
			// this priority. So currently we use overall pending slot requests number to restrain
			// the container requests of this priority.
			int pendingSlotRequests = getNumberPendingSlotRequests();
			int pendingSlotAllocation = pendingNumber.get() * tmResource.getSlotNum();
			if (pendingSlotRequests > pendingSlotAllocation) {
				requestYarnContainer(resource, priority, tmResource.getTaskResourceProfile().getResourceConstraints());
			} else {
				log.info("Skip request yarn container, there are enough pending slot allocation for slot requests." +
					" Priority {}. Resource {}. Pending slot allocation {}. Pending slot requests {}.",
					priority.getPriority(),
					resource,
					pendingSlotAllocation,
					pendingSlotRequests);
			}
		}
	}

	@VisibleForTesting
	static Map<String, Double> loadExtendedResourceConstrains(Configuration config, boolean loadMin) {
		String constraintsStr;
		if (loadMin) {
			constraintsStr = config.getString(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MIN_EXTENDED_RESOURCES);
		} else {
			constraintsStr = config.getString(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_EXTENDED_RESOURCES);
		}
		String[] constrains = constraintsStr.split(",");
		Map<String, Double> extendedResourceConstrains = new HashMap<>(constrains.length);
		for (String constrain : constrains) {
			String[] kv = constrain.split("=");
			if (kv.length == 2) {
				extendedResourceConstrains.put(kv[0].toLowerCase(), Double.valueOf(kv[1]));
			}
		}
		return extendedResourceConstrains;
	}

	/**
	 * Calculate the slot number in a task executor according to the resource.
	 *
	 * @param resourceProfile The resource profile of a request
	 * @return The slot number in a task executor.
	 */
	@VisibleForTesting
	int calculateSlotNumber(ResourceProfile resourceProfile) {
		if (resourceProfile.getCpuCores() <= 0 || resourceProfile.getMemoryInMB() <= 0) {
			return 1;
		}
		else {
			int minSlot = Math.max((int) Math.ceil(minCorePerContainer / resourceProfile.getCpuCores()),
				(int) Math.ceil(1.0 * minMemoryPerContainer / resourceProfile.getMemoryInMB()));
			int maxSlot = Math.min((int) Math.floor(maxCorePerContainer / resourceProfile.getCpuCores()),
				(int) Math.floor(1.0 * maxMemoryPerContainer / resourceProfile.getMemoryInMB()));

			for (org.apache.flink.api.common.resources.Resource extendedResource : resourceProfile.getExtendedResources().values()) {
				// Skip floating memory, it has been added to memory
				if (extendedResource.getName().equals(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME)) {
					continue;
				}

				Double minPerContainer = minExtendedResourcePerContainer.get(extendedResource.getName().toLowerCase());
				if (minPerContainer != null) {
					minSlot = Math.max(minSlot, (int) Math.ceil(minPerContainer / extendedResource.getValue()));
				}

				Double maxPerContainer = maxExtendedResourcePerContainer.get(extendedResource.getName().toLowerCase());
				if (maxPerContainer != null) {
					maxSlot = Math.min(maxSlot, (int) Math.floor(maxPerContainer / extendedResource.getValue()));
				}
			}

			// if container's max resource constraints conflict with min resource constraints,
			// follow the max resource constraints
			return Math.min(minSlot, maxSlot);
		}
	}

	private Resource generateContainerResource(TaskManagerResource tmResource) {
		int mem = Math.max(tmResource.getTotalContainerMemory(), minMemoryPerContainer);
		int vcore = (int) (Math.max(tmResource.getContainerCpuCores(), minCorePerContainer) * yarnVcoreRatio);
		Resource capability = Resource.newInstance(mem, vcore);
		Map<String, org.apache.flink.api.common.resources.Resource> extendedResources =
			Utils.getPureExtendedResources(tmResource.getTaskResourceProfile());
		requestAdapter.updateExtendedResources(capability, extendedResources);
		return capability;
	}

	private TaskManagerResource getTaskManagerResource(int priority) {
		Tuple2<TaskManagerResource, Set<SlotTag>> tuple = priorityToResourceAndTagsMap.get(priority);
		TaskManagerResource tmResource = tuple == null ? null : tuple.f0;
		if (tmResource != null) {
			return tmResource;
		}
		throw new IllegalArgumentException("The priority " + priority + " doesn't exist!");
	}

	/**
	 * Resources of containers allocated from Yarn RM may not be exactly the same as originally requested.
	 * When we removeContainerResource or requestYarnRequest, we should make sure not to use any container's
	 * allocated resources, or errors may occur when using AMRMClientAsync.
	 *
	 * @param priority Priority of this request.
	 * @return Original resource request.
	 */
	private Resource getOrigContainerResource(int priority) {
		TaskManagerResource tmResource = getTaskManagerResource(priority);
		return generateContainerResource(tmResource);
	}

	private ResourceConstraints getResourceConstraints(int priority) {
		return getTaskManagerResource(priority).getTaskResourceProfile().getResourceConstraints();
	}

	private void tryStartBlockedWorkers() {
		while (!priorityToBlockedWorkers.isEmpty()) {
			int priority = findFeasibleBlockedWorkerPriority();
			if (priority < 0) {
				break;
			}
			TaskManagerResource tmResource = priorityToResourceAndTagsMap.get(priority).f0;

			synchronized (spareSlotsAndBlockedWorkersLock) {
				int blockWorkerNum = priorityToBlockedWorkers.get(priority);
				if (blockWorkerNum > tmResource.getSlotNum()) {
					priorityToBlockedWorkers.put(priority, blockWorkerNum - tmResource.getSlotNum());
				} else {
					priorityToBlockedWorkers.remove(priority);
					if (tmResource.getSlotNum() > blockWorkerNum) {
						priorityToSpareSlots.put(priority, tmResource.getSlotNum() - blockWorkerNum);
					}
				}
			}

			Resource resource = generateContainerResource(tmResource);
			requestYarnContainer(resource, Priority.newInstance(priority),
				tmResource.getTaskResourceProfile().getResourceConstraints());
		}

		if (priorityToBlockedWorkers.isEmpty()) {
			slotManager.disableIdleTaskManagersFastTimeout();
		}
	}

	private int findFeasibleBlockedWorkerPriority() {
		for (Map.Entry<Integer, Integer> entry : priorityToBlockedWorkers.entrySet()) {
			Resource resource = generateContainerResource(priorityToResourceAndTagsMap.get(entry.getKey()).f0);
			if (!checkAllocateNewResourceExceedTotalResourceLimit(
				resource.getVirtualCores() / yarnVcoreRatio, resource.getMemory(), null, true)) {
				return entry.getKey();
			}
		}
		return -1;
	}
}
