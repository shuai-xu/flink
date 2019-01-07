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

package org.apache.flink.kubernetes.runtime.clusterframework;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesClientFactory;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.StrictlyMatchingSlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesBuilder;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.internal.PodOperationsImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_RPC_PORT;
import static org.apache.flink.kubernetes.configuration.Constants.RESOURCE_NAME_CPU;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * IT case for the Flink Kubernetes resource manager component.
 */
public class KubernetesResourceManagerITCase extends TestLogger {

	private final Time timeout = Time.seconds(10000L);

	protected static final boolean USE_MOCK_K8S_CLIENT = true;

	protected static final String APP_ID = "k8s-cluster-1234";

	protected static final String CONTAINER_IMAGE = "flink-k8s:latest";

	protected static final String MASTER_URL = "http://127.0.0.1:49359";

	protected static final String RPC_PORT = "11111";

	protected static final String HOSTNAME = "127.0.0.1";

	protected Configuration flinkConf;

	protected static final String RM_ADDRESS = "RM";
	protected static final String JM_ADDRESS = "JM";
	protected static final JobID JOB_ID = new JobID();
	protected static final JobMasterId JOB_MASTER_ID = JobMasterId.generate();
	protected static final ResourceProfile RESOURCE_PROFILE = new ResourceProfile(0.3, 64);

	protected KubernetesClient kubernetesClient;
	protected KubernetesResourceManager resourceManager;
	protected SlotManager slotManager;
	protected TaskExecutor taskExecutor;

	@Before
	public void setup() {
		flinkConf = new Configuration();
		flinkConf.setString(KubernetesConfigOptions.CLUSTER_ID, APP_ID);
		flinkConf.setString(KubernetesConfigOptions.MASTER_URL, MASTER_URL);
		flinkConf.setString(KubernetesConfigOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
		flinkConf.setString(TaskManagerOptions.RPC_PORT, RPC_PORT);
		flinkConf.setString(RestOptions.ADDRESS, HOSTNAME);
		flinkConf.setString(JobManagerOptions.ADDRESS, HOSTNAME);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, 128);
		flinkConf.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 128);
		flinkConf.setLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE, 10);
		flinkConf.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 64 << 20);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, 10);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NATIVE_MEMORY, 10);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY, 10);
		flinkConf.setDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MIN_CORE, 0.01);
		flinkConf.setDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_CORE, 0.01);
		if (!USE_MOCK_K8S_CLIENT) {
			// init kubernetes testing env
			// 1. create testing service
			kubernetesClient = KubernetesClientFactory.create(flinkConf);
			kubernetesClient.services().create(new ServiceBuilder()
				.withNewMetadata()
				.withName(APP_ID + Constants.SERVICE_NAME_SUFFIX)
				.endMetadata().withNewSpec()
				.withType("ClusterIP")
				.addNewPort()
				.withName(JOBMANAGER_RPC_PORT)
				.withPort(Integer.parseInt(RPC_PORT))
				.withProtocol("TCP")
				.endPort()
				.endSpec()
				.build());
			Service service = kubernetesClient.services().withName(APP_ID + Constants.SERVICE_NAME_SUFFIX).get();
			assertNotNull(service);
		}
	}

	@After
	public void teardown() {
		if (!USE_MOCK_K8S_CLIENT) {
			// clean kubernetes testing env
			// 1. remove testing service
			kubernetesClient.services().withName(APP_ID + Constants.SERVICE_NAME_SUFFIX).delete();
		}
	}

	class TestingKubernetesResourceManager extends KubernetesResourceManager {
		public TestingKubernetesResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler) {
			super(rpcService, resourceManagerEndpointId, resourceId, flinkConfig, resourceManagerConfiguration,
				highAvailabilityServices, heartbeatServices, slotManager, metricRegistry, jobLeaderIdService,
				clusterInformation, fatalErrorHandler);
		}

		protected KubernetesClient createKubernetesClient() {
			return createMockKubernetesClient(null);
		}

		protected void setupTaskManagerConfigMap() {
			tmConfigMap = new ConfigMapBuilder().build();
		}

		protected void setupOwnerReference() {
			this.setOwnerReference(new OwnerReferenceBuilder().build());
		}

		private KubernetesClient createMockKubernetesClient(List<Pair<Integer, Integer>> podPriorityIdLists) {
			KubernetesClient client = Mockito.mock(KubernetesClient.class);
			MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods = Mockito.mock(MixedOperation.class);

			PodList podList = new PodList();
			List<Pod> podListItems = new ArrayList<>();
			if (podPriorityIdLists != null) {
				podPriorityIdLists.stream().forEach(e -> podListItems.add(createPod(e.getKey(), e.getValue())));
			}
			podList.setItems(podListItems);
			Mockito.when(pods.create()).thenReturn(null);
			Mockito.when(pods.delete(Mockito.any(Pod.class))).thenReturn(true);
			FilterWatchListDeletable filter = Mockito.mock(PodOperationsImpl.class);
			Mockito.when(pods.withLabels(org.mockito.Matchers.anyMap())).thenReturn(filter);
			Mockito.when(filter.list()).thenReturn(podList);
			Mockito.when(filter.delete()).thenReturn(true);
			PodResource podResource = Mockito.mock(PodResource.class);
			Mockito.when(podResource.delete()).thenReturn(true);
			Mockito.when(pods.withName(Mockito.anyString())).thenReturn(podResource);
			Mockito.when(client.pods()).thenReturn(pods);

			MixedOperation configMaps = Mockito.mock(MixedOperation.class);
			ConfigMap configMap = new ConfigMap();
			Mockito.when(configMaps.createOrReplace()).thenReturn(configMap);
			Mockito.when(client.configMaps()).thenReturn(configMaps);
			return client;
		}
	}

	protected String createPodName(int podId) {
		return APP_ID + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR + podId;
	}

	protected Pod createPod(int priority, int podId) {
		Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_APP_KEY, APP_ID);
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		labels.put(Constants.LABEL_PRIORITY_KEY, String.valueOf(priority));
		ObjectMeta meta = new ObjectMeta();
		meta.setName(createPodName(podId));
		meta.setLabels(labels);
		Pod pod = new Pod();
		pod.setMetadata(meta);
		return pod;
	}

	public void initilize() throws Exception {
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		TestingHighAvailabilityServices testingHAServices = new TestingHighAvailabilityServices();
		final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
		final ResourceID taskManagerResourceId = new ResourceID(APP_ID + "-taskmanager-1");
		final UUID rmLeaderId = UUID.randomUUID();
		final TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		final SettableLeaderRetrievalService rmLeaderRetrievalService = new SettableLeaderRetrievalService(null, null);
		final ResourceID rmResourceId = new ResourceID(RM_ADDRESS);

		testingHAServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);
		testingHAServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);
		testingHAServices.setJobMasterLeaderRetriever(JOB_ID, new SettableLeaderRetrievalService(JM_ADDRESS, JOB_MASTER_ID.toUUID()));

		TestingRpcService rpcService = new TestingRpcService();
		ResourceManagerConfiguration resourceManagerConfiguration = new ResourceManagerConfiguration(
			Time.milliseconds(500L),
			Time.milliseconds(500L));
		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			testingHAServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));
		MetricRegistry metricRegistry = NoOpMetricRegistry.INSTANCE;
		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

		final TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(flinkConf);
		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(taskManagerResourceId, InetAddress.getLocalHost(), 1234);
		List<ResourceProfile> resourceProfiles = Arrays.asList(RESOURCE_PROFILE);
		final TaskSlotTable taskSlotTable = new TaskSlotTable(
			resourceProfiles,
			new ResourceProfile(1, 100),
			new TimerService<AllocationID>(scheduledExecutorService, 100L));
		slotManager = new StrictlyMatchingSlotManager(
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		final File[] taskExecutorLocalStateRootDirs =
			new File[]{new File(System.getProperty("java.io.tmpdir"), "localRecovery")};

		final TaskExecutorLocalStateStoresManager taskStateManager = new TaskExecutorLocalStateStoresManager(
			false,
			taskExecutorLocalStateRootDirs,
			rpcService.getExecutor());

		if (USE_MOCK_K8S_CLIENT) {
			resourceManager = new TestingKubernetesResourceManager(
				rpcService,
				FlinkResourceManager.RESOURCE_MANAGER_NAME,
				rmResourceId,
				flinkConf,
				resourceManagerConfiguration,
				testingHAServices,
				heartbeatServices,
				slotManager,
				metricRegistry,
				jobLeaderIdService,
				new ClusterInformation("localhost", 1234),
				testingFatalErrorHandler);
		} else {
			resourceManager = new KubernetesResourceManager(
				rpcService,
				FlinkResourceManager.RESOURCE_MANAGER_NAME,
				rmResourceId,
				flinkConf,
				resourceManagerConfiguration,
				testingHAServices,
				heartbeatServices,
				slotManager,
				metricRegistry,
				jobLeaderIdService,
				new ClusterInformation("localhost", 1234),
				testingFatalErrorHandler);
		}

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(taskManagerLocation)
			.setTaskSlotTable(taskSlotTable)
			.setTaskStateManager(taskStateManager)
			.build();

		taskExecutor = new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			testingHAServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			new BlobCacheService(
				flinkConf,
				new VoidBlobStore(),
				null),
			Executors.newSingleThreadExecutor(),
			testingFatalErrorHandler);

		JobMasterGateway jmGateway = mock(JobMasterGateway.class);
		when(jmGateway.registerTaskManager(any(String.class), any(TaskManagerLocation.class), any(Time.class)))
			.thenReturn(CompletableFuture.completedFuture(new JMTMRegistrationSuccess(taskManagerResourceId)));
		when(jmGateway.getHostname()).thenReturn(JM_ADDRESS);
		when(jmGateway.offerSlots(
			eq(taskManagerResourceId),
			any(Collection.class),
			any(Time.class))).thenReturn(mock(CompletableFuture.class, RETURNS_MOCKS));
		when(jmGateway.getFencingToken()).thenReturn(JOB_MASTER_ID);

		rpcService.registerGateway(RM_ADDRESS, resourceManager.getSelfGateway(ResourceManagerGateway.class));
		rpcService.registerGateway(JM_ADDRESS, jmGateway);
		rpcService.registerGateway(taskExecutor.getAddress(), taskExecutor.getSelfGateway(TaskExecutorGateway.class));

		// start RM
		resourceManager.start();

		// notify the RM that it is the leader
		CompletableFuture<UUID> isLeaderFuture = rmLeaderElectionService.isLeader(rmLeaderId);

		// wait for the completion of the leader election
		assertEquals(rmLeaderId, isLeaderFuture.get());

		// notify the TM about the new RM leader
		rmLeaderRetrievalService.notifyListener(RM_ADDRESS, rmLeaderId);
	}

	@Test
	public void testSlotAllocation() throws Exception {
		initilize();
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		CompletableFuture<RegistrationResponse> registrationResponseFuture = rmGateway.registerJobManager(
			JOB_MASTER_ID,
			new ResourceID(JM_ADDRESS),
			JM_ADDRESS,
			JOB_ID,
			timeout);
		RegistrationResponse registrationResponse = registrationResponseFuture.get();
		assertTrue(registrationResponse instanceof JobMasterRegistrationSuccess);
		final AllocationID allocationId = new AllocationID();
		final SlotRequest slotRequest = new SlotRequest(JOB_ID, allocationId, RESOURCE_PROFILE, JM_ADDRESS);
		CompletableFuture<Acknowledge> slotAck = rmGateway.requestSlot(JOB_MASTER_ID, slotRequest, timeout);
		// add worker node
		if (USE_MOCK_K8S_CLIENT) {
			resourceManager.handlePodMessage(Watcher.Action.ADDED, createPod(0, 1));
		} else {
			waitFor(() -> resourceManager.getNumberAllocatedWorkers() == 1, 100, 5000);
			// check cpu is correct
			KubernetesWorkerNode workerNode = resourceManager.getWorkerNodes().values().iterator().next();
			Assert.assertEquals(1, workerNode.getPod().getSpec().getContainers().size());
			String cpuAmount = workerNode.getPod().getSpec().getContainers().get(0).getResources().getRequests().get(RESOURCE_NAME_CPU).getAmount();
			Assert.assertEquals((int) (RESOURCE_PROFILE.getCpuCores() * 1000) + "m", cpuAmount);
		}
		Assert.assertEquals(1, resourceManager.getNumberAllocatedWorkers());
		// start task executor then waiting for registration
		taskExecutor.start();
		waitFor(() -> slotManager.getNumberRegisteredSlots() == 1, 100, 5000);
		Assert.assertEquals(1, slotManager.getNumberRegisteredSlots());
	}

	public static void waitFor(Supplier<Boolean> check, int checkEveryMillis, int waitForMillis)
		throws TimeoutException, InterruptedException {
		Preconditions.checkNotNull(check, "Input supplier interface should be initailized");
		Preconditions.checkArgument(waitForMillis >= checkEveryMillis,
			"Total wait time should be greater than check interval time");

		long st = System.currentTimeMillis();
		boolean result = check.get();

		while (!result && (System.currentTimeMillis() - st < waitForMillis)) {
			Thread.sleep(checkEveryMillis);
			result = check.get();
		}

		if (!result) {
			throw new TimeoutException("Timed out waiting for condition. ");
		}
	}
}
