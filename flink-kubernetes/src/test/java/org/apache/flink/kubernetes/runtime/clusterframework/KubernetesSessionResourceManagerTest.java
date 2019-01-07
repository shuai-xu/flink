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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.internal.PodOperationsImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * General tests for the Flink Kubernetes session resource manager component.
 */
public class KubernetesSessionResourceManagerTest extends TestLogger {

	protected static final int TASK_MANAGER_COUNT = 3;

	protected static final String APP_ID = "k8s-cluster-1234";

	protected static final String CONTAINER_IMAGE = "flink-k8s:latest";

	protected static final String MASTER_URL = "http://127.0.0.1:49359";

	protected static final String RPC_PORT = "11111";

	protected static final String HOSTNAME = "127.0.0.1";

	protected Configuration flinkConf;

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
		flinkConf.setInteger(KubernetesConfigOptions.TASK_MANAGER_COUNT, TASK_MANAGER_COUNT);
	}

	@SuppressWarnings("unchecked")
	protected KubernetesSessionResourceManager createKubernetesSessionResourceManager(Configuration conf) {
		ResourceManagerConfiguration rmConfig =
			new ResourceManagerConfiguration(Time.seconds(1), Time.seconds(10));
		final HighAvailabilityServices highAvailabilityServices = Mockito.mock(HighAvailabilityServices.class);
		Mockito.when(highAvailabilityServices.getResourceManagerLeaderElectionService()).thenReturn(Mockito.mock(LeaderElectionService.class));
		final HeartbeatServices heartbeatServices = Mockito.mock(HeartbeatServices.class);
		Mockito.when(heartbeatServices.createHeartbeatManagerSender(
			Matchers.any(ResourceID.class),
			Matchers.any(HeartbeatListener.class),
			Matchers.any(ScheduledExecutor.class),
			Matchers.any(Logger.class))).thenReturn(Mockito.mock(HeartbeatManager.class));
		return new KubernetesSessionResourceManager(
			new TestingRpcService(), "sessionResourceManager", ResourceID.generate(), conf,
			rmConfig,
			highAvailabilityServices, heartbeatServices,
			new SlotManager(Mockito.mock(ScheduledExecutor.class), Time.seconds(1), Time.seconds(1), Time.seconds(1)),
			Mockito.mock(MetricRegistry.class),
			Mockito.mock(JobLeaderIdService.class),
			new ClusterInformation("localhost", 1234),
			Mockito.mock(FatalErrorHandler.class));
	}

	private KubernetesClient createMockKubernetesClient(List<Integer> podIdLists) {
		KubernetesClient client = Mockito.mock(KubernetesClient.class);
		MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods = Mockito.mock(MixedOperation.class);

		PodList podList = new PodList();
		List<Pod> podListItems = new ArrayList<>();
		if (podIdLists != null) {
			podIdLists.stream().forEach(e -> podListItems.add(createPod(e)));
		}
		podList.setItems(podListItems);
		Mockito.when(pods.delete()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Pod pod = (Pod) invocation.getArguments()[0];
				podIdLists.remove(pod);
				return null;
			}
		});
		Mockito.when(pods.create()).thenReturn(null);
		FilterWatchListDeletable filter = Mockito.mock(PodOperationsImpl.class);
		Mockito.when(pods.withLabels(Matchers.anyMap())).thenReturn(filter);
		Mockito.when(filter.list()).thenReturn(podList);
		Mockito.when(client.pods()).thenReturn(pods);

		MixedOperation configMaps = Mockito.mock(MixedOperation.class);
		ConfigMap configMap = new ConfigMap();
		Mockito.when(configMaps.createOrReplace()).thenReturn(configMap);
		Mockito.when(client.configMaps()).thenReturn(configMaps);
		return client;
	}

	protected Pod createPod(int podId) {
		ObjectMeta meta = new ObjectMeta();
		meta.setName(APP_ID + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR + podId);
		Pod pod = new Pod();
		pod.setMetadata(meta);
		return pod;
	}

	protected KubernetesSessionResourceManager mockSpyKubernetesSessionRM(
		Configuration flinkConf, KubernetesClient kubernetesClient) {
		KubernetesSessionResourceManager kubernetesSessionRM =
			createKubernetesSessionResourceManager(flinkConf);
		KubernetesSessionResourceManager spyKubernetesSessionRM = Mockito.spy(kubernetesSessionRM);
		Mockito.doNothing().when(spyKubernetesSessionRM).setupOwnerReference();
		spyKubernetesSessionRM.setOwnerReference(new OwnerReferenceBuilder().build());
		Mockito.doReturn(kubernetesClient).when(spyKubernetesSessionRM).createKubernetesClient();
		return spyKubernetesSessionRM;
	}

	@Test
	public void testNormalProcess() throws Exception {
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockSpyKubernetesSessionRM(flinkConf, mockKubernetesClient);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesSessionRM).checkTMRegistered(Matchers.any());

		// start session RM
		spyKubernetesSessionRM.start();
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// add worker nodes
		int podNum = 0;
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum, spyKubernetesSessionRM.getPendingWorkerNodes().size());
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		}
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// task manager register timeout will never happen
		Mockito.verify(spyKubernetesSessionRM,
			Mockito.times(TASK_MANAGER_COUNT)).requestNewWorkerNode();

		spyKubernetesSessionRM.postStop();
	}

	@Test
	public void testGetPreviousWorkerNodes() throws Exception {
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(IntStream.of(1, 2).boxed().collect(Collectors.toList()));
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockSpyKubernetesSessionRM(flinkConf, mockKubernetesClient);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesSessionRM).checkTMRegistered(Matchers.any());

		// start session RM, get previous 2 worker nodes
		spyKubernetesSessionRM.start();
		Assert.assertEquals(2, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(TASK_MANAGER_COUNT - 2, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// add worker nodes
		int podNum = spyKubernetesSessionRM.getNumberAllocatedWorkers();
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// task manager register timeout will never happen
		Mockito.verify(spyKubernetesSessionRM,
			Mockito.times(TASK_MANAGER_COUNT - 2)).requestNewWorkerNode();

		spyKubernetesSessionRM.postStop();
	}

	@Test
	public void testTaskManagerRegisterTimeout() throws Exception {
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockSpyKubernetesSessionRM(flinkConf, mockKubernetesClient);

		// start session RM
		spyKubernetesSessionRM.start();
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// add worker nodes
		int podNum = spyKubernetesSessionRM.getNumberAllocatedWorkers();
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum,
				spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// trigger task manager register timeout
		spyKubernetesSessionRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesSessionRM::checkTMRegistered);
		Mockito.verify(spyKubernetesSessionRM, Mockito.times(TASK_MANAGER_COUNT * 2))
			.requestNewWorkerNode();

		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		while (podNum < TASK_MANAGER_COUNT * 2) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum - TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT * 2 - podNum,
				spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}

		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		spyKubernetesSessionRM.postStop();
	}

	@Test
	public void testExitWhenReachMaxFailedAttempts() throws Exception {
		// set max failed attempts = tm-count - 1
		int maxFailedAttempts = TASK_MANAGER_COUNT - 1;
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setInteger(KubernetesConfigOptions.WORKER_NODE_MAX_FAILED_ATTEMPTS, maxFailedAttempts);
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockSpyKubernetesSessionRM(newFlinkConf, mockKubernetesClient);

		// start session RM
		spyKubernetesSessionRM.start();
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// add worker nodes
		int podNum = spyKubernetesSessionRM.getNumberAllocatedWorkers();
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// trigger task manager register timeout
		spyKubernetesSessionRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesSessionRM::checkTMRegistered);
		// won't add new request for the last attempt
		Assert.assertEquals(maxFailedAttempts - 1, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		Assert.assertTrue(spyKubernetesSessionRM.isStopped());
	}
}
