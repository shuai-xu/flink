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
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
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
import org.apache.flink.types.DefaultPair;
import org.apache.flink.types.Pair;
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
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * General tests for the Flink Kubernetes resource manager component.
 */
public class KubernetesResourceManagerTest extends TestLogger {

	protected static final String APP_ID = "k8s-cluster-1234";

	protected static final String CONTAINER_IMAGE = "reg.docker.alibaba-inc.com/boyuan/flink-test:latest";

	protected static final String MASTER_URL = "http://127.0.0.1:49359";

	protected static final String RPC_PORT = "11111";

	protected static final String HOSTNAME = "127.0.0.1";

	protected Configuration flinkConf;

	protected SlotManager spySlotManager;

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
		spySlotManager = Mockito.spy(new SlotManager(Mockito.mock(ScheduledExecutor.class), Time.seconds(1), Time.seconds(1), Time.seconds(1)));
	}

	@SuppressWarnings("unchecked")
	protected KubernetesResourceManager createKubernetesResourceManager(Configuration conf) {
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
		return new KubernetesResourceManager(
			new TestingRpcService(), "ResourceManager", ResourceID.generate(), conf,
			rmConfig,
			highAvailabilityServices, heartbeatServices,
			spySlotManager,
			Mockito.mock(MetricRegistry.class),
			Mockito.mock(JobLeaderIdService.class),
			new ClusterInformation("localhost", 1234),
			Mockito.mock(FatalErrorHandler.class));
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
		Mockito.when(pods.withLabels(Matchers.anyMap())).thenReturn(filter);
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

	protected long[] getSortedWorkerNodeIds(KubernetesResourceManager spyKubernetesRM){
		long[] ids = spyKubernetesRM.getWorkerNodes().values().stream().mapToLong(e -> e.getPodId()).toArray();
		Arrays.sort(ids);
		return ids;
	}

	protected KubernetesResourceManager mockSpyKubernetesRM(
		Configuration flinkConf, KubernetesClient kubernetesClient) {
		KubernetesResourceManager kubernetesSessionRM =
			createKubernetesResourceManager(flinkConf);
		KubernetesResourceManager spyKubernetesRM = Mockito.spy(kubernetesSessionRM);
		Mockito.doNothing().when(spyKubernetesRM).setupOwnerReference();
		spyKubernetesRM.setOwnerReference(new OwnerReferenceBuilder().build());
		Mockito.doReturn(kubernetesClient).when(spyKubernetesRM).createKubernetesClient();
		Mockito.doNothing().when(spyKubernetesRM).setupTaskManagerConfigMap();
		return spyKubernetesRM;
	}

	@Test
	public void testNormalProcess() throws Exception {
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesResourceManager spyKubernetesRM =
			mockSpyKubernetesRM(flinkConf, mockKubernetesClient);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesRM).checkTMRegistered(Matchers.any());

		// start RM
		spyKubernetesRM.start();

		// request worker nodes
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		ResourceProfile resourceProfile2 = new ResourceProfile(1.0, 500);
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(3, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// cancel 1 worker node(id=1) with priority 0
		spyKubernetesRM.cancelNewWorker(resourceProfile1);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		// cancel 1 worker node(id=3) with priority 1
		spyKubernetesRM.cancelNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		Assert.assertEquals(new HashSet<>(), spyKubernetesRM.getWorkerNodes().keySet());

		// add removed worker node(id=1) with priority 0, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 1));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());
		// add removed worker node(id=3) with priority 1, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 3));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());

		// add worker node(id=2) with priority 0
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 2));
		Assert.assertEquals(1, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{2L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// add worker node(id=4) with priority 1
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 4));
		Assert.assertEquals(2, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{2L, 4L}, getSortedWorkerNodeIds(spyKubernetesRM));

		// stop worker node(id=4) with priority 1
		// mock that number of pending slot requests is larger than number of pending worker nodes
		Mockito.doReturn(2).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.stopWorker(spyKubernetesRM.getWorkerNodes().get(new ResourceID(createPodName(4))));
		// should left 1 worker node(id=2)
		Assert.assertEquals(1, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{2L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// should request new worker node inside stopWorker, now pending worker nodes: 2
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testNormalProcessForMultiSlots() throws Exception {
		// set max core=3 for multiple slots
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_CORE, 4.0);
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesResourceManager spyKubernetesRM =
			mockSpyKubernetesRM(newFlinkConf, mockKubernetesClient);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesRM).checkTMRegistered(Matchers.any());

		// start RM
		spyKubernetesRM.start();

		// request 50 worker nodes with priority 0 and 1
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		ResourceProfile resourceProfile2 = new ResourceProfile(2.0, 200);
		int requestNum = 0;
		for (int i = 0; i < 50; i++) {
			requestNum++;
			spyKubernetesRM.startNewWorker(resourceProfile1);
			spyKubernetesRM.startNewWorker(resourceProfile2);
			Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(requestNum % 4 == 0 ? requestNum / 4 : requestNum / 4 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
			Assert.assertEquals(requestNum % 2 == 0 ? requestNum / 2 : requestNum / 2 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		}

		// cancel 47 worker nodes with priority 0 and 1
		for (int j = 0; j < 47; j++) {
			requestNum--;
			spyKubernetesRM.cancelNewWorker(resourceProfile1);
			spyKubernetesRM.cancelNewWorker(resourceProfile2);
			Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(requestNum % 4 == 0 ? requestNum / 4 : requestNum / 4 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
			Assert.assertEquals(requestNum % 2 == 0 ? requestNum / 2 : requestNum / 2 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		}
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		List<String> ids = spyKubernetesRM.getPendingWorkerNodes().values().stream()
			.flatMap(e -> e.stream()).map(e -> e.getResourceIdString()).collect(Collectors.toList());
		Collections.sort(ids);
		Assert.assertEquals(3, ids.size());
		Assert.assertArrayEquals(new String[]{createPodName(36), createPodName(37),
			createPodName(38)}, ids.toArray());

		// add removed worker node(id=1) with priority 0, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 1));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());
		// add removed worker node(id=2) with priority 1, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 2));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());

		// add worker node(id=37) with priority 0
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 37));
		Assert.assertEquals(1, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{37L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// add worker node(id=38) with priority 1
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 38));
		Assert.assertEquals(2, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{37L, 38L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// add worker node(id=36) with priority 1
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 36));
		Assert.assertEquals(3, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{36L, 37L, 38L}, getSortedWorkerNodeIds(spyKubernetesRM));

		// no pending now
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// stop worker node(id=36) with priority 1
		// mock that number of pending slot requests is larger than number of pending worker nodes
		Mockito.doReturn(2).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.stopWorker(spyKubernetesRM.getWorkerNodes().get(new ResourceID(createPodName(36))));
		// should left 2 worker node(id=37,38)
		Assert.assertEquals(2, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{37L, 38L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// should request new worker node inside stopWorker, now pending worker nodes: 1
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testGetPreviousWorkerNodes() throws Exception {
		List podPriorityIdList = new ArrayList();
		podPriorityIdList.add(new DefaultPair(0, 1));
		podPriorityIdList.add(new DefaultPair(0, 3));
		podPriorityIdList.add(new DefaultPair(0, 5));
		podPriorityIdList.add(new DefaultPair(2, 4));
		podPriorityIdList.add(new DefaultPair(2, 6));
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(podPriorityIdList);
		KubernetesResourceManager spyKubernetesRM =
			mockSpyKubernetesRM(flinkConf, mockKubernetesClient);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesRM).checkTMRegistered(Matchers.any());

		// start RM, get previous 2 worker nodes
		spyKubernetesRM.start();
		Assert.assertEquals(5, spyKubernetesRM.getNumberAllocatedWorkers());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().size());

		// request worker nodes with two different profiles, priority should be 3 and 4
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		ResourceProfile resourceProfile2 = new ResourceProfile(1.0, 500);
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(3).size());
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(3).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(4).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(4).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(3, spyKubernetesRM.getPendingWorkerNodes().get(4).size());

		// add invalid worker node(id=2) with priority 0, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 2));
		Assert.assertEquals(5, spyKubernetesRM.getWorkerNodes().size());
		// add already exist worker node(id=4) with priority 2, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(2, 4));
		Assert.assertEquals(5, spyKubernetesRM.getWorkerNodes().size());

		// add worker node(id=7) with priority 3
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(3, 7));
		Assert.assertEquals(6, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L, 7L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(3).size());

		// add worker node(id=9) with priority 4
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(4, 9));
		Assert.assertEquals(7, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L, 7L, 9L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(4).size());

		// add worker node(id=8) with priority 3
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(3, 8));
		Assert.assertEquals(8, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().get(3).size());

		// request worker nodes with priority 3
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(3).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testTaskManagerRegisterTimeout() throws Exception {
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesResourceManager spyKubernetesRM =
			mockSpyKubernetesRM(flinkConf, mockKubernetesClient);

		// start RM
		spyKubernetesRM.start();
		Assert.assertEquals(0, spyKubernetesRM.getNumberAllocatedWorkers());

		// request worker nodes with priority 0
		int requestNum = 5;
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		for (int i = 0; i < requestNum; i++) {
			spyKubernetesRM.startNewWorker(resourceProfile1);
			Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(i + 1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}

		// add 2 worker nodes(id=1,2) with priority 0
		int addNum = 2;
		for (int i = 0; i < addNum; i++) {
			spyKubernetesRM
				.handlePodMessage(Watcher.Action.ADDED, createPod(0, i + 1));
			Assert.assertEquals(i + 1, spyKubernetesRM.getWorkerNodes().size());
			Assert.assertEquals(requestNum - 1 - i,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}
		Assert.assertArrayEquals(LongStream.rangeClosed(1, addNum).toArray(), getSortedWorkerNodeIds(spyKubernetesRM));

		Mockito.verify(spyKubernetesRM, Mockito.times(requestNum))
			.requestNewWorkerNode(Mockito.any(), Mockito.anyInt());

		// trigger task manager register timeout, should request new worker nodes
		Mockito.doReturn(requestNum).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesRM::checkTMRegistered);
		Mockito.verify(spyKubernetesRM, Mockito.times(requestNum + addNum))
			.requestNewWorkerNode(Mockito.any(), Mockito.anyInt());
		Assert.assertEquals(0, spyKubernetesRM.getNumberAllocatedWorkers());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(requestNum, spyKubernetesRM.getPendingWorkerNodes().get(0).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testExitWhenReachMaxFailedAttempts() throws Exception {
		// set max failed attempts = 2
		int maxFailedAttempts = 2;
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setInteger(KubernetesConfigOptions.WORKER_NODE_MAX_FAILED_ATTEMPTS, maxFailedAttempts);
		KubernetesClient mockKubernetesClient = createMockKubernetesClient(null);
		KubernetesResourceManager spyKubernetesRM =
			mockSpyKubernetesRM(newFlinkConf, mockKubernetesClient);

		// start RM
		spyKubernetesRM.start();
		Assert.assertEquals(0, spyKubernetesRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().size());

		// request worker nodes with priority 0
		int requestNum = 5;
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		for (int i = 0; i < requestNum; i++) {
			spyKubernetesRM.startNewWorker(resourceProfile1);
			Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(i + 1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}

		// add 2 worker nodes(id=1,2) with priority 0
		int addNum = 2;
		for (int i = 0; i < addNum; i++) {
			spyKubernetesRM
				.handlePodMessage(Watcher.Action.ADDED, createPod(0, i + 1));
			Assert.assertEquals(i + 1, spyKubernetesRM.getWorkerNodes().size());
			Assert.assertEquals(requestNum - 1 - i,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}
		Assert.assertArrayEquals(LongStream.rangeClosed(1, addNum).toArray(), getSortedWorkerNodeIds(spyKubernetesRM));

		Mockito.verify(spyKubernetesRM, Mockito.times(requestNum))
			.requestNewWorkerNode(Mockito.any(), Mockito.anyInt());

		// trigger task manager register timeout, should request new worker nodes
		Mockito.doReturn(requestNum).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesRM::checkTMRegistered);

		// won't add new request for the last attempt
		Assert.assertEquals(requestNum - 1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());

		// will stop internally
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}
}
