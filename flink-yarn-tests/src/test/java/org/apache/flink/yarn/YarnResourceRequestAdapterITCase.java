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

import org.apache.flink.api.common.operators.ResourceConstraints;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.StrictlyMatchingResourceConstraints;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceConstraintsOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

/**
 * Test cases for the deployment of Yarn Flink clusters with resource request adapter.
 */
public class YarnResourceRequestAdapterITCase extends YarnTestBase {

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-resource-request-adapter-it-case");
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	/**
	 * Testing resource request with resource constraints and non-existed extended resources in per-job mode,
	 * Expect that AM should just ignore these resource constraints and extended resources and won't be affected.
	 */
	@Test
	public void testResourceRequestWithConstraintsAndExtendedResource() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
		// disable slot sharing group
		configuration.setBoolean(JobManagerOptions.SLOT_ENABLE_SHARED_SLOT, false);
		final YarnClient yarnClient = getYarnClient();
		ResourceSpec resourceSpec = new ResourceSpec.Builder().setCpuCores(1).setHeapMemoryInMB(1024).build();
		// add extended resources
		resourceSpec.getExtendedResources().put(ResourceSpec.GPU_NAME,
			new CommonExtendedResource(ResourceSpec.GPU_NAME, 1));
		resourceSpec.getExtendedResources().put(ResourceSpec.MANAGED_MEMORY_NAME,
			new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, 1));
		resourceSpec.getExtendedResources().put(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
			new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, 1));

		// Set up resource constraints to update container priority
		ResourceConstraints constraints = new StrictlyMatchingResourceConstraints();
		constraints.addConstraint(ResourceConstraintsOptions.YARN_EXECUTION_TYPE, ResourceConstraintsOptions.YARN_EXECUTION_TYPE_OPPORTUNISTIC);

		try (final YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
			configuration,
			getYarnConfiguration(),
			System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR),
			yarnClient,
			true)) {

			yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));

			final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(768)
				.setTaskManagerMemoryMB(1024)
				.setSlotsPerTaskManager(1)
				.setNumberTaskManagers(1)
				.createClusterSpecification();

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(2);

			// set resource constraints
			env.addSource(new InfiniteSource()).setResources(resourceSpec).setResourceConstraints(constraints)
				.shuffle()
				.addSink(new DiscardingSink<>()).setResources(resourceSpec).setResourceConstraints(constraints);

			final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

			File testingJar = YarnTestBase.findFile("..", new TestingYarnClusterDescriptor.TestJarFinder("flink-yarn-tests"));

			jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));

			ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(
				clusterSpecification,
				jobGraph,
				false);

			// Wait until app got enough containers(1 AM + 4 TMs), timeout is 30s.
			GenericTestUtils.waitFor(() -> getRunningContainers() == 5, 100, 30000);
			Assert.assertEquals(5, getRunningContainers());

			clusterClient.shutdown();
		}
	}

	private static class InfiniteSource implements ParallelSourceFunction<Integer> {

		private static final long serialVersionUID = 1642561062000662861L;
		private volatile boolean running;
		private final Random random;

		InfiniteSource() {
			running = true;
			random = new Random();
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(random.nextInt());
				}

				Thread.sleep(5L);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
