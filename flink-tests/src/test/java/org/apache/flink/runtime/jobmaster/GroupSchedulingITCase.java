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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for group scheduling.
 *
 * @see JobMaster
 */
public class GroupSchedulingITCase extends AbstractTestBase {

	private MiniClusterClient clusterClient;

	private JobGraph jobGraph;

	@Before
	public void setUp() throws Exception {

		Assume.assumeTrue(
			"ClusterClient is not an instance of MiniClusterClient",
			miniClusterResource.getClusterClient() instanceof MiniClusterClient);

		clusterClient = (MiniClusterClient) miniClusterResource.getClusterClient();
		clusterClient.setDetached(true);

	}

	@Test
	public void testJobExecutionGroupByGroup() throws Exception {

		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(2);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		vertex4.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		jobGraph = new JobGraph(new JobID(), "test job", vertex1, vertex2, vertex3, vertex4);

		clusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader(), false);

		waitForJob(60);
	}

	@Test
	public void testJobExecutionWithGroupSplit() throws Exception {

		final JobVertex vertex1 = new JobVertex("vertex1");
		vertex1.setInvokableClass(NoOpInvokable.class);
		vertex1.setParallelism(2);

		final JobVertex vertex2 = new JobVertex("vertex2");
		vertex2.setInvokableClass(NoOpInvokable.class);
		vertex2.setParallelism(2);

		final JobVertex vertex3 = new JobVertex("vertex3");
		vertex3.setInvokableClass(NoOpInvokable.class);
		vertex3.setParallelism(2);

		final JobVertex vertex4 = new JobVertex("vertex4");
		vertex4.setInvokableClass(NoOpInvokable.class);
		vertex4.setParallelism(2);

		vertex2.connectNewDataSetAsInput(vertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertex4.connectNewDataSetAsInput(vertex3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		jobGraph = new JobGraph(new JobID(), "test job", vertex1, vertex2, vertex3, vertex4);
		jobGraph.getSchedulingConfiguration().setBoolean("job.scheduling.allow-auto-partition", true);

		clusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader(), false);

		waitForJob(600);
	}

	private void waitForJob(int timeout) throws Exception {
		for (int i = 0; i < timeout; i++) {
			try {
				final JobStatus jobStatus = clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
				//assertThat(jobStatus.isGloballyTerminalState(), equalTo(false));
				System.out.println("The job status is " + jobStatus);
				if (jobStatus == JobStatus.FINISHED) {
					return;
				}
			} catch (ExecutionException ignored) {
				// JobManagerRunner is not yet registered in Dispatcher
			}
			Thread.sleep(1000);
		}
		System.out.println("Reaching timeout, the job status is still not finished.");
		throw new AssertionError("Job did not become FINISHED within timeout.");
	}

	public static class NoOpInvokable extends AbstractInvokable {

		public NoOpInvokable(final Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			return;
		}

	}

}
