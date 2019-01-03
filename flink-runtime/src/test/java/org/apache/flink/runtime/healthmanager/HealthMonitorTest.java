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

package org.apache.flink.runtime.healthmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Tests for Health Monitor.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HealthMonitor.class})
public class HealthMonitorTest {

	/**
	 * test case which will trigger heap memory adjustment twice.
	 */
	@Test
	public void testMonitorWithOOM() throws Exception {
		MetricProvider metricProvider = Mockito.mock(MetricProvider.class);

		RestServerClient restServerClient = Mockito.mock(RestServerClient.class);

		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
				1, new ExecutorThreadFactory("health-manager"));

		JobID jobID = new JobID();
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();

		// job level configuration.
		Configuration config = new Configuration();
		config.setString("healthmonitor.health.check.interval.ms", "3000");
		config.setString("heap.memory.scale.timeout.ms", "10000");
		config.setString("heap.memory.scale.ratio", "1");

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig>  vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config = new RestServerClient.VertexConfig(
				1, 3, new ResourceSpec.Builder().setHeapMemoryInMB(10).build());
		RestServerClient.VertexConfig vertex2Config = new RestServerClient.VertexConfig(
				1, 3, new ResourceSpec.Builder().setHeapMemoryInMB(20).build());
		vertexConfigs.put(vertex1, vertex1Config);
		vertexConfigs.put(vertex2, vertex2Config);

		// job vertex config after first round rescale.
		Map<JobVertexID, RestServerClient.VertexConfig>  vertexConfigs2 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config2 = new RestServerClient.VertexConfig(
				1, 3, new ResourceSpec.Builder().setHeapMemoryInMB(20).build());
		RestServerClient.VertexConfig vertex2Config2 = new RestServerClient.VertexConfig(
				1, 3, new ResourceSpec.Builder().setHeapMemoryInMB(20).build());
		vertexConfigs2.put(vertex1, vertex1Config2);
		vertexConfigs2.put(vertex2, vertex2Config2);

		Map<JobVertexID, List<JobVertexID>> inputNodes = new HashMap<>();

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
				.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
				.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		Map<JobVertexID, List<JobException>> exceptions = new HashMap<>();
		List<JobException> oomError = new LinkedList<>();
		OutOfMemoryError error = new OutOfMemoryError("Java heap space");
		oomError.add(new JobException(error.getMessage(), error));
		exceptions.put(vertex1, oomError);

		// return oom exception twice to trigger rescale twice.
		Mockito.when(restServerClient.getFailover(Mockito.eq(jobID), Mockito.anyLong(), Mockito.anyLong()))
			.thenReturn(exceptions).thenReturn(exceptions).thenReturn(new HashMap<>());

		Map<ExecutionVertexID, ExecutionState> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0), ExecutionState.RUNNING);
		allTaskStats.put(new ExecutionVertexID(vertex2, 0), ExecutionState.SCHEDULED);
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, ExecutionState> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0), ExecutionState.RUNNING);
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0), ExecutionState.RUNNING);
		RestServerClient.JobStatus jobStatus2 = new RestServerClient.JobStatus(allTaskStats2);

		// mock slow scheduling.
		Mockito.when(restServerClient.getJobStatus(Mockito.eq(jobID)))
				.thenReturn(jobStatus).thenReturn(jobStatus2);

		HealthMonitor monitor = new HealthMonitor(
			jobID,
			metricProvider,
			restServerClient,
			executorService,
			new Configuration()
		);

		monitor.start();

		Thread.sleep(10000);

		monitor.stop();

		// verify rpc calls.
		Mockito.verify(restServerClient, Mockito.times(1))
				.rescale(
						Mockito.eq(jobID),
						Mockito.eq(vertex1),
						Mockito.eq(1),
						Mockito.eq(ResourceSpec.newBuilder().setHeapMemoryInMB(20).build()));

		Mockito.verify(restServerClient, Mockito.times(1))
				.rescale(
						Mockito.eq(jobID),
						Mockito.eq(vertex1),
						Mockito.eq(1),
						Mockito.eq(ResourceSpec.newBuilder().setHeapMemoryInMB(40).build()));

		Mockito.verify(restServerClient, Mockito.times(3))
				.getJobStatus(Mockito.eq(jobID));

		Mockito.verify(restServerClient, Mockito.times(2))
				.getJobConfig(Mockito.eq(jobID));
	}

}
