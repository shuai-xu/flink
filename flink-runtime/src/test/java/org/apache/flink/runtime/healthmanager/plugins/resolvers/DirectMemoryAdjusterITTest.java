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

package org.apache.flink.runtime.healthmanager.plugins.resolvers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.plugins.detectors.DirectOOMDetector;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Tests for DirectMemoryAdjuster.
 */
public class DirectMemoryAdjusterITTest {
	/**
	 * test direct memory adjustment triggered by direct oom.
	 */
	@Test
	public void testDirectOOMTriggerAdjustment() throws Exception {
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
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_TIME_OUT, 10000L);
		config.setDouble(HealthMonitorOptions.RESOURCE_SCALE_UP_RATIO, 2.0);
		config.setString(HealthMonitor.DETECTOR_CLASSES, DirectOOMDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, DirectMemoryAdjuster.class.getCanonicalName());
		config.setDouble(HealthMonitorOptions.RESOURCE_SCALE_MIN_DIFF_RATIO, 0);
		config.setInteger(HealthMonitorOptions.RESOURCE_SCALE_MIN_DIFF_MEM, 0);

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setDirectMemoryInMB(10).build());
		RestServerClient.VertexConfig vertex2Config = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setDirectMemoryInMB(20).build());
		vertexConfigs.put(vertex1, vertex1Config);
		vertexConfigs.put(vertex2, vertex2Config);

		// job vertex config after first round rescale.
		Map<JobVertexID, RestServerClient.VertexConfig>  vertexConfigs2 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config2 = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setDirectMemoryInMB(20).build());
		RestServerClient.VertexConfig vertex2Config2 = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setDirectMemoryInMB(20).build());
		vertexConfigs2.put(vertex1, vertex1Config2);
		vertexConfigs2.put(vertex2, vertex2Config2);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Collections.emptyList());

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		Map<JobVertexID, List<JobException>> exceptions = new HashMap<>();
		List<JobException> oomError = new LinkedList<>();
		OutOfMemoryError error = new OutOfMemoryError("Direct buffer memory");
		oomError.add(new JobException(error.getMessage(), error));
		exceptions.put(vertex1, oomError);

		// return oom exception twice to trigger rescale twice.
		Mockito.when(restServerClient.getFailover(Mockito.eq(jobID), Mockito.anyLong(), Mockito.anyLong()))
			.thenReturn(exceptions).thenReturn(exceptions).thenReturn(new HashMap<>());

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
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
		Map<JobVertexID, Tuple2<Integer, ResourceSpec>> vertexParallelismResource = new HashMap<>();
		vertexParallelismResource.put(vertex1, new Tuple2<>(1, ResourceSpec.newBuilder().setDirectMemoryInMB(20).build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));

		vertexParallelismResource.clear();
		vertexParallelismResource.put(vertex1, new Tuple2<>(1, ResourceSpec.newBuilder().setDirectMemoryInMB(40).build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}
}
