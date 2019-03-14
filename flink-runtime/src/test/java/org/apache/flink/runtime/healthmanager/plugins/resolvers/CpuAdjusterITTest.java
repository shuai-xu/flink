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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.JobTMMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighCpuDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.LowCpuDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.TestingJobStableDetector;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyVararg;

/**
 * Tests for CpuAdjusterITTest.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricUtils.class)
public class CpuAdjusterITTest {
	/**
	 * test cpu increase.
	 */
	@Test
	public void testCpuIncrease() throws Exception {
		MetricProvider metricProvider = Mockito.mock(MetricProvider.class);

		RestServerClient restServerClient = Mockito.mock(RestServerClient.class);

		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
			1, new ExecutorThreadFactory("health-manager"));

		JobID jobID = new JobID();
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		ExecutionVertexID executionVertexID1 = new ExecutionVertexID(vertex1, 0);

		// job level configuration.
		Configuration config = new Configuration();
		config.setLong(HealthMonitor.HEALTH_CHECK_INTERNAL, 1000L);
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_TIME_OUT, 10000L);
		config.setDouble(HealthMonitorOptions.RESOURCE_SCALE_UP_RATIO, 2.0);
		config.setDouble(HighCpuDetector.HIGH_CPU_SEVERE_THRESHOLD, 0.9);
		config.setLong(HealthMonitorOptions.RESOURCE_OPPORTUNISTIC_ACTION_DELAY, 100L);
		config.setString(HealthMonitor.DETECTOR_CLASSES, HighCpuDetector.class.getCanonicalName() + "," +
			TestingJobStableDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, CpuAdjuster.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(1.0).build());
		RestServerClient.VertexConfig vertex2Config = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(1.0).build());
		vertexConfigs.put(vertex1, vertex1Config);
		vertexConfigs.put(vertex2, vertex2Config);

		// job vertex config after first round rescale.
		Map<JobVertexID, RestServerClient.VertexConfig>  vertexConfigs2 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config2 = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(2.0).build());
		RestServerClient.VertexConfig vertex2Config2 = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(1.0).build());
		vertexConfigs2.put(vertex1, vertex1Config2);
		vertexConfigs2.put(vertex2, vertex2Config2);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Collections.emptyList());

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		JobTMMetricSubscription usageSub = Mockito.mock(JobTMMetricSubscription.class);
		JobTMMetricSubscription capacitySub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(usageSub.getValue()).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> usage1 = new HashMap<>();
				usage1.put("tmId", Tuple2.of(now, 1.0));
				return usage1;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
				InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> usage1 = new HashMap<>();
				usage1.put("tmId", Tuple2.of(now, 1.0));
				return usage1;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
				InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> usage1 = new HashMap<>();
				usage1.put("tmId", Tuple2.of(now, 1.0));
				return usage1;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> usage1 = new HashMap<>();
				usage1.put("tmId", Tuple2.of(now, 2.0));
				return usage1;
			}
		});
		Mockito.when(capacitySub.getValue()).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
				InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> capacity = new HashMap<>();
				capacity.put("tmId", Tuple2.of(now, 1.0));
				return capacity;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
				InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> capacity = new HashMap<>();
				capacity.put("tmId", Tuple2.of(now, 1.0));
				return capacity;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> capacity = new HashMap<>();
				capacity.put("tmId", Tuple2.of(now, 1.0));
				return capacity;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> capacity = new HashMap<>();
				capacity.put("tmId", Tuple2.of(now, 2.0));
				return capacity;
			}
		}).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				Map<String, Tuple2<Long, Double>> capacity = new HashMap<>();
				capacity.put("tmId", Tuple2.of(now, 4.0));
				return capacity;
			}
		});

		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class), Mockito.eq("Status.ProcessTree.CPU.Usage"), Mockito.anyLong(), Mockito.any(TimelineAggType.class)))
			.thenReturn(usageSub);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class), Mockito.eq("Status.ProcessTree.CPU.Allocated"), Mockito.anyLong(), Mockito.any(TimelineAggType.class)))
			.thenReturn(capacitySub);

		Mockito.when(restServerClient.getTaskManagerTasks(Mockito.eq("tmId")))
			.thenReturn(Arrays.asList(executionVertexID1));

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

		CheckpointStatistics checkpointStatistics = Mockito.mock(CheckpointStatistics.class);
		Mockito.when(checkpointStatistics.getLatestAckTimestamp()).thenReturn(System.currentTimeMillis());
		Mockito.when(restServerClient.getLatestCheckPointStates(Mockito.eq(jobID)))
			.thenReturn(checkpointStatistics);

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
		vertexParallelismResource.put(vertex1, new Tuple2<>(1, ResourceSpec.newBuilder().setCpuCores(2.0).build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));

		vertexParallelismResource.clear();
		vertexParallelismResource.put(vertex1, new Tuple2<>(1, ResourceSpec.newBuilder().setCpuCores(4.0).build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}

	/**
	 * test cpu dncrease.
	 */
	@Test
	public void testCpuDecrease() throws Exception {
		MetricProvider metricProvider = Mockito.mock(MetricProvider.class);

		RestServerClient restServerClient = Mockito.mock(RestServerClient.class);

		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
			1, new ExecutorThreadFactory("health-manager"));

		JobID jobID = new JobID();
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		ExecutionVertexID executionVertexID1 = new ExecutionVertexID(vertex1, 0);

		// job level configuration.
		Configuration config = new Configuration();
		config.setLong(HealthMonitor.HEALTH_CHECK_INTERNAL, 1000L);
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_TIME_OUT, 10000L);
		config.setDouble(LowCpuDetector.LOW_CPU_THRESHOLD, 0.6);
		config.setDouble(HealthMonitorOptions.RESOURCE_SCALE_DOWN_RATIO, 1.5);
		config.setLong(HealthMonitorOptions.RESOURCE_SCALE_DOWN_WAIT_TIME, -1L);
		config.setLong(HealthMonitorOptions.RESOURCE_OPPORTUNISTIC_ACTION_DELAY, -1L);
		config.setString(HealthMonitor.DETECTOR_CLASSES, LowCpuDetector.class.getCanonicalName() + ","
			+ TestingJobStableDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, CpuAdjuster.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(4.0).build());
		RestServerClient.VertexConfig vertex2Config = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(1.0).build());
		vertexConfigs.put(vertex1, vertex1Config);
		vertexConfigs.put(vertex2, vertex2Config);

		// job vertex config after first round rescale.
		Map<JobVertexID, RestServerClient.VertexConfig>  vertexConfigs2 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config2 = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(3.0).build());
		RestServerClient.VertexConfig vertex2Config2 = new RestServerClient.VertexConfig(
			1, 3, new ResourceSpec.Builder().setCpuCores(1.0).build());
		vertexConfigs2.put(vertex1, vertex1Config2);
		vertexConfigs2.put(vertex2, vertex2Config2);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Collections.emptyList());

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		JobTMMetricSubscription usageSub = Mockito.mock(JobTMMetricSubscription.class);
		JobTMMetricSubscription capacitySub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(usageSub.getValue()).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			double[] values = new double[]{2, 2, 2};
			int callCount = 0;
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				callCount++;
				Map<String, Tuple2<Long, Double>> usage = new HashMap<>();
				usage.put("tmId", Tuple2.of(now, values[callCount < values.length ? callCount - 1 : values.length - 1]));
				return usage;
			}
		});
		Mockito.when(capacitySub.getValue()).thenAnswer(new Answer<Map<String, Tuple2<Long, Double>>>() {
			double[] values = new double[]{4, 4, 3};
			int callCount = 0;
			@Override
			public Map<String, Tuple2<Long, Double>> answer(
					InvocationOnMock invocationOnMock) throws Throwable {
				long now = System.currentTimeMillis();
				callCount++;
				Map<String, Tuple2<Long, Double>> capacity = new HashMap<>();
				capacity.put("tmId", Tuple2.of(now, values[callCount < values.length ? callCount - 1 : values.length - 1]));
				return capacity;
			}
		});

		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class), Mockito.eq("Status.ProcessTree.CPU.Usage"), Mockito.anyLong(), Mockito.any(TimelineAggType.class)))
			.thenReturn(usageSub);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class), Mockito.eq("Status.ProcessTree.CPU.Allocated"), Mockito.anyLong(), Mockito.any(TimelineAggType.class)))
			.thenReturn(capacitySub);

		Mockito.when(restServerClient.getTaskManagerTasks(Mockito.eq("tmId")))
			.thenReturn(Arrays.asList(executionVertexID1));

		PowerMockito.mockStatic(MetricUtils.class);
		Mockito.when(MetricUtils.validateTmMetric(Mockito.any(HealthMonitor.class), anyLong(), anyVararg())).thenReturn(true);

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

		CheckpointStatistics checkpointStatistics = Mockito.mock(CheckpointStatistics.class);
		Mockito.when(checkpointStatistics.getLatestAckTimestamp()).thenReturn(System.currentTimeMillis());
		Mockito.when(restServerClient.getLatestCheckPointStates(Mockito.eq(jobID)))
			.thenReturn(checkpointStatistics);

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
		vertexParallelismResource.put(vertex1, new Tuple2<>(1, ResourceSpec.newBuilder().setCpuCores(3.0).build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}
}
