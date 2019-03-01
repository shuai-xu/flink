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
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.detectors.BackPressureDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.DelayIncreasingDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighDelayDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.OverParallelizedDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.TestingJobStableDetector;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Tests for ParallelismScaler.
 */
public class ParallelismScalerITTest {
	/**
	 * test parallelism scale up for delay.
	 * Vertex v1 initial parallelism = 2, target parallelism = 2, rescale to 4.
	 * Vertex v2 initial parallelism = 2, target parallelism = 1, edge (v1, v2) is forward, rescale to 4.
	 */
	@Test
	public void testScaleUpForDelay() throws Exception {
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
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_TIME_OUT, 10000L);
		config.setDouble(HealthMonitorOptions.PARALLELISM_MIN_RATIO, 2.0);
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL, 60000);
		config.setString(HealthMonitor.DETECTOR_CLASSES,
				HighDelayDetector.class.getCanonicalName() + "," +
				DelayIncreasingDetector.class.getCanonicalName() + "," +
				TestingJobStableDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, ParallelismScaler.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs1 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			2, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			2, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs1.put(vertex1, vertex1Config1);
		vertexConfigs1.put(vertex2, vertex2Config1);

		// job graph topology
		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Arrays.asList(Tuple2.of(vertex1, "FORWARD")));

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes));

		long now = System.currentTimeMillis();

		TaskMetricSubscription zeroSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(zeroSub.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex1

		TaskMetricSubscription v1InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1InputTps.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1LatencyCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountRange.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1LatencySumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumRange.getValue()).thenReturn(new Tuple2<>(now, 2.0e9));

		TaskMetricSubscription v1WaitOutputCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1WaitOutputSumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumRange.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1Delay.getValue()).thenReturn(new Tuple2<>(now, 11 * 60 * 1000.0));

		TaskMetricSubscription v1DelayRate = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1DelayRate.getValue()).thenReturn(new Tuple2<>(now, 10.0)).thenReturn(new Tuple2<>(now, 10.0)).thenReturn(new Tuple2<>(now, 10.0))
			.thenReturn(new Tuple2<>(now, 0.0)).thenReturn(new Tuple2<>(now, 0.0)).thenReturn(new Tuple2<>(now, 0.0));

		// vertex2

		TaskMetricSubscription v2InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2InputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencyCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencySumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumRange.getValue()).thenReturn(new Tuple2<>(now, 1.0e9));

		TaskMetricSubscription v2WaitOutputCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2WaitOutputSumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumRange.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		initMockMetrics(metricProvider, vertex1, vertex2, zeroSub,
			v1InputTps, v1OutputTps, v1LatencyCountRange, v1LatencySumRange,
			v1WaitOutputCountRange, v1WaitOutputSumRange, v1Delay, v1DelayRate,
			v2InputTps, v2OutputTps, v2LatencyCountRange, v2LatencySumRange,
			v2WaitOutputCountRange, v2WaitOutputSumRange);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		RestServerClient.JobStatus jobStatus2 = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		RestServerClient.JobStatus jobStatus3 = new RestServerClient.JobStatus(allTaskStats2);

		// mock slow scheduling.
		Mockito.when(restServerClient.getJobStatus(Mockito.eq(jobID)))
			.thenReturn(jobStatus).thenReturn(jobStatus2).thenReturn(jobStatus3);

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
		vertexParallelismResource.put(vertex1, new Tuple2<>(4, ResourceSpec.newBuilder().build()));
		vertexParallelismResource.put(vertex2, new Tuple2<>(4, ResourceSpec.newBuilder().build()));
		Mockito.verify(restServerClient, Mockito.atLeast(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}

	/**
	 * test parallelism scale up for back pressure.
	 * Vertex v1 initial parallelism = 2, target parallelism = 1, 50% wait output time, do not rescale.
	 * Vertex v2 initial parallelism = 1, target parallelism = 1, rescale to 2.
	 */
	@Test
	public void testScaleUpForBackPressure() throws Exception {
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
		config.setString("healthmonitor.back-pressure.threshold.ms", "0");
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_TIME_OUT, 10000);
		config.setDouble(HealthMonitorOptions.PARALLELISM_MIN_RATIO, 2.0);
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL, 60000);
		config.setString(HealthMonitor.DETECTOR_CLASSES, BackPressureDetector.class.getCanonicalName() + "," +
			TestingJobStableDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, ParallelismScaler.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs1 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			2, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs1.put(vertex1, vertex1Config1);
		vertexConfigs1.put(vertex2, vertex2Config1);

		// job graph topology
		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Arrays.asList(Tuple2.of(vertex1, "HASH")));

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes));

		long now = System.currentTimeMillis();

		TaskMetricSubscription zeroSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(zeroSub.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex1

		TaskMetricSubscription v1InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1InputTps.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1LatencyCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountRange.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1LatencySumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumRange.getValue()).thenReturn(new Tuple2<>(now, 2.0e9));

		TaskMetricSubscription v1WaitOutputCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1WaitOutputSumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumRange.getValue()).thenReturn(new Tuple2<>(now, 1.0e9 * 60))
			.thenReturn(new Tuple2<>(now, 1.0e9 * 60)).thenReturn(new Tuple2<>(now, 1.0e9 * 60)).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1Delay.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1DelayRate = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1DelayRate.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex2

		TaskMetricSubscription v2InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2InputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencyCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencySumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumRange.getValue()).thenReturn(new Tuple2<>(now, 1.0e9));

		TaskMetricSubscription v2WaitOutputCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2WaitOutputSumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumRange.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		initMockMetrics(metricProvider, vertex1, vertex2, zeroSub,
			v1InputTps, v1OutputTps, v1LatencyCountRange, v1LatencySumRange,
			v1WaitOutputCountRange, v1WaitOutputSumRange, v1Delay, v1DelayRate,
			v2InputTps, v2OutputTps, v2LatencyCountRange, v2LatencySumRange,
			v2WaitOutputCountRange, v2WaitOutputSumRange);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		RestServerClient.JobStatus jobStatus2 = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		RestServerClient.JobStatus jobStatus3 = new RestServerClient.JobStatus(allTaskStats2);

		// mock slow scheduling.
		Mockito.when(restServerClient.getJobStatus(Mockito.eq(jobID)))
			.thenReturn(jobStatus).thenReturn(jobStatus2).thenReturn(jobStatus3);

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
		vertexParallelismResource.put(vertex2, new Tuple2<>(2, ResourceSpec.newBuilder().build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}

	private void initMockMetrics(MetricProvider metricProvider, JobVertexID vertex1,
		JobVertexID vertex2, TaskMetricSubscription zeroSub, TaskMetricSubscription v1InputCount,
		TaskMetricSubscription v1OutputCount, TaskMetricSubscription v1LatencyCountRange,
		TaskMetricSubscription v1LatencySumRange, TaskMetricSubscription v1WaitOutputCountRange,
		TaskMetricSubscription v1WaitOutputSumRange, TaskMetricSubscription v1Delay,
		TaskMetricSubscription v1DelayRate, TaskMetricSubscription v2InputCount,
		TaskMetricSubscription v2OutputCount, TaskMetricSubscription v2LatencyCountRange,
		TaskMetricSubscription v2LatencySumRange, TaskMetricSubscription v2WaitOutputCountRange,
		TaskMetricSubscription v2WaitOutputSumRange) {
		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.any(JobID.class),
			Mockito.any(JobVertexID.class),
			Mockito.anyString(),
			Mockito.any(MetricAggType.class),
			Mockito.anyLong(),
			Mockito.any(TimelineAggType.class)
		)).then((Answer<TaskMetricSubscription>) invocation -> {
			JobVertexID vertexId = (JobVertexID) invocation.getArguments()[1];
			String metricName = (String) invocation.getArguments()[2];
			TimelineAggType aggType = (TimelineAggType) invocation.getArguments()[5];

			if (vertexId.equals(vertex1)) {
				if (metricName.equals(MetricNames.TASK_INPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v1InputCount;
					}
				} else if (metricName.equals(MetricNames.TASK_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v1OutputCount;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_COUNT)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v1LatencyCountRange;
					}
					if (aggType.equals(TimelineAggType.RANGE)) {
						return v1LatencyCountRange;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_SUM)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v1LatencySumRange;
					}
					if (aggType.equals(TimelineAggType.RANGE)) {
						return v1LatencySumRange;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v1WaitOutputCountRange;
					}
					if (aggType.equals(TimelineAggType.RANGE)) {
						return v1WaitOutputCountRange;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_SUM)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v1WaitOutputSumRange;
					}
					if (aggType.equals(TimelineAggType.RANGE)) {
						return v1WaitOutputSumRange;
					}
				} else if (metricName.equals(MetricNames.SOURCE_DELAY)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v1Delay;
					} else if (aggType.equals(TimelineAggType.RATE)) {
						return v1DelayRate;
					}
				}
			} else if (vertexId.equals(vertex2)) {
				if (metricName.equals(MetricNames.TASK_INPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v2InputCount;
					}
				} else if (metricName.equals(MetricNames.TASK_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v2OutputCount;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_COUNT)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v2LatencyCountRange;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_SUM)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v2LatencySumRange;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v2WaitOutputCountRange;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_SUM)) {
					if (aggType.equals(TimelineAggType.LATEST)) {
						return v2WaitOutputSumRange;
					}
				}
			}
			return zeroSub;
		});

		JobTMMetricSubscription tmMetricSub = Mockito.mock(JobTMMetricSubscription.class);
		Mockito.when(tmMetricSub.getValue()).thenReturn(null);
		Mockito.when(metricProvider.subscribeAllTMMetric(
			Mockito.any(JobID.class),
			Mockito.anyString(),
			Mockito.anyLong(),
			Mockito.any(TimelineAggType.class)
		)).thenReturn(tmMetricSub);
	}

	/**
	 * test parallelism scale down.
	 * Vertex v1 initial parallelism = 4, target parallelism = 2, rescale to 2.
	 * Vertex v2 initial parallelism = 4, target parallelism = 1, edge (v1, v2) is forward, rescale to 2.
	 */
	@Test
	public void testScaleDown() throws Exception {
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
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_TIME_OUT, 10000);
		config.setDouble(HealthMonitorOptions.PARALLELISM_MIN_RATIO, 1);
		config.setLong(HealthMonitorOptions.PARALLELISM_SCALE_INTERVAL, 60000);
		config.setString(HealthMonitor.DETECTOR_CLASSES, OverParallelizedDetector.class.getCanonicalName() + "," +
			TestingJobStableDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, ParallelismScaler.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs1 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			4, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			4, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs1.put(vertex1, vertex1Config1);
		vertexConfigs1.put(vertex2, vertex2Config1);

		// job vertex config after resale.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs2 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config2 = new RestServerClient.VertexConfig(
			2, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config2 = new RestServerClient.VertexConfig(
			2, 4, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		vertexConfigs2.put(vertex1, vertex1Config2);
		vertexConfigs2.put(vertex2, vertex2Config2);

		// job graph topology
		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Arrays.asList(Tuple2.of(vertex1, "FORWARD")));

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		long now = System.currentTimeMillis();

		TaskMetricSubscription zeroSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(zeroSub.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex1

		TaskMetricSubscription v1InputCount = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1InputCount.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1OutputCount = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1OutputCount.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1LatencyCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountRange.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1LatencySumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumRange.getValue()).thenReturn(new Tuple2<>(now, 2.0e9));

		TaskMetricSubscription v1WaitOutputCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1WaitOutputSumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumRange.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1Delay.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex2

		TaskMetricSubscription v2InputCount = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2InputCount.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2OutputCount = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2OutputCount.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencyCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencySumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumRange.getValue()).thenReturn(new Tuple2<>(now, 1.0e9));

		TaskMetricSubscription v2WaitOutputCountRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountRange.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2WaitOutputSumRange = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumRange.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2Delay.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		initMockMetrics(metricProvider, vertex1, vertex2, zeroSub,
			v1InputCount, v1OutputCount, v1LatencyCountRange, v1LatencySumRange,
			v1WaitOutputCountRange, v1WaitOutputSumRange, v1Delay, zeroSub,
			v2InputCount, v2OutputCount, v2LatencyCountRange, v2LatencySumRange,
			v2WaitOutputCountRange, v2WaitOutputSumRange);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		RestServerClient.JobStatus jobStatus2 = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(now, ExecutionState.RUNNING));
		RestServerClient.JobStatus jobStatus3 = new RestServerClient.JobStatus(allTaskStats2);

		// mock slow scheduling.
		Mockito.when(restServerClient.getJobStatus(Mockito.eq(jobID)))
			.thenReturn(jobStatus).thenReturn(jobStatus2).thenReturn(jobStatus3);

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
		vertexParallelismResource.put(vertex1, new Tuple2<>(2, ResourceSpec.newBuilder().build()));
		vertexParallelismResource.put(vertex2, new Tuple2<>(2, ResourceSpec.newBuilder().build()));
		Mockito.verify(restServerClient, Mockito.atLeast(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}
}
