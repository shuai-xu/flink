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
import org.apache.flink.runtime.healthmanager.plugins.detectors.DelayIncreasingDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.FailoverDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.FrequentFullGCDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighDelayDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.LowDelayDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.OverParallelizedDetector;
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
public class ParallelismScalerTest {
	/**
	 * test parallelism scale up.
	 */
	@Test
	public void testScaleUp() throws Exception {
		MetricProvider metricProvider = Mockito.mock(MetricProvider.class);
		RestServerClient restServerClient = Mockito.mock(RestServerClient.class);
		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
			1, new ExecutorThreadFactory("health-manager"));

		JobID jobID = new JobID();
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();

		// job level configuration.
		Configuration config = new Configuration();
		config.setString("healthmonitor.health.check.interval.ms", "3000");
		config.setString("parallelism.scale.timeout.ms", "10000");
		config.setString("parallelism.up-scale.tps.ratio", "2.0");
		config.setString(HealthMonitor.DETECTOR_CLASSES,
			FrequentFullGCDetector.class.getCanonicalName() + "," +
			FailoverDetector.class.getCanonicalName() + "," +
			HighDelayDetector.class.getCanonicalName() + "," +
			DelayIncreasingDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, ParallelismScaler.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs1 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		RestServerClient.VertexConfig vertex3Config1 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(5));
		vertexConfigs1.put(vertex1, vertex1Config1);
		vertexConfigs1.put(vertex2, vertex2Config1);
		vertexConfigs1.put(vertex3, vertex3Config1);

		// job graph topology
		Map<JobVertexID, List<JobVertexID>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Arrays.asList(vertex1));
		inputNodes.put(vertex3, Collections.emptyList());

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes));
			//.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		long now = System.currentTimeMillis();

		TaskMetricSubscription zeroSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(zeroSub.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex1

		TaskMetricSubscription v1InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1InputTps.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1LatencyCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1LatencyCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountMax.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1LatencySumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1LatencySumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumMax.getValue()).thenReturn(new Tuple2<>(now, 1.0e9));

		TaskMetricSubscription v1WaitOutputCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1WaitOutputCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountMax.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1WaitOutputSumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1WaitOutputSumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumMax.getValue()).thenReturn(new Tuple2<>(now, 0.5e9));

		TaskMetricSubscription v1Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1Delay.getValue()).thenReturn(new Tuple2<>(now, 10 * 60 * 1000.0));

		TaskMetricSubscription v1DelayRate = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1DelayRate.getValue()).thenReturn(new Tuple2<>(now, 10.0)).thenReturn(new Tuple2<>(now, 10.0)).thenReturn(new Tuple2<>(now, 10.0))
			.thenReturn(new Tuple2<>(now, 0.0)).thenReturn(new Tuple2<>(now, 0.0)).thenReturn(new Tuple2<>(now, 0.0));

		// vertex2

		TaskMetricSubscription v2InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2InputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencyCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2LatencyCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountMax.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencySumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2LatencySumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumMax.getValue()).thenReturn(new Tuple2<>(now, 1.0e9));

		TaskMetricSubscription v2WaitOutputCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2WaitOutputCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountMax.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2WaitOutputSumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2WaitOutputSumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumMax.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		initMockMetrics(metricProvider, vertex1, vertex2, zeroSub,
				v1InputTps, v1OutputTps, v1LatencyCountMin, v1LatencyCountMax, v1LatencySumMin,
				v1LatencySumMax, v1WaitOutputCountMin, v1WaitOutputCountMax, v1WaitOutputSumMin,
				v1WaitOutputSumMax, v1Delay, v1DelayRate,
				v2InputTps, v2OutputTps, v2LatencyCountMin, v2LatencyCountMax, v2LatencySumMin,
				v2LatencySumMax, v2WaitOutputCountMin, v2WaitOutputCountMax, v2WaitOutputSumMin,
				v2WaitOutputSumMax);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		allTaskStats.put(new ExecutionVertexID(vertex3, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex3, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus2 = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex3, 0),
				Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
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
		System.out.println(vertex1);
		System.out.println(vertex2);
		System.out.println(vertex3);
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}

	private void initMockMetrics(MetricProvider metricProvider, JobVertexID vertex1,
			JobVertexID vertex2, TaskMetricSubscription zeroSub, TaskMetricSubscription v1InputTps,
			TaskMetricSubscription v1OutputTps, TaskMetricSubscription v1LatencyCountMin,
			TaskMetricSubscription v1LatencyCountMax, TaskMetricSubscription v1LatencySumMin,
			TaskMetricSubscription v1LatencySumMax, TaskMetricSubscription v1WaitOutputCountMin,
			TaskMetricSubscription v1WaitOutputCountMax, TaskMetricSubscription v1WaitOutputSumMin,
			TaskMetricSubscription v1WaitOutputSumMax, TaskMetricSubscription v1Delay,
			TaskMetricSubscription v1DelayRate, TaskMetricSubscription v2InputTps,
			TaskMetricSubscription v2OutputTps, TaskMetricSubscription v2LatencyCountMin,
			TaskMetricSubscription v2LatencyCountMax, TaskMetricSubscription v2LatencySumMin,
			TaskMetricSubscription v2LatencySumMax, TaskMetricSubscription v2WaitOutputCountMin,
			TaskMetricSubscription v2WaitOutputCountMax, TaskMetricSubscription v2WaitOutputSumMin,
			TaskMetricSubscription v2WaitOutputSumMax) {
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
						return v1InputTps;
					}
				} else if (metricName.equals(MetricNames.TASK_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v1OutputTps;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_COUNT)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v1LatencyCountMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v1LatencyCountMin;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_SUM)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v1LatencySumMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v1LatencySumMin;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v1WaitOutputCountMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v1WaitOutputCountMin;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_SUM)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v1WaitOutputSumMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v1WaitOutputSumMin;
					}
				} else if (metricName.equals(MetricNames.SOURCE_DELAY)) {
					if (aggType.equals(TimelineAggType.AVG)) {
						return v1Delay;
					} else if (aggType.equals(TimelineAggType.RATE)) {
						return v1DelayRate;
					}
				}
			} else if (vertexId.equals(vertex2)) {
				if (metricName.equals(MetricNames.TASK_INPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v2InputTps;
					}
				} else if (metricName.equals(MetricNames.TASK_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.RATE)) {
						return v2OutputTps;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_COUNT)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v2LatencyCountMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v2LatencyCountMin;
					}
				} else if (metricName.equals(MetricNames.TASK_LATENCY_SUM)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v2LatencySumMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v2LatencySumMin;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_COUNT)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v2WaitOutputCountMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v2WaitOutputCountMin;
					}
				} else if (metricName.equals(MetricNames.WAIT_OUTPUT_SUM)) {
					if (aggType.equals(TimelineAggType.MAX)) {
						return v2WaitOutputSumMax;
					} else if (aggType.equals(TimelineAggType.MIN)) {
						return v2WaitOutputSumMin;
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
		JobVertexID vertex3 = new JobVertexID();

		// job level configuration.
		Configuration config = new Configuration();
		config.setString("healthmonitor.health.check.interval.ms", "3000");
		config.setString("parallelism.scale.timeout.ms", "10000");
		config.setString("parallelism.down-scale.tps.ratio", "1.0");
		config.setString(HealthMonitor.DETECTOR_CLASSES,
			FrequentFullGCDetector.class.getCanonicalName() + "," +
			FailoverDetector.class.getCanonicalName() + "," +
			LowDelayDetector.class.getCanonicalName() + "," +
			OverParallelizedDetector.class.getCanonicalName());
		config.setString(HealthMonitor.RESOLVER_CLASSES, ParallelismScaler.class.getCanonicalName());

		// initial job vertex config.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs1 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config1 = new RestServerClient.VertexConfig(
			2, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config1 = new RestServerClient.VertexConfig(
			2, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		RestServerClient.VertexConfig vertex3Config1 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(5));
		vertexConfigs1.put(vertex1, vertex1Config1);
		vertexConfigs1.put(vertex2, vertex2Config1);
		vertexConfigs1.put(vertex3, vertex3Config1);

		// job vertex config after resale.
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs2 = new HashMap<>();
		RestServerClient.VertexConfig vertex1Config2 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(1, 2));
		RestServerClient.VertexConfig vertex2Config2 = new RestServerClient.VertexConfig(
			2, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(3, 4));
		RestServerClient.VertexConfig vertex3Config2 = new RestServerClient.VertexConfig(
			1, 2, new ResourceSpec.Builder().build(), Lists.newArrayList(5));
		vertexConfigs2.put(vertex1, vertex1Config2);
		vertexConfigs2.put(vertex2, vertex2Config2);
		vertexConfigs2.put(vertex3, vertex3Config2);

		// job graph topology
		Map<JobVertexID, List<JobVertexID>> inputNodes = new HashMap<>();
		inputNodes.put(vertex1, Collections.emptyList());
		inputNodes.put(vertex2, Arrays.asList(vertex1));
		inputNodes.put(vertex3, Collections.emptyList());

		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID)))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs1, inputNodes))
			.thenReturn(new RestServerClient.JobConfig(config, vertexConfigs2, inputNodes));

		long now = System.currentTimeMillis();

		TaskMetricSubscription zeroSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(zeroSub.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex1

		TaskMetricSubscription v1InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1InputTps.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1LatencyCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1LatencyCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencyCountMax.getValue()).thenReturn(new Tuple2<>(now, 1000.0));

		TaskMetricSubscription v1LatencySumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1LatencySumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1LatencySumMax.getValue()).thenReturn(new Tuple2<>(now, 1.0e9));

		TaskMetricSubscription v1WaitOutputCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1WaitOutputCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputCountMax.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v1WaitOutputSumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1WaitOutputSumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1WaitOutputSumMax.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v1Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v1Delay.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		// vertex2

		TaskMetricSubscription v2InputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2InputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2OutputTps = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2OutputTps.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencyCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2LatencyCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencyCountMax.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2LatencySumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2LatencySumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2LatencySumMax.getValue()).thenReturn(new Tuple2<>(now, 2.0e9));

		TaskMetricSubscription v2WaitOutputCountMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2WaitOutputCountMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputCountMax.getValue()).thenReturn(new Tuple2<>(now, 2000.0));

		TaskMetricSubscription v2WaitOutputSumMin = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumMin.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2WaitOutputSumMax = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2WaitOutputSumMax.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		TaskMetricSubscription v2Delay = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(v2Delay.getValue()).thenReturn(new Tuple2<>(now, 0.0));

		initMockMetrics(metricProvider, vertex1, vertex2, zeroSub,
				v1InputTps, v1OutputTps, v1LatencyCountMin, v1LatencyCountMax, v1LatencySumMin,
				v1LatencySumMax, v1WaitOutputCountMin, v1WaitOutputCountMax, v1WaitOutputSumMin,
				v1WaitOutputSumMax, v1Delay, zeroSub,
				v2InputTps, v2OutputTps, v2LatencyCountMin, v2LatencyCountMax, v2LatencySumMin,
				v2LatencySumMax, v2WaitOutputCountMin, v2WaitOutputCountMax, v2WaitOutputSumMin,
				v2WaitOutputSumMax);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats = new HashMap<>();
		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		allTaskStats.put(new ExecutionVertexID(vertex3, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus = new RestServerClient.JobStatus(allTaskStats);

		allTaskStats.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats.put(new ExecutionVertexID(vertex3, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.SCHEDULED));
		RestServerClient.JobStatus jobStatus2 = new RestServerClient.JobStatus(allTaskStats);

		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> allTaskStats2 = new HashMap<>();
		allTaskStats2.put(new ExecutionVertexID(vertex1, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex2, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
		allTaskStats2.put(new ExecutionVertexID(vertex3, 0),
			Tuple2.of(System.currentTimeMillis(), ExecutionState.RUNNING));
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
		vertexParallelismResource.put(vertex1, new Tuple2<>(1, ResourceSpec.newBuilder().build()));
		Mockito.verify(restServerClient, Mockito.times(1))
			.rescale(
				Mockito.eq(jobID),
				Mockito.eq(vertexParallelismResource));
	}
}
