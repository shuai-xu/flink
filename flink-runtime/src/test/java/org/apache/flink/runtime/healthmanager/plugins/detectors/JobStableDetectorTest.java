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

package org.apache.flink.runtime.healthmanager.plugins.detectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricAggType;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricNames;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyVararg;

/**
 * Test for JobStableDetector.
 */
public class JobStableDetectorTest extends DetectorTestBase {

	@Test
	public void testDetectJobStable() throws Exception {

		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, Mockito.mock(RestServerClient.VertexConfig.class));
		vertexConfigs.put(vertex2, Mockito.mock(RestServerClient.VertexConfig.class));
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		RestServerClient.JobStatus jobStatus = Mockito.mock(RestServerClient.JobStatus.class);
		Mockito.when(restClient.getJobStatus(jobID)).thenReturn(jobStatus);

		long now = System.currentTimeMillis();
		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> status = new HashMap<>();
		status.put(new ExecutionVertexID(vertex1, 0), new Tuple2<>(now, ExecutionState.RUNNING));
		status.put(new ExecutionVertexID(vertex1, 1), new Tuple2<>(now, ExecutionState.RUNNING));
		status.put(new ExecutionVertexID(vertex2, 0), new Tuple2<>(now, ExecutionState.RUNNING));
		Mockito.when(jobStatus.getTaskStatus()).thenReturn(status);

		TaskMetricSubscription initTimeSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(initTimeSub.getValue()).thenReturn(new Tuple2<>(now, 1.0));
		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.eq(jobID),
			Mockito.any(JobVertexID.class),
			Mockito.eq(MetricNames.TASK_INIT_TIME),
			Mockito.eq(MetricAggType.MIN),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.LATEST)))
			.thenReturn(initTimeSub);

		JobStableDetector detector = new JobStableDetector();
		detector.open(monitor);
		Symptom symptom = detector.detect();

		assertTrue(symptom instanceof JobStable);
		assertTrue(((JobStable) symptom).getStableTime() >= 0);
	}

	@Test
	public void testDetectTaskNotRunning() throws Exception {

		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, Mockito.mock(RestServerClient.VertexConfig.class));
		vertexConfigs.put(vertex2, Mockito.mock(RestServerClient.VertexConfig.class));
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);

		RestServerClient.JobStatus jobStatus = Mockito.mock(RestServerClient.JobStatus.class);
		Mockito.when(restClient.getJobStatus(jobID)).thenReturn(jobStatus);

		long now = System.currentTimeMillis();
		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> status = new HashMap<>();
		status.put(new ExecutionVertexID(vertex1, 0), new Tuple2<>(now, ExecutionState.RUNNING));
		status.put(new ExecutionVertexID(vertex1, 1), new Tuple2<>(now, ExecutionState.DEPLOYING));
		status.put(new ExecutionVertexID(vertex2, 0), new Tuple2<>(now, ExecutionState.RUNNING));
		Mockito.when(jobStatus.getTaskStatus()).thenReturn(status);

		TaskMetricSubscription initTimeSub = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(initTimeSub.getValue()).thenReturn(new Tuple2<>(now, 1.0));
		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.eq(jobID),
			Mockito.any(JobVertexID.class),
			Mockito.eq(MetricNames.TASK_INIT_TIME),
			Mockito.eq(MetricAggType.MIN),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.LATEST)))
			.thenReturn(initTimeSub);

		JobStableDetector detector = new JobStableDetector();
		detector.open(monitor);
		Symptom symptom = detector.detect();

		assertEquals(JobStable.UNSTABLE, symptom);
	}

	@Test
	public void testDetectTaskNotInitialized() throws Exception {
		HealthMonitor monitor = Mockito.mock(HealthMonitor.class);

		JobID jobID = new JobID();
		Mockito.when(monitor.getJobID()).thenReturn(jobID);
		Mockito.when(monitor.getConfig()).thenReturn(new Configuration());

		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		vertexConfigs.put(vertex1, Mockito.mock(RestServerClient.VertexConfig.class));
		vertexConfigs.put(vertex2, Mockito.mock(RestServerClient.VertexConfig.class));

		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(jobConfig.getVertexConfigs()).thenReturn(vertexConfigs);
		Mockito.when(monitor.getJobConfig()).thenReturn(jobConfig);

		RestServerClient restClient = Mockito.mock(RestServerClient.class);
		Mockito.when(monitor.getRestServerClient()).thenReturn(restClient);

		RestServerClient.JobStatus jobStatus = Mockito.mock(RestServerClient.JobStatus.class);
		Mockito.when(restClient.getJobStatus(jobID)).thenReturn(jobStatus);

		long now = System.currentTimeMillis();
		Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> status = new HashMap<>();
		status.put(new ExecutionVertexID(vertex1, 0), new Tuple2<>(now, ExecutionState.RUNNING));
		status.put(new ExecutionVertexID(vertex1, 1), new Tuple2<>(now, ExecutionState.RUNNING));
		status.put(new ExecutionVertexID(vertex2, 0), new Tuple2<>(now, ExecutionState.RUNNING));
		Mockito.when(jobStatus.getTaskStatus()).thenReturn(status);

		MetricProvider metricProvider = Mockito.mock(MetricProvider.class);
		Mockito.when(monitor.getMetricProvider()).thenReturn(metricProvider);

		TaskMetricSubscription initTimeSub1 = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(initTimeSub1.getValue()).thenReturn(new Tuple2<>(now, 1.0));
		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.eq(jobID),
			Mockito.eq(vertex1),
			Mockito.eq(MetricNames.TASK_INIT_TIME),
			Mockito.eq(MetricAggType.MIN),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.LATEST)))
			.thenReturn(initTimeSub1);
		TaskMetricSubscription initTimeSub2 = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(initTimeSub2.getValue()).thenReturn(new Tuple2<>(now, -1.0));
		Mockito.when(metricProvider.subscribeTaskMetric(
			Mockito.eq(jobID),
			Mockito.eq(vertex2),
			Mockito.eq(MetricNames.TASK_INIT_TIME),
			Mockito.eq(MetricAggType.MIN),
			Mockito.anyLong(),
			Mockito.eq(TimelineAggType.LATEST)))
			.thenReturn(initTimeSub2);

		PowerMockito.mockStatic(MetricUtils.class);
		Mockito.when(MetricUtils.validateTaskMetric(Mockito.any(HealthMonitor.class), Mockito.anyLong(), anyVararg())).thenReturn(true);

		JobStableDetector detector = new JobStableDetector();
		detector.open(monitor);
		Symptom symptom = detector.detect();

		assertEquals(JobStable.UNSTABLE, symptom);
	}
}
