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
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStuck;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexBackPressure;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDelayIncreasing;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFailover;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighDelay;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexLongTimeFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOverParallelized;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatistics;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for parallelism scaler.
 */
public class ParallelismScalerTest {

	@Test
	public void testParseSymptoms() {
		ParallelismScaler scaler = new ParallelismScaler();

		List<Symptom> symptoms = new LinkedList<>();

		JobStable jobStable = mock(JobStable.class);
		symptoms.add(jobStable);

		JobVertexFrequentFullGC frequentFullGC = mock(JobVertexFrequentFullGC.class);
		symptoms.add(frequentFullGC);

		JobVertexLongTimeFullGC longTimeFullGC = mock(JobVertexLongTimeFullGC.class);
		symptoms.add(longTimeFullGC);

		JobVertexFailover failover = mock(JobVertexFailover.class);
		symptoms.add(failover);

		JobStuck jobStuck = mock(JobStuck.class);
		symptoms.add(jobStuck);

		JobVertexHighDelay highDelay = mock(JobVertexHighDelay.class);
		symptoms.add(highDelay);

		JobVertexDelayIncreasing delayIncreasing = mock(JobVertexDelayIncreasing.class);
		symptoms.add(delayIncreasing);

		JobVertexBackPressure backPressure = mock(JobVertexBackPressure.class);
		symptoms.add(backPressure);

		JobVertexOverParallelized overParallelized = mock(JobVertexOverParallelized.class);
		symptoms.add(overParallelized);

		scaler.parseSymptoms(symptoms);

		assertEquals(jobStable, Whitebox.getInternalState(scaler, "jobStableSymptom"));
		assertEquals(frequentFullGC, Whitebox.getInternalState(scaler, "frequentFullGCSymptom"));
		assertEquals(longTimeFullGC, Whitebox.getInternalState(scaler, "longTimeFullGCSymptom"));
		assertEquals(failover, Whitebox.getInternalState(scaler, "failoverSymptom"));
		assertEquals(jobStuck, Whitebox.getInternalState(scaler, "jobStuckSymptom"));
		assertEquals(highDelay, Whitebox.getInternalState(scaler, "highDelaySymptom"));
		assertEquals(delayIncreasing, Whitebox.getInternalState(scaler, "delayIncreasingSymptom"));
		assertEquals(backPressure, Whitebox.getInternalState(scaler, "backPressureSymptom"));
		assertEquals(overParallelized, Whitebox.getInternalState(scaler, "overParallelizedSymptom"));

		scaler.parseSymptoms(new LinkedList<>());
		assertEquals(null, Whitebox.getInternalState(scaler, "jobStableSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "frequentFullGCSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "longTimeFullGCSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "failoverSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "jobStuckSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "highDelaySymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "delayIncreasingSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "backPressureSymptom"));
		assertEquals(null, Whitebox.getInternalState(scaler, "overParallelizedSymptom"));

	}

	@Test
	public void testGetSubDagTargetTpsRatioForDelay() {
		JobID jobID = new JobID();
		JobVertexID vertexID = new JobVertexID();
		ParallelismScaler scaler = new ParallelismScaler();

		Map<JobVertexID, JobVertexID> vertexToRoot = new HashMap<>();
		vertexToRoot.put(vertexID, vertexID);
		Whitebox.setInternalState(scaler, "vertex2SubDagRoot", vertexToRoot);
		Whitebox.setInternalState(scaler, "subDagRoot2UpstreamVertices", new HashMap<>());
		Whitebox.setInternalState(scaler, "scaleTpsRatio", 4);

		Whitebox.setInternalState(scaler, "needScaleUpForDelay", true);

		// parallel source
		JobVertexHighDelay highDelay = new JobVertexHighDelay(jobID, Collections.singletonList(vertexID), Collections.singletonList(vertexID));
		JobVertexDelayIncreasing delayIncreasing = new JobVertexDelayIncreasing(jobID, Collections.singletonList(vertexID));
		Whitebox.setInternalState(scaler, "highDelaySymptom", highDelay);
		Whitebox.setInternalState(scaler, "delayIncreasingSymptom", delayIncreasing);

		Map<JobVertexID, ParallelismScaler.TaskMetrics> allMetrics = new HashMap<>();
		ParallelismScaler.TaskMetrics taskMetrics = new ParallelismScaler.TaskMetrics(
				vertexID,
				true,
				10,
				0,
				1.1,
				0,
				0.1,
				0,
				0.9,
				0.1,
				32
		);
		allMetrics.put(vertexID, taskMetrics);

		assertTrue(Math.abs(3.2 - scaler.getSubDagScaleUpRatio(allMetrics).get(vertexID)) < 1e-6);

		// parallel source without severe delay
		highDelay = new JobVertexHighDelay(jobID, Collections.singletonList(vertexID), Collections.emptyList());
		delayIncreasing = new JobVertexDelayIncreasing(jobID, Collections.singletonList(vertexID));
		Whitebox.setInternalState(scaler, "highDelaySymptom", highDelay);
		Whitebox.setInternalState(scaler, "delayIncreasingSymptom", delayIncreasing);

		allMetrics = new HashMap<>();
		taskMetrics = new ParallelismScaler.TaskMetrics(
				vertexID,
				true,
				10,
				0,
				1.1,
				0,
				0.1,
				0,
				0.9,
				0.1,
				32
		);
		allMetrics.put(vertexID, taskMetrics);

		assertTrue(Math.abs(3.2 - scaler.getSubDagScaleUpRatio(allMetrics).get(vertexID)) < 1e-6);

		// not parallel source
		highDelay = new JobVertexHighDelay(jobID, Collections.singletonList(vertexID), Collections.singletonList(vertexID));
		delayIncreasing = new JobVertexDelayIncreasing(jobID, Collections.singletonList(vertexID));
		Whitebox.setInternalState(scaler, "highDelaySymptom", highDelay);
		Whitebox.setInternalState(scaler, "delayIncreasingSymptom", delayIncreasing);

		taskMetrics = new ParallelismScaler.TaskMetrics(
				vertexID,
				false,
				10,
				0,
				1.1,
				0,
				0.1,
				0,
				0.9,
				0.1,
				32
		);
		allMetrics.put(vertexID, taskMetrics);

		assertTrue(Math.abs(40 - scaler.getSubDagScaleUpRatio(allMetrics).get(vertexID)) < 1e-6);

		// not parallel source and target ratio less than ratio.
		Whitebox.setInternalState(scaler, "scaleTpsRatio", 11);
		taskMetrics = new ParallelismScaler.TaskMetrics(
				vertexID,
				false,
				10,
				0,
				1.1,
				0,
				0.1,
				0,
				0.9,
				0.1,
				32
		);
		allMetrics.put(vertexID, taskMetrics);

		assertTrue(Math.abs(110 - scaler.getSubDagScaleUpRatio(allMetrics).get(vertexID)) < 1e-6);
	}

	@Test
	public void testUpdateTargetParallelismSubjectToConstraints() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();
		JobVertexID vertex4 = new JobVertexID();
		ParallelismScaler scaler = new ParallelismScaler();
		Map<JobVertexID, Integer> targetParallelism = new HashMap<>();
		targetParallelism.put(vertex2, 12);
		targetParallelism.put(vertex3, 4);
		targetParallelism.put(vertex4, 7);

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig config1 = new RestServerClient.VertexConfig(32, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config2 = new RestServerClient.VertexConfig(157, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config3 = new RestServerClient.VertexConfig(28, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config4 = new RestServerClient.VertexConfig(97, 1000, ResourceSpec.DEFAULT);
		vertexConfigs.put(vertex1, config1);
		vertexConfigs.put(vertex2, config2);
		vertexConfigs.put(vertex3, config3);
		vertexConfigs.put(vertex4, config4);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputConfigs = new HashMap<>();
		inputConfigs.put(vertex1, Collections.emptyList());
		inputConfigs.put(vertex2, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		inputConfigs.put(vertex3, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		inputConfigs.put(vertex4, Collections.singletonList(Tuple2.of(vertex2, "HASH")));
		RestServerClient.JobConfig jobConfig = new RestServerClient.JobConfig(new Configuration(), vertexConfigs, inputConfigs);
		scaler.analyzeJobGraph(jobConfig);
		scaler.updateTargetParallelismsSubjectToConstraints(targetParallelism, null, jobConfig);

		Map<JobVertexID, Integer> expectParallelism = new HashMap<>();
		expectParallelism.put(vertex1, 32);
		expectParallelism.put(vertex2, 12);
		expectParallelism.put(vertex3, 4);
		expectParallelism.put(vertex4, 7);

		assertEquals(expectParallelism, targetParallelism);

		targetParallelism = new HashMap<>();
		targetParallelism.put(vertex2, 12);
		targetParallelism.put(vertex3, 4);
		targetParallelism.put(vertex4, 7);
		Map<JobVertexID, Integer> minParallelisms = new HashMap<>();
		minParallelisms.put(vertex2, 15);
		scaler.updateTargetParallelismsSubjectToConstraints(targetParallelism, minParallelisms, jobConfig);

		expectParallelism = new HashMap<>();
		expectParallelism.put(vertex1, 32);
		expectParallelism.put(vertex2, 15);
		expectParallelism.put(vertex3, 4);
		expectParallelism.put(vertex4, 7);

		assertEquals(expectParallelism, targetParallelism);

	}

	@Test
	public void testMinParallelism() {
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();
		JobVertexID vertex4 = new JobVertexID();
		ParallelismScaler scaler = new ParallelismScaler();

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig config1 = new RestServerClient.VertexConfig(32, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config2 = new RestServerClient.VertexConfig(157, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config3 = new RestServerClient.VertexConfig(28, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config4 = new RestServerClient.VertexConfig(97, 1000, ResourceSpec.DEFAULT);
		vertexConfigs.put(vertex1, config1);
		vertexConfigs.put(vertex2, config2);
		vertexConfigs.put(vertex3, config3);
		vertexConfigs.put(vertex4, config4);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputConfigs = new HashMap<>();
		inputConfigs.put(vertex1, Collections.emptyList());
		inputConfigs.put(vertex2, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		inputConfigs.put(vertex3, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		inputConfigs.put(vertex4, Collections.singletonList(Tuple2.of(vertex2, "HASH")));
		RestServerClient.JobConfig jobConfig = new RestServerClient.JobConfig(new Configuration(), vertexConfigs, inputConfigs);

		Map<JobVertexID, TaskCheckpointStatistics> checkpointInfo = new HashMap<>();
		TaskCheckpointStatistics statisticsOfVertex2 = new TaskCheckpointStatistics(0, CheckpointStatsStatus.COMPLETED, 0, 8, 8, 0, 0, 0, 0);
		checkpointInfo.put(vertex2, statisticsOfVertex2);

		Map<JobVertexID, TaskMetricSubscription> partitionCountSubscription = new HashMap<>();
		TaskMetricSubscription metric1 = Mockito.mock(TaskMetricSubscription.class);
		Mockito.when(metric1.getValue()).thenReturn(Tuple2.of(0L, 8.0));
		partitionCountSubscription.put(vertex1, metric1);
		Whitebox.setInternalState(scaler, "sourcePartitionCountSubs", partitionCountSubscription);
		Whitebox.setInternalState(scaler, "maxPartitionPerTask", 2);
		Whitebox.setInternalState(scaler, "stateSizeThreshold", 4);

		Map<JobVertexID, Integer> expectedMinParallelism = new HashMap<>();
		expectedMinParallelism.put(vertex1, 4);
		expectedMinParallelism.put(vertex2, 2);
		expectedMinParallelism.put(vertex3, 1);
		expectedMinParallelism.put(vertex4, 1);
		scaler.analyzeJobGraph(jobConfig);
		assertEquals(expectedMinParallelism, scaler.getVertexMinParallelisms(jobConfig, checkpointInfo));
	}

	@Test
	public void testGetTargetParallelism() {
		ParallelismScaler scaler = new ParallelismScaler();
		Whitebox.setInternalState(scaler, "reservedParallelismRatio", 1.2);
		Whitebox.setInternalState(scaler, "scaleTpsRatio", 2);
		JobVertexID vertex1 = new JobVertexID();
		JobVertexID vertex2 = new JobVertexID();
		JobVertexID vertex3 = new JobVertexID();
		JobVertexID vertex4 = new JobVertexID();

		Map<JobVertexID, RestServerClient.VertexConfig> vertexConfigs = new HashMap<>();
		RestServerClient.VertexConfig config1 = new RestServerClient.VertexConfig(32, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config2 = new RestServerClient.VertexConfig(157, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config3 = new RestServerClient.VertexConfig(28, 1000, ResourceSpec.DEFAULT);
		RestServerClient.VertexConfig config4 = new RestServerClient.VertexConfig(97, 1000, ResourceSpec.DEFAULT);
		vertexConfigs.put(vertex1, config1);
		vertexConfigs.put(vertex2, config2);
		vertexConfigs.put(vertex3, config3);
		vertexConfigs.put(vertex4, config4);

		Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputConfigs = new HashMap<>();
		inputConfigs.put(vertex1, Collections.emptyList());
		inputConfigs.put(vertex2, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		inputConfigs.put(vertex3, Collections.singletonList(Tuple2.of(vertex1, "HASH")));
		inputConfigs.put(vertex4, Collections.singletonList(Tuple2.of(vertex2, "HASH")));
		RestServerClient.JobConfig jobConfig = new RestServerClient.JobConfig(new Configuration(), vertexConfigs, inputConfigs);
		scaler.analyzeJobGraph(jobConfig);

		Map<JobVertexID, Double> scaleupRatio = new HashMap<>();
		Set<JobVertexID> scaleDownSet = new HashSet<>();
		Map<JobVertexID, ParallelismScaler.TaskMetrics> metrics = new HashMap<>();
		ParallelismScaler.TaskMetrics v1Metrics = new ParallelismScaler.TaskMetrics(
				vertex1, true, 1, 1, 1, 1, 1, 256, 1, 1, 256);
		ParallelismScaler.TaskMetrics v2Metrics = new ParallelismScaler.TaskMetrics(
				vertex2, false, 1, 1,  1, 1, 1, 1, 1, 1, 1);
		ParallelismScaler.TaskMetrics v3Metrics = new ParallelismScaler.TaskMetrics(
				vertex3, false, 1, 1, 1, 1, 1, 1, 1, 1, 1);
		ParallelismScaler.TaskMetrics v4Metrics = new ParallelismScaler.TaskMetrics(
				vertex4, false, 1, 1, 1, 1, 1, 1, 1, 1, 1);
		metrics.put(vertex1, v1Metrics);
		metrics.put(vertex2, v2Metrics);
		metrics.put(vertex3, v3Metrics);
		metrics.put(vertex4, v4Metrics);
		scaleupRatio.clear();
		scaleDownSet.clear();
		Map<JobVertexID, Integer> expectedParallelism = new HashMap<>();
		expectedParallelism.put(vertex1, 4);
		expectedParallelism.put(vertex2, 4);
		expectedParallelism.put(vertex3, 4);
		expectedParallelism.put(vertex4, 4);

		scaleupRatio.put(vertex1, 4.0);
		assertEquals(expectedParallelism, scaler.getVertexTargetParallelisms(scaleupRatio, scaleDownSet, metrics));

		scaleupRatio.clear();
		scaleDownSet.clear();
		expectedParallelism.clear();

		scaleDownSet.add(vertex1);
		expectedParallelism.put(vertex1, 512);
		assertEquals(expectedParallelism, scaler.getVertexTargetParallelisms(scaleupRatio, scaleDownSet, metrics));

		scaleDownSet.clear();
		expectedParallelism.clear();
		scaleDownSet.add(vertex2);
		expectedParallelism.put(vertex2, 2);
		assertEquals(expectedParallelism, scaler.getVertexTargetParallelisms(scaleupRatio, scaleDownSet, metrics));
	}
}
