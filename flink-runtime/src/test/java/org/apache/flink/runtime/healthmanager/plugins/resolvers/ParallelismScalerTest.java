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
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStable;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobStuck;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexBackPressure;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexDelayIncreasing;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFailover;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFrequentFullGC;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighDelay;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexHighStateSize;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexOverParallelized;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

		JobVertexHighStateSize highStateSize = mock(JobVertexHighStateSize.class);
		symptoms.add(highStateSize);

		scaler.parseSymptoms(symptoms);

		assertEquals(jobStable, Whitebox.getInternalState(scaler, "jobStableSymptom"));
		assertEquals(frequentFullGC, Whitebox.getInternalState(scaler, "frequentFullGCSymptom"));
		assertEquals(failover, Whitebox.getInternalState(scaler, "failoverSymptom"));
		assertEquals(jobStuck, Whitebox.getInternalState(scaler, "jobStuckSymptom"));
		assertEquals(highDelay, Whitebox.getInternalState(scaler, "highDelaySymptom"));
		assertEquals(delayIncreasing, Whitebox.getInternalState(scaler, "delayIncreasingSymptom"));
		assertEquals(backPressure, Whitebox.getInternalState(scaler, "backPressureSymptom"));
		assertEquals(overParallelized, Whitebox.getInternalState(scaler, "overParallelizedSymptom"));
		assertEquals(highStateSize, Whitebox.getInternalState(scaler, "highStateSizeSymptom"));
	}

	@Test
	public void testGetSubDagTargetTpsRatioOfParallelSource() {
		JobID jobID = new JobID();
		JobVertexID vertexID = new JobVertexID();
		ParallelismScaler scaler = new ParallelismScaler();

		Map<JobVertexID, JobVertexID> vertexToRoot = new HashMap<>();
		vertexToRoot.put(vertexID, vertexID);
		Whitebox.setInternalState(scaler, "vertex2SubDagRoot", vertexToRoot);
		Whitebox.setInternalState(scaler, "subDagRoot2UpstreamVertices", new HashMap<>());

		Whitebox.setInternalState(scaler, "needScaleUpForDelay", true);
		JobVertexHighDelay highDelay = new JobVertexHighDelay(jobID, Collections.singletonList(vertexID));
		JobVertexDelayIncreasing delayIncreasing = new JobVertexDelayIncreasing(jobID, Collections.singletonList(vertexID));
		Whitebox.setInternalState(scaler, "highDelaySymptom", highDelay);
		Whitebox.setInternalState(scaler, "delayIncreasingSymptom", delayIncreasing);

		// parallel source
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

		assertTrue(Math.abs(3.2 - scaler.getSubDagTargetTpsRatio(allMetrics).get(vertexID)) < 1e-6);

		// not parallel source
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

		assertTrue(Math.abs(10 - scaler.getSubDagTargetTpsRatio(allMetrics).get(vertexID)) < 1e-6);

	}
}
