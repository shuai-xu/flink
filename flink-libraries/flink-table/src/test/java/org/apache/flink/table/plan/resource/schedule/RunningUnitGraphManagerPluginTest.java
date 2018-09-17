/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.resource.schedule;

import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.schedule.VertexScheduler;
import org.apache.flink.table.plan.resource.BatchExecRelStage;
import org.apache.flink.table.plan.resource.RelRunningUnit;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for RunningUnitGraphManagerPlugin.
 */
public class RunningUnitGraphManagerPluginTest extends TestLogger {

	private List<JobVertex> jobVertexList;
	private List<RelRunningUnit> relRunningUnitList;
	private List<BatchExecRelStage> relStageList;
	private VertexScheduler scheduler;
	private JobGraph jobGraph;

	@Before
	public void setUp() {
		jobVertexList = new ArrayList<>();
		relRunningUnitList = new ArrayList<>();
		relStageList = new ArrayList<>();
		scheduler = mock(VertexScheduler.class);
		jobGraph = mock(JobGraph.class);
	}

	@Test
	public void testSchedule() throws Exception {

		createJobVertexIDs(6);

		Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = new HashMap<>();
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(0).getID(), k->new ArrayList<>()).add(0);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(1).getID(), k->new ArrayList<>()).add(1);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(2).getID(), k->new ArrayList<>()).add(2);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(3).getID(), k->new ArrayList<>()).addAll(Arrays.asList(3, 4));
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(4).getID(), k->new ArrayList<>()).add(6);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(5).getID(), k->new ArrayList<>()).addAll(Arrays.asList(7, 8, 9));

		createBatchExecRelStages(13);
		when(relStageList.get(0).getTransformationIDList()).thenReturn(Arrays.asList(0));
		when(relStageList.get(1).getTransformationIDList()).thenReturn(Arrays.asList(1));
		when(relStageList.get(2).getTransformationIDList()).thenReturn(Arrays.asList(2));
		when(relStageList.get(3).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(4).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(5).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(5).getDependStageList(BatchExecRelStage.DependType.DATA_TRIGGER)).thenReturn(Arrays.asList(relStageList.get(3), relStageList.get(4)));
		when(relStageList.get(6).getTransformationIDList()).thenReturn(Arrays.asList(4));
		when(relStageList.get(7).getTransformationIDList()).thenReturn(Arrays.asList(6));
		when(relStageList.get(8).getTransformationIDList()).thenReturn(Arrays.asList(7));
		when(relStageList.get(9).getTransformationIDList()).thenReturn(Arrays.asList(7));
		when(relStageList.get(9).getDependStageList(BatchExecRelStage.DependType.PRIORITY)).thenReturn(Arrays.asList(relStageList.get(8)));
		when(relStageList.get(10).getTransformationIDList()).thenReturn(Arrays.asList(8));
		when(relStageList.get(11).getTransformationIDList()).thenReturn(Arrays.asList(8));
		when(relStageList.get(11).getDependStageList(BatchExecRelStage.DependType.DATA_TRIGGER)).thenReturn(Arrays.asList(relStageList.get(10)));
		when(relStageList.get(12).getTransformationIDList()).thenReturn(Arrays.asList(9));

		createRelRunningUnits(5);

		when(relRunningUnitList.get(0).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(0), relStageList.get(1), relStageList.get(3)));
		when(relRunningUnitList.get(1).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(2), relStageList.get(4)));
		when(relRunningUnitList.get(2).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(5), relStageList.get(6), relStageList.get(7), relStageList.get(8)));
		when(relRunningUnitList.get(3).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(9), relStageList.get(10)));
		when(relRunningUnitList.get(4).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(11), relStageList.get(12)));

		when(relStageList.get(8).getRunningUnitList()).thenReturn(Sets.newHashSet(relRunningUnitList.get(2)));

		RunningUnitGraphManagerPlugin plugin = new RunningUnitGraphManagerPlugin();
		plugin.open(scheduler, jobGraph, vertexToStreamNodeIds, relRunningUnitList);

		plugin.onSchedulingStarted();
		verifyScheduleJobVertex(0, 1, 3);
		triggerJobVertexStatusChanged(plugin, 0, 1);
		triggerTaskStatusChanged(plugin, 3, 0);
		verify(scheduler, times(6)).scheduleExecutionVertices(any());
		triggerTaskStatusChanged(plugin, 3, 1);
		verifyScheduleJobVertex(2);
		triggerJobVertexStatusChanged(plugin, 2);
		triggerPartitionConsumable(plugin, 3);
		verifyScheduleJobVertex(4, 5);
	}

	private void setRelStage(int relStageIndex, Integer... transformationIDs) {
		when(relStageList.get(relStageIndex).getTransformationIDList()).thenReturn(Arrays.asList(transformationIDs));
	}

	@Test
	public void testScheduleChain() {
		createJobVertexIDs(5);

		Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = new HashMap<>();
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(0).getID(), k->new ArrayList<>()).add(0);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(1).getID(), k->new ArrayList<>()).addAll(Arrays.asList(1, 2));
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(2).getID(), k->new ArrayList<>()).add(3);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(3).getID(), k->new ArrayList<>()).add(4);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(4).getID(), k->new ArrayList<>()).add(5);

		createBatchExecRelStages(13);
		when(relStageList.get(0).getTransformationIDList()).thenReturn(Arrays.asList(0));
		when(relStageList.get(1).getTransformationIDList()).thenReturn(Arrays.asList(1));
		when(relStageList.get(2).getTransformationIDList()).thenReturn(Arrays.asList(1));
		when(relStageList.get(2).getDependStageList(BatchExecRelStage.DependType.DATA_TRIGGER)).thenReturn(Arrays.asList(relStageList.get(1)));
		when(relStageList.get(3).getTransformationIDList()).thenReturn(Arrays.asList(2));
		when(relStageList.get(4).getTransformationIDList()).thenReturn(Arrays.asList(2));
		when(relStageList.get(4).getDependStageList(BatchExecRelStage.DependType.PRIORITY)).thenReturn(Arrays.asList(relStageList.get(4)));
		when(relStageList.get(5).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(6).getTransformationIDList()).thenReturn(Arrays.asList(4));

		createRelRunningUnits(5);

		when(relRunningUnitList.get(0).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(0), relStageList.get(1)));
		when(relRunningUnitList.get(1).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(2), relStageList.get(3)));
		when(relRunningUnitList.get(2).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(4), relStageList.get(5), relStageList.get(6)));

		when(relStageList.get(1).getRunningUnitList()).thenReturn(Sets.newHashSet(relRunningUnitList.get(0)));

		RunningUnitGraphManagerPlugin plugin = new RunningUnitGraphManagerPlugin();
		plugin.open(scheduler, jobGraph, vertexToStreamNodeIds, relRunningUnitList);

		plugin.onSchedulingStarted();
		verifyScheduleJobVertex(0, 1);
		triggerJobVertexStatusChanged(plugin, 0, 1);
		verifyScheduleJobVertex(2, 3);
		triggerJobVertexStatusChanged(plugin, 2, 3);
		verifyScheduleJobVertex(4);
	}

	private void triggerPartitionConsumable(RunningUnitGraphManagerPlugin plugin, int producerIndex) {
		IntermediateDataSetID id = new IntermediateDataSetID();
		when(jobGraph.getResultProducerID(id)).thenReturn(jobVertexList.get(producerIndex).getID());
		ResultPartitionConsumableEvent event = new ResultPartitionConsumableEvent(id, 0);
		plugin.onResultPartitionConsumable(event);
	}

	private void verifyScheduleJobVertex(int... jobVertexIndexes) {
		for (int jobVertexIndex : jobVertexIndexes) {
			verify(scheduler).scheduleExecutionVertices(Collections.singletonList(new ExecutionVertexID(jobVertexList.get(jobVertexIndex).getID(), 0)));
			verify(scheduler).scheduleExecutionVertices(Collections.singletonList(new ExecutionVertexID(jobVertexList.get(jobVertexIndex).getID(), 1)));
		}
	}

	private void triggerJobVertexStatusChanged(RunningUnitGraphManagerPlugin plugin, int... jobVertexIndexes) {
		for (int jobVertexIndex : jobVertexIndexes) {
			triggerTaskStatusChanged(plugin, jobVertexIndex, 0);
			triggerTaskStatusChanged(plugin, jobVertexIndex, 1);
		}
	}

	private void triggerTaskStatusChanged(RunningUnitGraphManagerPlugin plugin, int jobVertexIndex, int taskIndex) {
		ExecutionVertexStateChangedEvent event = new ExecutionVertexStateChangedEvent(new ExecutionVertexID(jobVertexList.get(jobVertexIndex).getID(), taskIndex), ExecutionState.DEPLOYING);
		plugin.onExecutionVertexStateChanged(event);
	}

	private void createBatchExecRelStages(int num) {
		for (int i = 0; i < num; i++) {
			relStageList.add(mock(BatchExecRelStage.class));
		}
	}

	private void createRelRunningUnits(int num) {
		for (int i = 0; i < num; i++) {
			relRunningUnitList.add(mock(RelRunningUnit.class));
		}
	}

	private void createJobVertexIDs(int num) {
		for (int i = 0; i < num; i++) {
			JobVertexID jobVertexID = new JobVertexID();
			JobVertex jobVertex = new JobVertex(String.valueOf(i), jobVertexID);
			jobVertex.setParallelism(2);
			jobVertexList.add(jobVertex);
		}
		when(jobGraph.getVerticesAsArray()).thenReturn(jobVertexList.toArray(new JobVertex[jobVertexList.size()]));
	}
}
