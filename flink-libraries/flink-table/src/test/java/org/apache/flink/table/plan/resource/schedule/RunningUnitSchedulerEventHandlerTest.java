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

import org.apache.flink.runtime.event.TaskStateChangedEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.JobScheduler;
import org.apache.flink.runtime.scheduler.LogicalTask;
import org.apache.flink.table.plan.resource.BatchExecRelStage;
import org.apache.flink.table.plan.resource.RelRunningUnit;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for RunningUnitSchedulerEventHandler.
 */
public class RunningUnitSchedulerEventHandlerTest extends TestLogger {

	private List<TestLogicalTask> taskList = new ArrayList<>();
	private List<JobVertexID> jobVertexIDs = new ArrayList<>();
	private List<RelRunningUnit> relRunningUnitList = new ArrayList<>();
	private List<BatchExecRelStage> relStageList = new ArrayList<>();
	private Map<JobVertexID, String> jobVerticesNameMap = new HashMap<>();

	@Test
	public void testDefaultSchedulerEventHandlerInEagerMode() throws Exception {
		JobScheduler scheduler = mock(JobScheduler.class);

		createJobVertexIDs(6);
		createTasks(12);
		for (int i = 0; i < taskList.size(); i++) {
			taskList.get(i).setJobVertexID(jobVertexIDs.get(i / 2));
		}
		doReturn(taskList).when(scheduler).getAllTasks();
		for (int i = 0; i < 6; i++) {
			doReturn(Arrays.asList(taskList.get(2 * i), taskList.get(2 * i + 1))).when(scheduler).getTasks(jobVertexIDs.get(i));
		}

		Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = new HashMap<>();
		vertexToStreamNodeIds.computeIfAbsent(jobVertexIDs.get(0), k->new ArrayList<>()).add(0);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexIDs.get(1), k->new ArrayList<>()).add(1);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexIDs.get(2), k->new ArrayList<>()).add(2);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexIDs.get(3), k->new ArrayList<>()).addAll(Arrays.asList(3, 4));
		vertexToStreamNodeIds.computeIfAbsent(jobVertexIDs.get(4), k->new ArrayList<>()).add(6);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexIDs.get(5), k->new ArrayList<>()).addAll(Arrays.asList(7, 8, 9));

		createBatchExecRelStages(13);
		when(relStageList.get(0).getTransformationIDList()).thenReturn(Arrays.asList(0));
		when(relStageList.get(1).getTransformationIDList()).thenReturn(Arrays.asList(1));
		when(relStageList.get(2).getTransformationIDList()).thenReturn(Arrays.asList(2));
		when(relStageList.get(3).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(3).getRelID()).thenReturn(3);
		when(relStageList.get(4).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(4).getRelID()).thenReturn(3);
		when(relStageList.get(4).getStageID()).thenReturn(1);
		when(relStageList.get(5).getTransformationIDList()).thenReturn(Arrays.asList(3));
		when(relStageList.get(5).getDependStageList()).thenReturn(Arrays.asList(relStageList.get(3), relStageList.get(4)));
		when(relStageList.get(6).getTransformationIDList()).thenReturn(Arrays.asList(4));
		when(relStageList.get(7).getTransformationIDList()).thenReturn(Arrays.asList(6));
		when(relStageList.get(8).getTransformationIDList()).thenReturn(Arrays.asList(7));
		when(relStageList.get(8).getRelID()).thenReturn(7);
		when(relStageList.get(9).getTransformationIDList()).thenReturn(Arrays.asList(7));
		when(relStageList.get(9).getDependStageList()).thenReturn(Arrays.asList(relStageList.get(8)));
		when(relStageList.get(10).getTransformationIDList()).thenReturn(Arrays.asList(8));
		when(relStageList.get(10).getRelID()).thenReturn(8);
		when(relStageList.get(11).getTransformationIDList()).thenReturn(Arrays.asList(8));
		when(relStageList.get(11).getDependStageList()).thenReturn(Arrays.asList(relStageList.get(10)));
		when(relStageList.get(12).getTransformationIDList()).thenReturn(Arrays.asList(9));

		createRelRunningUnits(5);

		when(relRunningUnitList.get(0).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(0), relStageList.get(1), relStageList.get(3)));
		when(relRunningUnitList.get(1).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(2), relStageList.get(4)));
		when(relRunningUnitList.get(2).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(5), relStageList.get(6), relStageList.get(7), relStageList.get(8)));
		when(relRunningUnitList.get(3).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(9), relStageList.get(10)));
		when(relRunningUnitList.get(4).getAllRelStages()).thenReturn(Arrays.asList(relStageList.get(11), relStageList.get(12)));

		RunningUnitSchedulerEventHandler schedulerEventHandler = new RunningUnitSchedulerEventHandler();
		schedulerEventHandler.open(jobVerticesNameMap, scheduler, vertexToStreamNodeIds, relRunningUnitList);

		schedulerEventHandler.onSchedulerStarted();
		verify(scheduler).scheduleTasks(Arrays.asList(taskList.get(0), taskList.get(1), taskList.get(2), taskList.get(3), taskList.get(6), taskList.get(7)));
		for (int index : Arrays.asList(0, 1, 2, 3, 6)) {
			schedulerEventHandler.onTaskStateChanged(new TaskStateChangedEvent(taskList.get(index), ExecutionState.DEPLOYING));
		}
		verify(scheduler).scheduleTasks(any());
		schedulerEventHandler.onTaskStateChanged(new TaskStateChangedEvent(taskList.get(7), ExecutionState.DEPLOYING));
		verify(scheduler).scheduleTasks(Arrays.asList(taskList.get(4), taskList.get(5)));
		schedulerEventHandler.onOperatorEvent(new RelStageDoneEvent(null, new RelStageID(3, 0)));
		verify(scheduler, times(2)).scheduleTasks(any());
		schedulerEventHandler.onOperatorEvent(new RelStageDoneEvent(null, new RelStageID(3, 1)));
		verify(scheduler, times(2)).scheduleTasks(any());
		for (int index : Arrays.asList(4, 5)) {
			schedulerEventHandler.onTaskStateChanged(new TaskStateChangedEvent(taskList.get(index), ExecutionState.DEPLOYING));
		}
		verify(scheduler).scheduleTasks(Arrays.asList(taskList.get(8), taskList.get(9), taskList.get(10), taskList.get(11)));
		verify(scheduler, times(3)).scheduleTasks(any());
		schedulerEventHandler.onOperatorEvent(new RelStageDoneEvent(null, new RelStageID(7, 0)));
		schedulerEventHandler.onOperatorEvent(new RelStageDoneEvent(null, new RelStageID(8, 0)));
		verify(scheduler, times(3)).scheduleTasks(any());
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
			jobVertexIDs.add(jobVertexID);
			jobVerticesNameMap.put(jobVertexID, "name");
		}
	}

	private void createTasks(int num) {
		for (int i = 0; i < num; i++) {
			TestLogicalTask task = new TestLogicalTask(i % 2);
			taskList.add(task);
		}
	}

	class TestLogicalTask implements LogicalTask {
		private JobVertexID jobVertexID;
		private ExecutionState state = ExecutionState.CREATED;
		private int taskNumber;

		public TestLogicalTask(int taskNumber) {
			this.taskNumber = taskNumber;
		}

		@Override
		public JobVertexID getVertexId() {
			return jobVertexID;
		}

		public void setJobVertexID(JobVertexID jobVertexID) {
			this.jobVertexID = jobVertexID;
		}

		@Override
		public int getTaskNumber() {
			return taskNumber;
		}

		public void setState(ExecutionState state) {
			this.state = state;
		}

		@Override
		public ExecutionState getExecutionState() {
			return state;
		}

		@Override
		public boolean isInputDataConsumable() {
			return false;
		}

		@Override
		public String toString() {
			return jobVertexIDs.indexOf(jobVertexID) + ", " + taskNumber;
		}
	}
}
