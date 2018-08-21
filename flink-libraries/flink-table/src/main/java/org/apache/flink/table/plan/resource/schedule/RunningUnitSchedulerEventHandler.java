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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.event.OperatorEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.event.TaskFailoverEvent;
import org.apache.flink.runtime.event.TaskStateChangedEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.JobScheduler;
import org.apache.flink.runtime.scheduler.LogicalTask;
import org.apache.flink.runtime.scheduler.SchedulerEventHandler;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.plan.resource.BatchExecRelStage;
import org.apache.flink.table.plan.resource.RelRunningUnit;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.JOB_VERTEX_TO_NAME_MAP;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.JOB_VERTEX_TO_STREAM_NODE_MAP;

/**
 * Schedule job based on runningUnit.
 */
public class RunningUnitSchedulerEventHandler implements SchedulerEventHandler {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);
	public static final String RUNNING_UNIT_CONF_KEY = "runningUnit.key";

	private Map<JobVertexID, LogicalJobVertex> jobVertices = new LinkedHashMap<>();
	private List<LogicalJobVertexRunningUnit> allJobVertexRunningUnitList = new LinkedList<>();
	private AtomicInteger scheduledUnitNum = new AtomicInteger();
	// for sink who not chain with pre vertex.
	private LogicalJobVertexRunningUnit leftJobVertexRunningUnit;

	private LinkedList<LogicalJobVertexRunningUnit> jobVertexRunningUnitQueue = new LinkedList<>();
	private Set<LogicalJobVertexRunningUnit> scheduledJobVertexRunningUnitSet = Collections.newSetFromMap(new IdentityHashMap<>());
	private LogicalJobVertexRunningUnit currentScheduledUnit = null;
	private Map<RelStageID, List<LogicalJobVertexRunningUnit>> eventRunningUnitMap = new LinkedHashMap<>();

	private Map<JobVertexID, String> jobVerticesNameMap;
	private Map<BatchExecRelStage, RelRunningUnit> stageUnitMap = new IdentityHashMap<>();
	private Map<RelRunningUnit, Set<BatchExecRelStage>> unitStageMap = new LinkedHashMap<>();
	private JobScheduler scheduler;

	@Override
	public void open(JobScheduler scheduler, Configuration jobConfig, ClassLoader userClassLoader) {
		try {
			Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = InstantiationUtil.readObjectFromConfig(jobConfig, JOB_VERTEX_TO_STREAM_NODE_MAP, userClassLoader);
			Map<JobVertexID, String> jobVerticesNameMap = InstantiationUtil.readObjectFromConfig(jobConfig, JOB_VERTEX_TO_NAME_MAP, userClassLoader);
			List<RelRunningUnit> relRunningUnits = InstantiationUtil.readObjectFromConfig(jobConfig, RUNNING_UNIT_CONF_KEY, userClassLoader);
			open(jobVerticesNameMap, scheduler, vertexToStreamNodeIds, relRunningUnits);
		} catch (IOException e) {
			LOG.warn("catch IOException.", e);
		} catch (ClassNotFoundException e) {
			LOG.warn("catch ClassNotFoundException.", e);
		}
	}

	@VisibleForTesting
	public void open(Map<JobVertexID, String> jobVerticesNameMap,
			JobScheduler scheduler, Map<JobVertexID,
			ArrayList<Integer>> vertexToStreamNodeIds,
			List<RelRunningUnit> relRunningUnits) {
		this.jobVerticesNameMap = jobVerticesNameMap;
		this.scheduler = scheduler;
		Map<Integer, JobVertexID> streamNodeIdToVertex = reverseMap(vertexToStreamNodeIds);
		buildJobVertices();
		buildJobRunningUnits(relRunningUnits, streamNodeIdToVertex);
	}

	private void buildJobRunningUnits(List<RelRunningUnit> relRunningUnits, Map<Integer, JobVertexID> streamNodeIdToVertex) {
		Map<JobVertexID, LogicalJobVertexRunningUnit> jobVertexRunningUnitMap = new HashMap<>();
		for (RelRunningUnit relRunningUnit : relRunningUnits) {
			List<BatchExecRelStage> allStages = relRunningUnit.getAllRelStages();
			for (BatchExecRelStage stage : allStages) {
				stageUnitMap.put(stage, relRunningUnit);
			}
			Set<BatchExecRelStage> containStages = Collections.newSetFromMap(new IdentityHashMap<>());
			containStages.addAll(allStages);
			unitStageMap.put(relRunningUnit, containStages);
		}

		avoidDeadLockDepend(relRunningUnits);

		for (RelRunningUnit relRunningUnit : relRunningUnits) {
			List<BatchExecRelStage> allStages = relRunningUnit.getAllRelStages();

			Set<LogicalJobVertex> jobVertexSet = new LinkedHashSet<>();
			for (BatchExecRelStage stage : allStages) {
				for (Integer transformationID : stage.getTransformationIDList()) {
					jobVertexSet.add(jobVertices.get(streamNodeIdToVertex.get(transformationID)));
				}
			}
			LogicalJobVertexRunningUnit jobVertexRunningUnit = new LogicalJobVertexRunningUnit(jobVertexSet);

			// jobVertex runningUnit depend event.
			for (BatchExecRelStage stage : allStages) {
				for (BatchExecRelStage dependStage : stage.getDependStageList()) {
					RelStageID relStageID = new RelStageID(dependStage.getRelID(), dependStage.getStageID());
					eventRunningUnitMap.computeIfAbsent(relStageID, k -> new LinkedList<>()).add(jobVertexRunningUnit);
					jobVertexRunningUnit.addDependRelStage(relStageID);
				}
			}
			for (LogicalJobVertex logicalJobVertex : jobVertexRunningUnit.getJobVertexSet()) {
				jobVertexRunningUnitMap.put(logicalJobVertex.getJobVertexID(), jobVertexRunningUnit);
			}
			allJobVertexRunningUnitList.add(jobVertexRunningUnit);
		}

		// deal with left jobIDs
		Set<LogicalJobVertex> jobVertexSet = new LinkedHashSet<>();
		for (JobVertexID jobVertexID : jobVerticesNameMap.keySet()) {
			if (!jobVertexRunningUnitMap.containsKey(jobVertexID)) {
				LogicalJobVertex logicalJobVertex = new LogicalJobVertex(jobVertexID, scheduler.getTasks(jobVertexID));
				jobVertexSet.add(logicalJobVertex);
			}
		}
		leftJobVertexRunningUnit = new LogicalJobVertexRunningUnit(jobVertexSet);
	}

	// if loop depend, remove the stage.
	private void avoidDeadLockDepend(List<RelRunningUnit> relRunningUnits) {
		for (RelRunningUnit unit : relRunningUnits) {
			for (BatchExecRelStage stage : unitStageMap.get(unit)) {
				List<BatchExecRelStage> toRemoveStages = new LinkedList<>();
				for (BatchExecRelStage dependStage : stage.getDependStageList()) {
					Set<RelRunningUnit> visitedRunningUnit = new HashSet<>();
					if (loopDepend(stageUnitMap.get(dependStage), unit, visitedRunningUnit)) {
						toRemoveStages.add(dependStage);
					}
				}
				for (BatchExecRelStage toRemove : toRemoveStages) {
					stage.removeDependStage(toRemove);
				}
			}
		}
	}

	private boolean loopDepend(RelRunningUnit dependUnit, RelRunningUnit preUnit, Set<RelRunningUnit> visitedRunningUnit) {
		if (dependUnit == preUnit) {
			return true;
		}
		if (visitedRunningUnit.contains(dependUnit)) {
			return false;
		} else {
			visitedRunningUnit.add(dependUnit);
		}
		for (BatchExecRelStage containStage : unitStageMap.get(dependUnit)) {
			for (BatchExecRelStage dependStage : containStage.getDependStageList()) {
				if (loopDepend(stageUnitMap.get(dependStage), preUnit, visitedRunningUnit)) {
					return true;
				}
			}
		}
		return false;
	}

	private void buildJobVertices() {
		Collection<? extends LogicalTask> taskCollection = scheduler.getAllTasks();
		Set<JobVertexID> jobVertexIDSet = new LinkedHashSet<>();
		for (LogicalTask task : taskCollection) {
			jobVertexIDSet.add(task.getVertexId());
		}
		for (JobVertexID jobVertexID : jobVertexIDSet) {
			Collection<? extends LogicalTask> tasks = scheduler.getTasks(jobVertexID);
			LogicalJobVertex jobVertex = new LogicalJobVertex(jobVertexID, tasks);
			jobVertices.put(jobVertexID, jobVertex);
		}
	}

	private Map<Integer, JobVertexID> reverseMap(Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds) {
		Map<Integer, JobVertexID> streamNodeIdToVertex = new LinkedHashMap<>();
		for (Map.Entry<JobVertexID, ArrayList<Integer>> entry : vertexToStreamNodeIds.entrySet()) {
			for (Integer transId : entry.getValue()) {
				streamNodeIdToVertex.put(transId, entry.getKey());
			}
		}
		return streamNodeIdToVertex;
	}

	@Override
	public void close() {
		// do nothing.
	}

	@Override
	public void onSchedulerStarted() {
		for (LogicalJobVertexRunningUnit jobRunningUnit : allJobVertexRunningUnitList) {
			if (jobRunningUnit.dependEventsReady()) {
				scheduleJobRunningUnit(jobRunningUnit);
			}
		}
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		// do nothing.
	}

	@Override
	public void onTaskStateChanged(TaskStateChangedEvent event) {
		if (event.getTaskNewExecutionState() == ExecutionState.DEPLOYING) {
			LogicalJobVertexRunningUnit preJobVertexRunningUnit = currentScheduledUnit;
			jobVertices.get(event.getTask().getVertexId()).taskScheduled();
			if (preJobVertexRunningUnit.allTasksDeploying()) {
				finishCurrentScheduling(preJobVertexRunningUnit);
			}
		}
	}

	@Override
	public void onOperatorEvent(OperatorEvent event) {
		if (!(event instanceof RelStageDoneEvent)) {
			return;
		}
		RelStageDoneEvent relStageDoneEvent = (RelStageDoneEvent) event;
		LOG.info("receive event from : " + jobVerticesNameMap.get(event.getVertexID()) + ", event id: " + relStageDoneEvent.getRelStageID());
		RelStageID relStageID = relStageDoneEvent.getRelStageID();
		List<LogicalJobVertexRunningUnit> unitList = eventRunningUnitMap.get(relStageID);
		if (unitList == null) {
			return;
		}
		for (LogicalJobVertexRunningUnit jobRunningUnit : unitList) {
			jobRunningUnit.receiveEventID(relStageID);
			if (jobRunningUnit.dependEventsReady()) {
				scheduleJobRunningUnit(jobRunningUnit);
			}
		}
	}

	private synchronized void finishCurrentScheduling(LogicalJobVertexRunningUnit preJobVertexRunningUnit) {
		if (preJobVertexRunningUnit != currentScheduledUnit) {
			// currentScheduledUnit has been finished by other task and be replaced to others.
			return;
		}
		if (unitDeployedAndCheckAllDeployed()) {
			return;
		}
		do {
			currentScheduledUnit = jobVertexRunningUnitQueue.pollFirst();
			if (currentScheduledUnit != null
					&& currentScheduledUnit.allTasksDeploying()
					&& unitDeployedAndCheckAllDeployed()) {
				// all jobVertices have been scheduled.
				return;
			}
		} while (currentScheduledUnit != null && currentScheduledUnit.allTasksDeploying());

		if (currentScheduledUnit != null) {
			scheduler.scheduleTasks(currentScheduledUnit.getToScheduleTasks());
		}
	}

	private synchronized void scheduleJobRunningUnit(LogicalJobVertexRunningUnit jobVertexRunningUnit) {
		if (!scheduledJobVertexRunningUnitSet.add(jobVertexRunningUnit)) {
			return;
		}
		if (jobVertexRunningUnit.allTasksDeploying()) {
			unitDeployedAndCheckAllDeployed();
			return;
		}
		if (currentScheduledUnit == null) {
			currentScheduledUnit = jobVertexRunningUnit;
			scheduler.scheduleTasks(jobVertexRunningUnit.getToScheduleTasks());
		} else {
			jobVertexRunningUnitQueue.add(jobVertexRunningUnit);
		}
	}

	private synchronized boolean unitDeployedAndCheckAllDeployed() {
		if (scheduledUnitNum.incrementAndGet() == allJobVertexRunningUnitList.size() && !leftJobVertexRunningUnit.allTasksDeploying()) {
			currentScheduledUnit = leftJobVertexRunningUnit;
			scheduler.scheduleTasks(leftJobVertexRunningUnit.getToScheduleTasks());
		}
		return scheduledUnitNum.get() >= allJobVertexRunningUnitList.size();
	}

	@Override
	public void onTaskFailover(TaskFailoverEvent event) {
		// TODO
	}

	@Override
	public boolean scheduleInBatch() {
		return true;
	}

}
