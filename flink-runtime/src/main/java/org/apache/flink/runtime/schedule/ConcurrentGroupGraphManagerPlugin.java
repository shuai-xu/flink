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

package org.apache.flink.runtime.schedule;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobType;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.ControlType;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobControlEdge;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SchedulingMode;
import org.apache.flink.runtime.jobmaster.ExecutionSlotAllocator;
import org.apache.flink.runtime.jobmaster.GraphManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A Scheduler plugin which schedules tasks in a concurrent group at the same time.
 */
public class ConcurrentGroupGraphManagerPlugin implements GraphManagerPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(ConcurrentGroupGraphManagerPlugin.class);

	private Set<ConcurrentSchedulingGroup> concurrentSchedulingGroups = new HashSet<>();

	private Map<ExecutionVertexID, ConcurrentSchedulingGroup> executionToConcurrentSchedulingGroups = new LinkedHashMap<>();

	private Map<JobVertexID, Set<JobVertex>> predecessorToSuccessors = new HashMap<>();

	private Map<JobVertexID, JobVertexID> successorToPredecessors = new HashMap<>();

	private VertexInputTracker inputTracker;

	private VertexScheduler scheduler;

	private JobGraph jobGraph;

	private ExecutionGraph executionGraph;

	private GraphManager graphManager;

	private ExecutionSlotAllocator executionSlotAllocator;

	private boolean allowGroupSplit;

	@Override
	public void open(
			VertexScheduler scheduler,
			JobGraph jobGraph,
			SchedulingConfig schedulingConfig,
			ExecutionGraph eg,
			GraphManager graphManager,
			ExecutionSlotAllocator executionSlotAllocator) {
		this.scheduler = scheduler;
		this.jobGraph = jobGraph;
		this.inputTracker = new VertexInputTracker(jobGraph, scheduler, schedulingConfig);
		this.executionGraph = eg;
		this.graphManager = graphManager;
		this.allowGroupSplit = schedulingConfig.getConfiguration().getBoolean("", false);
		this.executionSlotAllocator = executionSlotAllocator;
		initConcurrentSchedulingGroups();
		buildStartOnFinishRelation(jobGraph);
	}

	private void initConcurrentSchedulingGroups() {
		List<ConcurrentJobVertexGroup> concurrentJobVertexGroups = new ArrayList<>();
		List<JobVertex> allJobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			LOG.debug("All executions will be in one group for streaming job {}", jobGraph.getJobID());

			List<ExecutionVertex> allExecutionVertices = new ArrayList<>(executionGraph.getRegisteredExecutions().size());
			for (ExecutionVertex ev : executionGraph.getAllExecutionVertices()) {
				allExecutionVertices.add(ev);
			}
			concurrentJobVertexGroups.add(new ConcurrentJobVertexGroup(allJobVertices));
			this.concurrentSchedulingGroups.add(
					new ConcurrentSchedulingGroup(allExecutionVertices, false));
		} else {
			buildConcurrentSchedulingGroups(allJobVertices);
		}

		if (LOG.isDebugEnabled()) {
			for (ConcurrentSchedulingGroup group : concurrentSchedulingGroups) {
				LOG.debug("Concurrent group has {} with preceding {}", group.getExecutionVertices(), group.hasPrecedingGroup());
			}
		}
	}

	@Override
	public void close() {
		// do nothing.
	}

	@Override
	public void reset() {

	}

	@Override
	public void onSchedulingStarted() {
		concurrentSchedulingGroups.stream().forEach(
				(group) -> {
					if (!group.hasPrecedingGroup()) {
						scheduleGroup(group);
					}
		});
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();
		final Collection<Collection<ExecutionVertexID>> consumerVertices = jobGraph
				.getResultPartitionConsumerExecutionVertices(event.getResultID(), event.getPartitionNumber());
		for (Collection<ExecutionVertexID> executionVertexIDs : consumerVertices) {
			for (ExecutionVertexID executionVertexID : executionVertexIDs) {
				if (isReadyToSchedule(executionVertexID)) {
					verticesToSchedule.add(executionVertexID);
				}
			}
		}

		scheduleInConcurrentGroup(verticesToSchedule);
	}

	@Override
	public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
		final Set<ConcurrentSchedulingGroup> groupToSchedule = new HashSet<>();

		// For streaming job, region always will be less than concurrent group.
		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			scheduler.scheduleExecutionVertices(event.getAffectedExecutionVertexIDs());
		} else {
			for (ExecutionVertexID executionVertexID : event.getAffectedExecutionVertexIDs()) {
				if (isReadyToSchedule(executionVertexID)) {
					ConcurrentSchedulingGroup groupsBelongTo = executionToConcurrentSchedulingGroups.get(executionVertexID);
					groupToSchedule.add(groupsBelongTo);
				}
			}

			for (ConcurrentSchedulingGroup group : groupToSchedule) {
				if (!graphManager.cacheGroupIfReconciling(group)) {
					scheduleGroup(group);
				}
			}
		}
	}

	@Override
	public synchronized void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {
		final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();
		if (event.getNewExecutionState() == ExecutionState.FINISHED) {
			if (scheduler.getExecutionJobVertexStatus(event.getExecutionVertexID().getJobVertexID()) == ExecutionState.FINISHED) {
				Set<JobVertex> successorVertices = predecessorToSuccessors.get(event.getExecutionVertexID().getJobVertexID());

				if (successorVertices != null) {
					for (JobVertex successor : successorVertices) {
						for (int i = 0; i < successor.getParallelism(); i++) {
							ExecutionVertexID executionVertexID = new ExecutionVertexID(successor.getID(), i);
							if (isReadyToSchedule(executionVertexID)) {
								verticesToSchedule.add(executionVertexID);
							}
						}
					}
				}
			}
		}

		scheduleInConcurrentGroup(verticesToSchedule);
	}

	@Override
	public boolean allowLazyDeployment() {
		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			return false;
		}
		return true;
	}

	private List<ConcurrentSchedulingGroup> buildConcurrentSchedulingGroups(List<JobVertex> jobVerticesTopologically) {
		List<ConcurrentJobVertexGroup> concurrentJobVertexGroups = new ArrayList<>();
		List<ConcurrentSchedulingGroup> schedulingGroups = new ArrayList<>();

		final Set<JobVertex> visitedJobVertices = new HashSet<>();

		for (JobVertex jobVertex : jobVerticesTopologically) {
			if (visitedJobVertices.add(jobVertex)) {

				List<JobVertex> concurrentVertices = new ArrayList<>();
				concurrentVertices.add(jobVertex);

				if (jobVertex.getInControlEdges().isEmpty()) {
					for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
						if (output.getConsumers().size() > 0) {
							JobEdge jobEdge = output.getConsumers().get(0);
							if (jobVerticesTopologically.contains(jobEdge.getTarget()) &&
									!visitedJobVertices.contains(jobEdge.getTarget()) &&
									jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
								visitedJobVertices.add(jobEdge.getTarget());
								concurrentVertices.add(jobEdge.getTarget());
								concurrentVertices.addAll(
										getAllConcurrentVertices(jobEdge.getTarget(), jobVerticesTopologically, visitedJobVertices));
							}
						}
					}
				}
				if (!concurrentVertices.isEmpty()) {
					concurrentJobVertexGroups.add(new ConcurrentJobVertexGroup(concurrentVertices));
				}
			}
		}

		LOG.debug("{} vertex group was built with {} vertices.", concurrentJobVertexGroups.size(), jobVerticesTopologically.size());

		for (ConcurrentJobVertexGroup regionGroup : concurrentJobVertexGroups) {
			schedulingGroups.addAll(buildSchedulingGroupsFromJobVertexGroup(regionGroup));
		}

		this.concurrentSchedulingGroups.addAll(schedulingGroups);
		for (ConcurrentSchedulingGroup schedulingGroup: schedulingGroups) {
			for (ExecutionVertex ev : schedulingGroup.getExecutionVertices()) {
				executionToConcurrentSchedulingGroups.put(ev.getExecutionVertexID(), schedulingGroup);
			}
		}

		LOG.info("{} concurrent group was built with {} vertices for job {}.",
				schedulingGroups.size(), jobVerticesTopologically.size(), jobGraph.getJobID());

		return schedulingGroups;
	}

	public void resplitSchedulingGroup(
			List<JobVertex> assignedJobVertices,
			List<JobVertex> unAssignedJobVertices,
			ConcurrentSchedulingGroup originalGroup) {
		LOG.info("Split the scheduling group {} as resource is not enough.", originalGroup);

		concurrentSchedulingGroups.remove(originalGroup);

		// 1. Build groups for the job vertices that have been assigned resource.
		List<ConcurrentSchedulingGroup> newAssignedGroups = buildConcurrentSchedulingGroups(assignedJobVertices);
		// 2. Update the result partition.
		Set<JobVertex> visistedJobVertices = new HashSet<>();
		for (ConcurrentSchedulingGroup group: newAssignedGroups) {
			for (ExecutionVertex ev : group.getExecutionVertices()) {
				JobVertex jobVertex = ev.getJobVertex().getJobVertex();
				if (visistedJobVertices.add(jobVertex)) {
					for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
						IntermediateDataSet output = jobVertex.getProducedDataSets().get(i);
						if (!output.getConsumers().isEmpty()) {
							JobEdge jobEdge = output.getConsumers().get(0);
							for (ExecutionVertex executionVertex : executionGraph.getJobVertex(jobEdge.getTarget().getID()).getTaskVertices()) {
								if (executionVertex.getExecutionState() == ExecutionState.CREATED) {
									jobEdge.setSchedulingMode(SchedulingMode.SEQUENTIAL);
									ev.getJobVertex().getProducedDataSets()[i].setResultType(ResultPartitionType.BLOCKING);
									break;
								}
							}
						}
					}
				}
			}
		}
		// 3. Build groups for the job vertices that have not been assigned resource.
		buildConcurrentSchedulingGroups(unAssignedJobVertices);
		// 4. Rebuild virtual relations.
		buildStartOnFinishRelation(jobGraph);
		// 5. Rebuild failover region.
		// 6. Deploy the tasks.
		for (ConcurrentSchedulingGroup group : newAssignedGroups) {
			for (ExecutionVertex ev : group.getExecutionVertices()) {
				try {
					ev.getCurrentExecutionAttempt().deploy();
				} catch (Exception e) {
					LOG.info("Fail to deploy execution {}", ev, e);
					ev.getCurrentExecutionAttempt().fail(e);
				}
			}
		}
	}

	@VisibleForTesting
	Set<ConcurrentSchedulingGroup> getConcurrentSchedulingGroups() {
		return concurrentSchedulingGroups;
	}

	private Set<JobVertex> getAllConcurrentVertices(
			JobVertex jobVertex,
			List<JobVertex> allJobVerticesTopologically,
			Set<JobVertex> visitedJobVertices) {
		Set<JobVertex> concurrentVertices = new HashSet<>();

		for (JobEdge jobEdge : jobVertex.getInputs()) {
			if (jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT &&
					allJobVerticesTopologically.contains(jobEdge.getSource().getProducer()) &&
					jobEdge.getSource().getProducer().getInControlEdges().isEmpty() &&
					jobEdge.getSource().getProducer().getOutControlEdges().isEmpty()) {

				visitedJobVertices.add(jobEdge.getSource().getProducer());
				concurrentVertices.add(jobEdge.getSource().getProducer());
				concurrentVertices.addAll(getAllConcurrentVertices(jobEdge.getSource().getProducer(), allJobVerticesTopologically, visitedJobVertices));
			}
		}
		if (jobVertex.getInControlEdges().isEmpty()) {
			for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
				if (output.getConsumers().size() > 0) {
					JobEdge jobEdge = output.getConsumers().get(0);
					if (!visitedJobVertices.contains(jobEdge.getTarget()) &&
							jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
						visitedJobVertices.add(jobEdge.getTarget());
						concurrentVertices.add(jobEdge.getTarget());
						concurrentVertices.addAll(getAllConcurrentVertices(jobEdge.getTarget(), allJobVerticesTopologically, visitedJobVertices));
					}
				}
			}
		}
		return concurrentVertices;
	}

	private List<ConcurrentSchedulingGroup> buildSchedulingGroupsFromJobVertexGroup(
			ConcurrentJobVertexGroup jobVertexGroup) {

		final List<ConcurrentSchedulingGroup> schedulingGroups = new ArrayList<>();

		List<JobVertex> jobVerticesTopologically = jobVertexGroup.getVertices();
		if (jobVerticesTopologically.size() == 1) {
			for (ExecutionVertex ev : executionGraph.getJobVertex(jobVerticesTopologically.get(0).getID()).getTaskVertices()) {
				schedulingGroups.add(
						new ConcurrentSchedulingGroup(
								Collections.singletonList(ev),
								jobVertexGroup.hasPrecedingGroup()));
			}
		} else {
			List<ExecutionVertex> executionVertices = new ArrayList<>();
			for (JobVertex jobVertex : jobVerticesTopologically) {
				for (ExecutionVertex ev : executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()) {
					executionVertices.add(ev);
				}
			}
			schedulingGroups.add(new ConcurrentSchedulingGroup(
					executionVertices,
					jobVertexGroup.hasPrecedingGroup()));
		}

		return schedulingGroups;
	}

	private void scheduleInConcurrentGroup(Set<ExecutionVertexID> verticesToSchedule) {
		Set<ConcurrentSchedulingGroup> groupsToSchedule = new HashSet<>();
		for (ExecutionVertexID vertexID : verticesToSchedule) {
			ConcurrentSchedulingGroup groupsBelongTo = executionToConcurrentSchedulingGroups.get(vertexID);
			if (groupsBelongTo == null) {
				throw new RuntimeException("Can not find a group for " + vertexID + ", this is logic error.");
			}
			groupsToSchedule.add(groupsBelongTo);
		}
		for (ConcurrentSchedulingGroup group : groupsToSchedule) {
			if (!graphManager.cacheGroupIfReconciling(group)) {
				scheduleGroup(group);
			}
		}
	}

	private boolean isReadyToSchedule(ExecutionVertexID vertexID) {
		ExecutionVertexStatus vertexStatus = scheduler.getExecutionVertexStatus(vertexID);

		// only CREATED vertices can be scheduled
		if (vertexStatus.getExecutionState() != ExecutionState.CREATED) {
			return false;
		}

		JobVertexID predecessorId = successorToPredecessors.get(vertexID.getJobVertexID());
		if (predecessorId != null && scheduler.getExecutionJobVertexStatus(predecessorId) != ExecutionState.FINISHED) {
			return false;
		}

		// source vertices can be scheduled at once
		if (jobGraph.findVertexByID(vertexID.getJobVertexID()).isInputVertex()) {
			return true;
		}

		// query whether the inputs are ready overall
		return inputTracker.areInputsReady(vertexID);
	}

	private void buildStartOnFinishRelation(JobGraph jobGraph) {
		successorToPredecessors.clear();
		predecessorToSuccessors.clear();

		for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			for (JobControlEdge controlEdge : jobVertex.getOutControlEdges()) {
				LOG.debug("ControlEdge from {} to {} with type {}",
						controlEdge.getSource().getID(), controlEdge.getTarget().getID(), controlEdge.getControlType());
				if (controlEdge.getControlType() == ControlType.START_ON_FINISH) {
					Set<JobVertex> concurrentAncestors = getAllConcurrentAncestors(controlEdge.getTarget());
					for (JobVertex ancestor : concurrentAncestors) {
						successorToPredecessors.put(ancestor.getID(), jobVertex.getID());
					}
					Set<JobVertex> existingSuccessors = predecessorToSuccessors.putIfAbsent(jobVertex.getID(), concurrentAncestors);
					if (existingSuccessors != null) {
						existingSuccessors.addAll(concurrentAncestors);
					}
				}
			}
		}
	}

	private static Set<JobVertex> getAllConcurrentAncestors(JobVertex jobVertex) {
		if (jobVertex.isInputVertex()) {
			return Collections.singleton(jobVertex);
		} else {
			Set<JobVertex> ancestors = new HashSet<>();
			for (JobEdge jobEdge : jobVertex.getInputs()) {
				if (jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
					ancestors.addAll(getAllConcurrentAncestors(jobEdge.getSource().getProducer()));
				}
			}
			if (ancestors.isEmpty()) {
				ancestors.add(jobVertex);
			}
			return ancestors;
		}
	}

	public void scheduleGroup(ConcurrentSchedulingGroup schedulingGroup) {

		List<ExecutionVertex> executionVertices = schedulingGroup.getExecutionVertices();
		List<Execution> scheduledExecutions = new ArrayList<>();

		for (ExecutionVertex ev : executionVertices) {
			if (ev.getExecutionState() == ExecutionState.CREATED) {
				scheduledExecutions.add(ev.getCurrentExecutionAttempt());
			}
		}
		CompletableFuture<Collection<Void>> allocationFuture =
				executionSlotAllocator.allocateSlotsFor(scheduledExecutions);
		CompletableFuture<Void> currentSchedulingFuture = allocationFuture
				.exceptionally(
						throwable -> {
							if (!allowGroupSplit) {
								for (Execution execution : scheduledExecutions) {
									execution.fail(ExceptionUtils.stripCompletionException(throwable));
								}
								throw new CompletionException(throwable);
							} else {
								boolean hasFailure = false;
								for (Execution execution : scheduledExecutions) {
									if (hasFailure) {
										execution.rollbackToCreatedAndReleaseSlot();
									}
									else if (execution.getAssignedResource() == null) {
										hasFailure = true;
									}
								}
								groupSplit(schedulingGroup);
								return null;
							}
						}
				)
				.handleAsync(
						(ignored, throwable) -> {
							if (throwable != null) {
								throw new CompletionException(throwable);
							} else {
								boolean hasFailure = false;
								for (int i = 0; i <  scheduledExecutions.size(); i++) {
									try {
										scheduledExecutions.get(i).deploy();
									} catch (Exception e) {
										hasFailure = true;
										LOG.info("Fail to deploy execution {}", scheduledExecutions.get(i), e);
										scheduledExecutions.get(i).fail(e);
									}
								}
								if (hasFailure) {
									throw new CompletionException(
											new FlinkException("Fail to deploy some executions."));
								}
							}
							return null;
						}, executionGraph.getFutureExecutor());

		currentSchedulingFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
					if (strippedThrowable instanceof CancellationException) {
						// cancel the individual allocation futures
						allocationFuture.cancel(false);
					}
					executionGraph.unregisterSchedulingFuture(currentSchedulingFuture);
				});
		executionGraph.registerSchedulingFuture(currentSchedulingFuture);
	}

	@VisibleForTesting
	void groupSplit(ConcurrentSchedulingGroup group) {
		List<ExecutionVertex> executionVertices = group.getExecutionVertices();
		List<JobVertex> assignedJobVertices = new ArrayList<>();
		assignedJobVertices.add(executionVertices.get(0).getJobVertex().getJobVertex());

		int i = 1;
		for (; i < executionVertices.size(); i++) {
			ExecutionVertex ev = executionVertices.get(i);
			if (!ev.getJobvertexId().equals(executionVertices.get(i - 1).getJobvertexId()) &&
					!executionVertices.get(i - 1).getJobvertexId().equals(
							assignedJobVertices.get(assignedJobVertices.size() - 1).getID())) {
				assignedJobVertices.add(executionVertices.get(i - 1).getJobVertex().getJobVertex());
			}
			if (ev.getExecutionState() == ExecutionState.SCHEDULED && ev.getCurrentAssignedResource() == null) {
				break;
			}
		}
		if (assignedJobVertices.size() == 1) {
			if (executionVertices.get(i).getJobvertexId().equals(assignedJobVertices.get(0).getID())) {
				for (; i < executionVertices.size(); i++) {
					if (!executionVertices.get(i).getJobvertexId().equals(assignedJobVertices.get(0).getID())) {
						break;
					}
				}
			}
		}
		for (i = i - 1; i > 0; i--) {
			if (executionVertices.get(i).getExecutionState() == ExecutionState.SCHEDULED &&
					executionVertices.get(i).getCurrentAssignedResource() != null) {
				executionVertices.get(i).getCurrentExecutionAttempt().rollbackToCreatedAndReleaseSlot();
			}
			if (executionVertices.get(i - 1).getJobvertexId().equals(
					assignedJobVertices.get(assignedJobVertices.size() - 1).getID())) {
				break;
			}
		}
		List<JobVertex> unAssignedJobVertices = new ArrayList<>();
		for (; i < executionVertices.size(); i++) {
			if (unAssignedJobVertices.isEmpty() ||
					!unAssignedJobVertices.get(unAssignedJobVertices.size() - 1).getID().equals(
							executionVertices.get(i).getJobvertexId())) {
				unAssignedJobVertices.add(executionVertices.get(i).getJobVertex().getJobVertex());
			}
		}

		resplitSchedulingGroup(assignedJobVertices, unAssignedJobVertices, group);
	}
}
